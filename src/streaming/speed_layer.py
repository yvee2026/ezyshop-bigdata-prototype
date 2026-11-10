"""Speed layer: Spark Structured Streaming from Kafka.

    python -m src.streaming.speed_layer [--master local[3]] [--trigger 30] [--max-offsets N]

Two streaming queries share one SparkSession:

Query A  bronze_events   Kafka (3 topics) -> parse/validate -> bronze/{clicks,orders,reviews}
                         (append, partitioned by event_date) + bronze/_quarantine for bad records.
                         foreachBatch with Delta txnAppId/txnVersion => a replayed micro-batch is
                         skipped by Delta, so restarts never duplicate bronze rows.

Query B  live_metrics    Kafka -> parse -> watermark(10 min) -> dropDuplicatesWithinWatermark(event_id)
                         -> 1-minute tumbling windows per category -> MongoDB via bulk upserts on a
                         deterministic _id "<window_start>|<category>" (idempotent on replay).

A StreamingQueryListener writes per-batch progress (input rows, rows/s, event-time min/avg/max,
durations) to Mongo `pipeline_metrics`; the latency and throughput benchmarks read it.
"""
from __future__ import annotations

import argparse
import datetime as dt
import signal
import time

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from src.common import mongo
from src.common.bronze import parse, validity
from src.common.config import Config, get_config
from src.common.schemas import CLICKS_JSON, ORDERS_JSON, REVIEWS_JSON
from src.common.sentiment import vader_udf
from src.common.spark_session import build_spark

TOPICS = ("clicks", "orders", "reviews")
ALL = "__all__"  # store-wide row so dashboard totals count each order exactly once


# --------------------------------------------------------------------------- sources
def kafka_stream(spark: SparkSession, cfg: Config, starting: str, max_offsets: int) -> DataFrame:
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
            .option("subscribe", ",".join([cfg.topic_clicks, cfg.topic_orders, cfg.topic_reviews]))
            .option("startingOffsets", starting)
            .option("maxOffsetsPerTrigger", max_offsets)   # bounds batch size during bursts
            .option("failOnDataLoss", "false")
            .load()
            .select(F.col("topic"), F.col("value").cast("string").alias("value"),
                    F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset")))


def topic_name(cfg: Config, t: str) -> str:
    return {"clicks": cfg.topic_clicks, "orders": cfg.topic_orders, "reviews": cfg.topic_reviews}[t]


def product_category_map(spark: SparkSession, cfg: Config):
    rows = spark.read.jdbc(cfg.jdbc_url, "products", properties=cfg.jdbc_props).select("product_id", "category").collect()
    pairs = []
    for r in rows:
        pairs += [F.lit(r.product_id), F.lit(r.category)]
    return F.create_map(*pairs), len(rows)


# --------------------------------------------------------------------------- query A: bronze
def bronze_writer(cfg: Config):
    def write(batch: DataFrame, batch_id: int) -> None:
        # GDPR: drop events of erased customers at ingestion (suppression list from erase_customer)
        suppressed = [d["_id"] for d in mongo.db().gdpr_suppressed.find({}, {"_id": 1})]
        if suppressed:
            keep = ~F.lit(False)
            for pid in suppressed:
                keep = keep & ~F.col("value").contains(pid)
            batch = batch.where(keep)
        batch.persist()
        for t in TOPICS:
            good, bad = parse(batch.where(F.col("topic") == topic_name(cfg, t)).drop("topic"), t, source="kafka")
            (good.write.format("delta").mode("append").partitionBy("event_date")
                 .option("txnAppId", f"bronze_{t}").option("txnVersion", batch_id)
                 .option("mergeSchema", "true").save(cfg.path("bronze", t)))
            (bad.write.format("delta").mode("append").partitionBy("event_date")
                .option("txnAppId", f"quarantine_{t}").option("txnVersion", batch_id)
                .save(cfg.path("bronze", "_quarantine")))
        batch.unpersist()
    return write


# --------------------------------------------------------------------------- query B: live metrics
def facts(raw: DataFrame, cfg: Config, cat_map) -> DataFrame:
    """Normalise the three topics into one fact stream: (event_id, event_time, category, measures).

    Single pass over the Kafka micro-batch: each record is parsed with its topic's schema and
    turned into an array of fact structs, then exploded. (A first version used one union branch
    per topic/measure; Spark then scanned and parsed the Kafka batch 5 times: TEST_LOG T05.)
    """
    t = F.col("topic")
    is_c, is_o, is_r = t == cfg.topic_clicks, t == cfg.topic_orders, t == cfg.topic_reviews
    df = raw.select(
        "topic",
        F.when(is_c, F.from_json("value", CLICKS_JSON)).alias("jc"),
        F.when(is_o, F.from_json("value", ORDERS_JSON)).alias("jo"),
        F.when(is_r, F.from_json("value", REVIEWS_JSON)).alias("jr"),
    )
    df = (df.withColumn("event_id", F.coalesce("jc.event_id", "jo.event_id", "jr.event_id"))
            .withColumn("_ts", F.to_timestamp(F.coalesce("jc.event_time", "jo.event_time", "jr.event_time")))
            .where(F.coalesce(F.when(is_c, validity("clicks", "jc"))
                               .when(is_o, validity("orders", "jo"))
                               .when(is_r, validity("reviews", "jr")), F.lit(False)))
            # VADER only has text on review rows; other rows pass "" and score 0 instantly
            .withColumn("_sent", vader_udf()(F.coalesce(F.col("jr.text"), F.lit("")))))

    def fact(category, views=0, carts=0, orders=0, revenue=0.0, sent=0.0, reviews=0):
        lit = lambda v: v if isinstance(v, Column) else F.lit(v)  # noqa: E731
        return F.struct(lit(category).cast("string").alias("category"),
                        lit(views).cast("long").alias("views"), lit(carts).cast("long").alias("add_to_cart"),
                        lit(orders).cast("long").alias("orders"), lit(revenue).cast("double").alias("revenue"),
                        lit(sent).cast("double").alias("sent_sum"), lit(reviews).cast("long").alias("reviews"))

    view = (F.col("jc.action") == "view").cast("long")
    cart = (F.col("jc.action") == "add_to_cart").cast("long")
    click_facts = F.array(fact(cat_map[F.col("jc.product_id")], view, cart), fact(ALL, view, cart))

    # an order counts once in every category it contains (with that category's revenue share)
    # and exactly once in __all__ (with the order total)
    items = F.col("jo.items")
    cats = F.array_distinct(F.transform(items, lambda x: cat_map[x["product_id"]]))
    order_facts = F.concat(
        F.transform(cats, lambda c: fact(c, orders=1, revenue=F.aggregate(
            F.filter(items, lambda x: cat_map[x["product_id"]] == c), F.lit(0.0),
            lambda acc, x: acc + x["qty"] * x["unit_price"]))),
        F.array(fact(ALL, orders=1, revenue=F.col("jo.total"))))

    review_facts = F.array(fact(cat_map[F.col("jr.product_id")], sent=F.col("_sent"), reviews=1))

    # Build the array in its own projection, then explode. Exploding the CASE expression directly
    # next to the Python UDF column failed with "Couldn't find event_id" (TEST_LOG T06).
    df = df.select("event_id", F.col("_ts").alias("event_time"),
                   F.when(is_c, click_facts).when(is_o, order_facts).otherwise(review_facts).alias("facts"))
    return (df.select("event_id", "event_time", F.explode("facts").alias("f"))
              .select("event_id", "event_time", "f.*").where(F.col("category").isNotNull()))


def live_aggregates(fact: DataFrame, cfg: Config) -> DataFrame:
    return (fact.withWatermark("event_time", f"{cfg.watermark_minutes} minutes")
                # at-least-once delivery + injected duplicates: drop repeats of the same event
                .dropDuplicatesWithinWatermark(["event_id", "category"])
                .groupBy(F.window("event_time", f"{cfg.window_minutes} minute").alias("w"), "category")
                .agg(F.sum("views").alias("views"), F.sum("add_to_cart").alias("add_to_cart"),
                     F.sum("orders").alias("orders"), F.round(F.sum("revenue"), 2).alias("revenue"),
                     F.sum("sent_sum").alias("sent_sum"), F.sum("reviews").alias("reviews"),
                     F.max("event_time").alias("max_event_time")))


def mongo_writer(query_name: str):
    def write(batch: DataFrame, batch_id: int) -> None:
        t0 = time.time()
        rows = batch.collect()   # one row per updated (window, category): small by construction
        t_collected = time.time()
        now = dt.datetime.now(dt.timezone.utc)
        metrics, sentiment = [], []
        newest = None
        for r in rows:
            ws = r["w"]["start"].replace(tzinfo=dt.timezone.utc)
            key = f"{ws.isoformat()}|{r['category']}"
            met = r["max_event_time"].replace(tzinfo=dt.timezone.utc) if r["max_event_time"] else None
            newest = met if newest is None or (met and met > newest) else newest
            metrics.append({
                "_id": key, "window_start": ws, "window_end": r["w"]["end"].replace(tzinfo=dt.timezone.utc),
                "category": r["category"], "views": int(r["views"]), "add_to_cart": int(r["add_to_cart"]),
                "orders": int(r["orders"]), "revenue": float(r["revenue"] or 0.0),
                "conversion_rate": (r["orders"] / r["views"]) if r["views"] else None,
                "cart_rate": (r["add_to_cart"] / r["views"]) if r["views"] else None,
                "max_event_time": met, "updated_at": now, "batch_id": batch_id,
            })
            if r["reviews"]:
                sentiment.append({"_id": key, "window_start": ws, "category": r["category"],
                                  "avg_sentiment": float(r["sent_sum"]) / int(r["reviews"]),
                                  "reviews": int(r["reviews"]), "updated_at": now})
        mongo.upsert_set("live_metrics", metrics)
        mongo.upsert_set("live_sentiment", sentiment)
        t_written = time.time()
        write_ts = dt.datetime.now(dt.timezone.utc)
        mongo.db().pipeline_metrics.insert_one({
            "type": "batch", "query": query_name, "batch_id": batch_id, "ts": write_ts,
            "rows_upserted": len(metrics), "max_event_time": newest,
            "freshness_s": (write_ts - newest).total_seconds() if newest else None,
            "batch_compute_s": round(t_collected - t0, 3), "mongo_write_s": round(t_written - t_collected, 3),
        })
    return write


class ProgressToMongo(StreamingQueryListener):
    """Persists StreamingQueryProgress so throughput/latency can be analysed after the run."""

    def __init__(self, tag: str):
        self.tag = tag

    def onQueryStarted(self, event):  # noqa: N802
        pass

    def onQueryIdle(self, event):  # noqa: N802
        pass

    def onQueryTerminated(self, event):  # noqa: N802
        pass

    def onQueryProgress(self, event):  # noqa: N802
        p = event.progress
        try:
            et = dict(p.eventTime or {})
            dur = dict(p.durationMs or {})
            ts = dt.datetime.fromisoformat(p.timestamp.replace("Z", "+00:00"))
            end = ts + dt.timedelta(milliseconds=dur.get("triggerExecution", 0))

            def lag(key):
                if key not in et:
                    return None
                return (end - dt.datetime.fromisoformat(et[key].replace("Z", "+00:00"))).total_seconds()

            mongo.db().pipeline_metrics.insert_one({
                "type": "progress", "tag": self.tag, "query": p.name, "batch_id": p.batchId, "ts": end,
                "num_input_rows": p.numInputRows, "input_rows_per_s": p.inputRowsPerSecond,
                "processed_rows_per_s": p.processedRowsPerSecond, "duration_ms": dur,
                "event_time": et, "latency_newest_s": lag("max"), "latency_avg_s": lag("avg"),
                "latency_oldest_s": lag("min"),
            })
        except Exception as exc:  # never let metrics break the pipeline
            print(f"[listener] failed to record progress: {exc}")


# --------------------------------------------------------------------------- main
def start(cfg: Config, master: str | None, trigger_s: int, max_offsets: int, starting: str,
          ckpt_suffix: str = "", tag: str = "default", bronze: bool = True,
          extra_conf: dict | None = None):
    spark = build_spark("ezyshop-speed-layer", cfg, master=master, extra={
        "spark.sql.streaming.stateStore.providerClass":
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        **(extra_conf or {}),
    })
    mongo.ensure_indexes()
    spark.streams.addListener(ProgressToMongo(tag))
    cat_map, n_products = product_category_map(spark, cfg)
    print(f"speed layer: master={spark.sparkContext.master} trigger={trigger_s}s maxOffsetsPerTrigger={max_offsets} "
          f"window={cfg.window_minutes}m watermark={cfg.watermark_minutes}m products={n_products}", flush=True)

    raw = kafka_stream(spark, cfg, starting, max_offsets)
    queries = []
    if bronze:
        queries.append(raw.writeStream.queryName("bronze_events").foreachBatch(bronze_writer(cfg))
                       .option("checkpointLocation", cfg.path("_checkpoints", f"bronze_events{ckpt_suffix}"))
                       .trigger(processingTime=f"{trigger_s} seconds").start())
    agg = live_aggregates(facts(raw, cfg, cat_map), cfg)
    queries.append(agg.writeStream.queryName("live_metrics").outputMode("update")
                   .foreachBatch(mongo_writer("live_metrics"))
                   .option("checkpointLocation", cfg.path("_checkpoints", f"live_metrics{ckpt_suffix}"))
                   .trigger(processingTime=f"{trigger_s} seconds").start())
    return spark, queries


def main() -> None:
    cfg = get_config()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--master", default=None, help="e.g. local[1] or local[3]; default SPARK_MASTER")
    ap.add_argument("--trigger", type=int, default=cfg.trigger_seconds)
    ap.add_argument("--max-offsets", type=int, default=cfg.max_offsets_per_trigger)
    ap.add_argument("--starting-offsets", default="earliest")
    ap.add_argument("--run-seconds", type=int, default=0, help="stop after N seconds (tests/benchmarks)")
    a = ap.parse_args()

    spark, queries = start(cfg, a.master, a.trigger, a.max_offsets, a.starting_offsets)
    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    t0 = time.time()
    try:
        while not stop["flag"] and all(q.isActive for q in queries):
            if a.run_seconds and time.time() - t0 > a.run_seconds:
                break
            time.sleep(2)
        for q in queries:
            if q.exception():
                raise q.exception()
    except KeyboardInterrupt:
        pass
    finally:
        print("stopping streaming queries ...", flush=True)
        for q in queries:
            q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
