"""Reconcile Kafka -> bronze -> Mongo counts (used for the kill/restart recovery test).

    docker compose exec -T spark python scripts/verify_counts.py

Checks
1. No loss into bronze:   Kafka messages (end - start offsets) == bronze rows + quarantine rows (source=kafka)
2. No duplicate writes:   bronze rows == distinct (kafka_partition, kafka_offset)
3. No double counting:    Mongo __all__ orders/views == distinct order/view event_ids in bronze
                          (same time range; differences can only come from events later than the watermark)
"""
import json

from confluent_kafka import Consumer, TopicPartition
from pyspark.sql import functions as F

from src.common import mongo
from src.common.config import get_config
from src.common.spark_session import build_spark

cfg = get_config()
spark = build_spark("verify-counts", master="local[2]")
result = {}

# ---- Kafka message counts
c = Consumer({"bootstrap.servers": cfg.kafka_bootstrap, "group.id": "verify-counts"})
kafka_total = {}
for t in ("clicks", "orders", "reviews"):
    name = getattr(cfg, f"topic_{t}")
    md = c.list_topics(name, timeout=10).topics[name]
    n = 0
    for p in md.partitions:
        lo, hi = c.get_watermark_offsets(TopicPartition(name, p), timeout=10)
        n += hi - lo
    kafka_total[t] = n
c.close()

q = spark.read.format("delta").load(cfg.path("bronze", "_quarantine")).where("source='kafka'")
ok = True
for t in ("clicks", "orders", "reviews"):
    b = spark.read.format("delta").load(cfg.path("bronze", t)).where("source='kafka'")
    rows = b.count()
    distinct_off = b.select("kafka_partition", "kafka_offset").distinct().count()
    bad = q.where(F.col("topic") == t).count()
    result[t] = {"kafka_messages": kafka_total[t], "bronze_rows": rows, "quarantine_rows": bad,
                 "bronze_distinct_offsets": distinct_off,
                 "no_loss": rows + bad == kafka_total[t], "no_duplicate_writes": rows == distinct_off}
    ok &= result[t]["no_loss"] and result[t]["no_duplicate_writes"]

# ---- Mongo vs bronze (distinct events) over the windows present in Mongo
docs = list(mongo.db().live_metrics.find({"category": "__all__"}, {"window_start": 1, "orders": 1, "views": 1}))
if docs:
    lo = min(d["window_start"] for d in docs)
    m_orders = sum(d["orders"] for d in docs)
    m_views = sum(d["views"] for d in docs)
    o = (spark.read.format("delta").load(cfg.path("bronze", "orders"))
         .where((F.col("source") == "kafka") & (F.col("event_time") >= F.lit(lo))))
    v = (spark.read.format("delta").load(cfg.path("bronze", "clicks"))
         .where((F.col("source") == "kafka") & (F.col("action") == "view") & (F.col("event_time") >= F.lit(lo))))
    b_orders, b_views = o.select("event_id").distinct().count(), v.select("event_id").distinct().count()
    result["mongo_vs_bronze"] = {
        "mongo_orders": m_orders, "bronze_distinct_orders": b_orders, "orders_diff": m_orders - b_orders,
        "mongo_views": m_views, "bronze_distinct_views": b_views, "views_diff": m_views - b_views,
        "bronze_order_rows_incl_duplicates": o.count(),
    }
    # Mongo must never exceed the distinct count (that would be double counting)
    ok &= m_orders <= b_orders and m_views <= b_views

print(json.dumps(result, indent=2, default=str))
print("COUNTS OK" if ok else "COUNTS MISMATCH")
spark.stop()
