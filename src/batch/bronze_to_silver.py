"""Batch step 1: bronze -> silver.

* Snapshot the system-of-record tables from Postgres over JDBC (products, inventory,
  suppliers, campaigns), as A2's nightly "batch import / CDC" path does.
* Deduplicate events on event_id (at-least-once delivery and injected duplicates).
* Explode order line items and join them to products. This is where the semi-structured
  order JSON is integrated with the structured product master.
* Silver tables are rebuilt in full each night (idempotent; fine at prototype scale; an
  incremental MERGE on event_id would replace this at production volume).
"""
from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import Config


def _jdbc(spark: SparkSession, cfg: Config, table: str) -> DataFrame:
    return spark.read.jdbc(cfg.jdbc_url, table, properties=cfg.jdbc_props)


def _write(df: DataFrame, path: str, partition: str | None = None) -> None:
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition:
        w = w.partitionBy(partition)
    w.save(path)


def _dedup(df: DataFrame) -> DataFrame:
    # Keep the first-ingested copy of each event_id.
    return df.dropDuplicates(["event_id"])


def run(spark: SparkSession, cfg: Config) -> dict:
    t0 = time.time()
    stats: dict = {}

    # ---- structured system of record (Postgres -> silver snapshots)
    products = _jdbc(spark, cfg, "products").withColumn("price", F.col("price").cast("double"))
    for name, df in [("products", products), ("inventory", _jdbc(spark, cfg, "inventory")),
                     ("suppliers", _jdbc(spark, cfg, "suppliers")), ("campaigns", _jdbc(spark, cfg, "campaigns"))]:
        _write(df, cfg.path("silver", name))
        stats[f"pg_{name}"] = df.count()
    prod = F.broadcast(spark.read.format("delta").load(cfg.path("silver", "products"))
                       .select("product_id", "category", "subcategory", "supplier_id", F.col("price").alias("list_price")))

    # ---- orders (semi-structured JSON with nested items)
    b_orders = spark.read.format("delta").load(cfg.path("bronze", "orders"))
    orders = _dedup(b_orders).withColumn("event_month", F.date_format("event_date", "yyyy-MM"))
    orders = orders.withColumn("n_items", F.size("items")).withColumn(
        "units", F.aggregate("items", F.lit(0), lambda acc, x: acc + x["qty"]))
    orders.persist()
    _write(orders.drop("items"), cfg.path("silver", "orders"), "event_month")

    items = (orders.select("order_id", "event_id", "customer_pid", "event_time", "event_date", "event_month",
                           "campaign_id", F.posexplode("items").alias("line_no", "it"))
             .select("*", F.col("it.product_id").alias("product_id"), F.col("it.qty").alias("qty"),
                     F.col("it.unit_price").alias("unit_price")).drop("it")
             .withColumn("line_revenue", F.round(F.col("qty") * F.col("unit_price"), 2))
             .join(prod, "product_id", "left")
             .withColumn("category", F.coalesce("category", F.lit("unknown"))))
    _write(items, cfg.path("silver", "order_items"), "event_month")

    # ---- reviews (free text: the unstructured source) and clicks (live only)
    b_reviews = spark.read.format("delta").load(cfg.path("bronze", "reviews"))
    reviews = (_dedup(b_reviews).join(prod.select("product_id", "category", "subcategory"), "product_id", "left")
               .withColumn("event_month", F.date_format("event_date", "yyyy-MM")))
    _write(reviews, cfg.path("silver", "reviews"), "event_month")

    b_clicks = spark.read.format("delta").load(cfg.path("bronze", "clicks"))
    clicks = _dedup(b_clicks).join(prod.select("product_id", "category"), "product_id", "left")
    _write(clicks, cfg.path("silver", "clicks"), "event_date")

    stats.update({
        "bronze_orders": b_orders.count(), "silver_orders": spark.read.format("delta").load(cfg.path("silver", "orders")).count(),
        "silver_order_items": spark.read.format("delta").load(cfg.path("silver", "order_items")).count(),
        "bronze_reviews": b_reviews.count(), "silver_reviews": spark.read.format("delta").load(cfg.path("silver", "reviews")).count(),
        "bronze_clicks": b_clicks.count(), "silver_clicks": spark.read.format("delta").load(cfg.path("silver", "clicks")).count(),
        "unknown_product_lines": spark.read.format("delta").load(cfg.path("silver", "order_items")).where("category='unknown'").count(),
    })
    orders.unpersist()
    stats["seconds"] = round(time.time() - t0, 1)
    print(f"[silver] orders {stats['bronze_orders']:,} -> {stats['silver_orders']:,} (dedup), "
          f"items {stats['silver_order_items']:,}, reviews {stats['bronze_reviews']:,} -> {stats['silver_reviews']:,}, "
          f"clicks {stats['bronze_clicks']:,} -> {stats['silver_clicks']:,}, unknown products {stats['unknown_product_lines']} "
          f"({stats['seconds']} s)", flush=True)
    return stats
