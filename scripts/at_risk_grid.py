"""Grid over at-risk thresholds, scored against the planted churn cohort (synthetic data only).

    docker compose exec -T spark python scripts/at_risk_grid.py [--as-of 2026-11-09]
"""
import argparse
import datetime as dt

from pyspark.sql import functions as F

from src.common.config import get_config
from src.common.spark_session import build_spark
from src.generator.events import World

ap = argparse.ArgumentParser()
ap.add_argument("--as-of", type=dt.date.fromisoformat, default=dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1))
a = ap.parse_args()
cfg = get_config()
spark = build_spark("at-risk-grid", master="local[3]")
planted = World(cfg.pseudo_salt, cfg.num_customers, cfg.random_seed).churners
d30, d120, d90 = (a.as_of - dt.timedelta(days=n) for n in (30, 120, 90))
o = spark.read.format("delta").load(cfg.path("silver", "orders")).where(F.col("event_date") <= F.lit(a.as_of))
f = o.groupBy("customer_pid").agg(
    F.sum((F.col("event_date") > F.lit(d30)).cast("int")).alias("o30"),
    F.sum(((F.col("event_date") > F.lit(d120)) & (F.col("event_date") <= F.lit(d30))).cast("int")).alias("o90"))
r = (spark.read.format("delta").load(cfg.path("silver", "reviews_scored"))
     .where((F.col("event_date") > F.lit(d90)) & (F.col("event_date") <= F.lit(a.as_of)))
     .groupBy("customer_pid").agg(F.sum((F.col("label") == "neg").cast("int")).alias("neg"), F.avg("compound").alias("avgc")))
pdf = f.join(r, "customer_pid", "full").fillna({"o30": 0, "o90": 0, "neg": 0}).toPandas()
pdf["planted"] = pdf.customer_pid.isin(planted)
print(f"customers={len(pdf)} planted={pdf.planted.sum()}  prior-90d orders: median={pdf.o90.median()}")
print("min_prior drop  neg_thr  flagged  precision  recall")
for min_prior in (3, 5, 8):
    for drop in (0.5, 0.75, 1.0):
        for neg_thr in (-0.3, -0.5, None):
            freq = (pdf.o90 >= min_prior) & ((1 - pdf.o30 / (pdf.o90 / 3)) >= drop)
            voice = (pdf.neg >= 1) & (pdf.avgc <= neg_thr) if neg_thr is not None else False
            flag = freq | voice
            tp = (flag & pdf.planted).sum()
            n = flag.sum()
            print(f"{min_prior:9d} {drop:4.2f} {str(neg_thr):>8s} {n:8d} {tp / n if n else 0:10.3f} {tp / pdf.planted.sum():7.3f}")
spark.stop()
