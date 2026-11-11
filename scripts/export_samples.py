"""Write small committed extracts of inputs and outputs to sample_data/.

    docker compose exec -T spark python scripts/export_samples.py

Inputs:  raw events as JSON lines (clicks, orders, reviews; incl. quarantined examples) and the
         Postgres catalogue tables as CSV.
Outputs: live_metrics + live_sentiment (Mongo), gold forecast / forecast_eval / review_sentiment /
         at_risk_customers / reorder_signals (CSV), and the latest batch metrics (JSON).
Only pseudonymised IDs appear in any file.
"""
import json
import os

from pyspark.sql import functions as F

from src.common import mongo
from src.common.config import get_config
from src.common.spark_session import build_spark
from src.generator import catalogue

OUT = "sample_data"
IN_DIR, OUT_DIR = os.path.join(OUT, "inputs"), os.path.join(OUT, "outputs")
os.makedirs(IN_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

cfg = get_config()
spark = build_spark("export-samples", master="local[2]")


def jsonl(df, path, n):
    rows = [json.loads(r) for r in df.limit(n).toJSON().collect()]
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"wrote {path} ({len(rows)} rows)")


def csv(df, path, n=None):
    pdf = (df.limit(n) if n else df).toPandas()
    pdf.to_csv(path, index=False)
    print(f"wrote {path} ({len(pdf)} rows)")


# ---- inputs (live events from Kafka as stored in bronze; drop ingest metadata)
for t, n in (("clicks", 300), ("orders", 200), ("reviews", 200)):
    df = (spark.read.format("delta").load(cfg.path("bronze", t)).where("source='kafka'")
          .drop("ingest_time", "event_date", "source").orderBy(F.desc("event_time")))
    jsonl(df, os.path.join(IN_DIR, f"{t}.jsonl"), n)
q = spark.read.format("delta").load(cfg.path("bronze", "_quarantine")).select("topic", "reason", "raw_value", "kafka_partition", "kafka_offset")
jsonl(q, os.path.join(IN_DIR, "quarantined_examples.jsonl"), 30)
catalogue.export_csv(IN_DIR)

# ---- outputs
live = list(mongo.db().live_metrics.find({}, {"_id": 0}).sort("window_start", -1).limit(300))
with open(os.path.join(OUT_DIR, "live_metrics.json"), "w") as f:
    json.dump(live, f, indent=1, default=str)
sent = list(mongo.db().live_sentiment.find({}, {"_id": 0}).sort("window_start", -1).limit(150))
with open(os.path.join(OUT_DIR, "live_sentiment.json"), "w") as f:
    json.dump(sent, f, indent=1, default=str)
print(f"wrote live_metrics.json ({len(live)}) and live_sentiment.json ({len(sent)})")
for t, n in (("forecast", None), ("forecast_eval", None), ("review_sentiment", None),
             ("at_risk_customers", 300), ("reorder_signals", None), ("category_week_sentiment", None)):
    df = spark.read.format("delta").load(cfg.path("gold", t))
    if t == "at_risk_customers":
        df = df.orderBy(F.desc("score"))
    csv(df, os.path.join(OUT_DIR, f"{t}.csv"), n)
csv(spark.read.format("delta").load(cfg.path("gold", "daily_category_sales")).orderBy(F.desc("event_date")),
    os.path.join(OUT_DIR, "daily_category_sales_recent.csv"), 400)
m = mongo.db().batch_metrics.find_one({"_id": "latest"}, {"_id": 0})
with open(os.path.join(OUT_DIR, "nightly_batch_metrics.json"), "w") as f:
    json.dump(m, f, indent=1, default=str)
fm = mongo.db().forecasts.find_one({"_id": "__metrics__"}, {"_id": 0})
with open(os.path.join(OUT_DIR, "forecast_metrics.json"), "w") as f:
    json.dump(fm, f, indent=1, default=str)
print("wrote nightly_batch_metrics.json, forecast_metrics.json")
spark.stop()
