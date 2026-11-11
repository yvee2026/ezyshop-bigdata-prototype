"""After `make erase CUSTOMER=X`, confirm the stream wrote no new rows for X (suppression at ingestion).

    docker compose exec -T spark python scripts/check_suppression.py C00042
"""
import sys

from pyspark.sql import functions as F

from src.common import mongo
from src.common.config import get_config
from src.common.pseudonymise import pseudonymise
from src.common.spark_session import build_spark

cid = sys.argv[1] if len(sys.argv) > 1 else "C00042"
cfg = get_config()
pid = pseudonymise(cid, cfg.pseudo_salt)
since = mongo.db().gdpr_suppressed.find_one({"_id": pid})["since"]
spark = build_spark("check-suppression", master="local[2]")
total = 0
for t in ("clicks", "orders", "reviews"):
    n = spark.read.format("delta").load(cfg.path("bronze", t)).where(F.col("customer_pid") == pid).count()
    total += n
    print(f"bronze/{t}: {n} rows for {cid}")
new_kafka = spark.read.format("delta").load(cfg.path("bronze", "clicks")).where(
    (F.col("source") == "kafka") & (F.col("ingest_time") > F.lit(since))).count()
print(f"suppressed since {since}; bronze/clicks rows ingested since then (all customers): {new_kafka}")
print("SUPPRESSION OK" if total == 0 and new_kafka > 0 else "SUPPRESSION CHECK FAILED")
spark.stop()
