"""Infrastructure smoke test: Spark <-> object store (Delta over S3A), Postgres (JDBC), Kafka.

    docker compose exec -T spark python scripts/smoke_spark.py
"""
import time

from src.common.config import get_config
from src.common.spark_session import build_spark

cfg = get_config()
spark = build_spark("smoke", master="local[2]")
ok = True

# 1. Delta write / read / delete on the object store
path = cfg.path("_smoke", "delta_roundtrip")
t = time.time()
spark.range(1000).withColumnRenamed("id", "n").write.format("delta").mode("overwrite").save(path)
from delta.tables import DeltaTable  # noqa: E402

DeltaTable.forPath(spark, path).delete("n < 10")
n = spark.read.format("delta").load(path).count()
print(f"[delta/s3a] rows after delete = {n} (expect 990) in {time.time() - t:.1f}s")
ok &= n == 990

# 2. JDBC read from Postgres
prod = spark.read.jdbc(cfg.jdbc_url, "products", properties=cfg.jdbc_props)
print(f"[jdbc] products = {prod.count()} (expect 200), categories = {prod.select('category').distinct().count()}")
ok &= prod.count() == 200

# 3. Kafka source reachable (batch read of current offsets)
kdf = (spark.read.format("kafka").option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
       .option("subscribe", f"{cfg.topic_clicks},{cfg.topic_orders},{cfg.topic_reviews}")
       .option("startingOffsets", "earliest").load())
print(f"[kafka] messages currently in topics = {kdf.count()}")

spark.stop()
print("SMOKE OK" if ok else "SMOKE FAILED")
raise SystemExit(0 if ok else 1)
