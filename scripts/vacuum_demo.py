"""Show that VACUUM is what makes an erasure physical: time travel works after DELETE, fails after VACUUM.

    docker compose exec -T spark python scripts/vacuum_demo.py C00043
"""
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common.config import get_config
from src.common.pseudonymise import pseudonymise
from src.common.spark_session import build_spark
from src.gdpr.erase_customer import erase

cid = sys.argv[1] if len(sys.argv) > 1 else "C00043"
cfg = get_config()
spark = build_spark("vacuum-demo", master="local[2]")
path = cfg.path("bronze", "orders")
pid = pseudonymise(cid, cfg.pseudo_salt)
v_before = DeltaTable.forPath(spark, path).history(1).first()["version"]
print(f"bronze/orders rows for {cid} before erasure: "
      f"{spark.read.format('delta').load(path).where(F.col('customer_pid') == pid).count()} (table version {v_before})")
erase(spark, cfg, cid, vacuum_now=True, quiet=True)
print(f"after DELETE + VACUUM, current rows: {spark.read.format('delta').load(path).where(F.col('customer_pid') == pid).count()}")
try:
    n = spark.read.format("delta").option("versionAsOf", v_before).load(path).where(F.col("customer_pid") == pid).count()
    print(f"time travel to version {v_before}: still returned {n} rows")
except Exception as exc:
    print(f"time travel to version {v_before} now FAILS: {type(exc).__name__}: {str(exc).splitlines()[0][:160]}")
spark.stop()
