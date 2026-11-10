"""Delete streaming checkpoints (and optionally live Mongo collections).

Needed when the streaming query's plan changes incompatibly (Spark refuses to resume a
checkpoint whose source list differs) or before a clean benchmark run.

    python scripts/reset_stream.py [--only live_metrics] [--mongo]
"""
import argparse

from src.common import mongo
from src.common.config import get_config
from src.common.spark_session import build_spark

ap = argparse.ArgumentParser()
ap.add_argument("--only", default=None, help="checkpoint name prefix to delete (default: all)")
ap.add_argument("--mongo", action="store_true", help="also clear live_metrics, live_sentiment, pipeline_metrics")
a = ap.parse_args()

cfg = get_config()
spark = build_spark("reset-stream", master="local[1]")
jvm = spark.sparkContext._jvm
conf = spark.sparkContext._jsc.hadoopConfiguration()
root = jvm.org.apache.hadoop.fs.Path(cfg.path("_checkpoints"))
fs = root.getFileSystem(conf)
if fs.exists(root):
    for st in fs.listStatus(root):
        name = st.getPath().getName()
        if a.only is None or name.startswith(a.only):
            fs.delete(st.getPath(), True)
            print(f"deleted checkpoint {name}")
if a.mongo:
    for c in ("live_metrics", "live_sentiment", "pipeline_metrics"):
        n = mongo.db()[c].delete_many({}).deleted_count
        print(f"cleared mongo {c}: {n} docs")
spark.stop()
