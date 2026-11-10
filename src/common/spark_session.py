"""SparkSession factory with Delta Lake, S3A (object storage) and Kafka configured.

Jars are baked into the image at /opt/jars (docker/spark/Dockerfile), so no Maven access is
needed at runtime.
"""
from __future__ import annotations

import glob
import os

from pyspark.sql import SparkSession

from src.common.config import Config, get_config


def _jars() -> str:
    jar_dir = os.getenv("SPARK_JARS_DIR", "/opt/jars")
    jars = sorted(glob.glob(os.path.join(jar_dir, "*.jar")))
    if not jars:
        raise RuntimeError(f"No jars found in {jar_dir}; is this running inside the spark image?")
    return ",".join(jars)


def build_spark(app_name: str, cfg: Config | None = None, master: str | None = None,
                extra: dict[str, str] | None = None) -> SparkSession:
    cfg = cfg or get_config()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or cfg.spark_master)
        .config("spark.jars", _jars())
        .config("spark.driver.memory", cfg.spark_driver_memory)
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A -> S3-compatible object store (SeaweedFS locally; S3/ADLS/GCS in production)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", cfg.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Local-scale tuning: default 200 shuffle partitions is far too many for a laptop
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for k, v in (extra or {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
