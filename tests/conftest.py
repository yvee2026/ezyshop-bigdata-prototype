import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("PSEUDO_SALT", "unit-test-salt")


@pytest.fixture(scope="session")
def spark():
    """Small local SparkSession without S3/Kafka, for pure-transformation tests."""
    from pyspark.sql import SparkSession

    s = (SparkSession.builder.master("local[2]").appName("unit-tests")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
