"""Build-time helper: resolve the Spark packages once so runtime never needs Maven.

Starts a throwaway SparkSession with spark.jars.packages, which makes Ivy download the
jars (and their transitive deps) into /opt/ivy/jars. The Dockerfile then moves them to
/opt/jars, and src/common/spark_session.py loads them via spark.jars.
"""
from pyspark.sql import SparkSession

PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    "io.delta:delta-spark_2.12:3.2.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.7.12",
]

spark = (
    SparkSession.builder.master("local[1]")
    .appName("fetch-jars")
    .config("spark.jars.packages", ",".join(PACKAGES))
    .config("spark.jars.ivy", "/opt/ivy")
    .getOrCreate()
)
print("Spark", spark.version, "resolved packages OK")
spark.stop()
