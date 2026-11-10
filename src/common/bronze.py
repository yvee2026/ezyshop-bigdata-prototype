"""Parse + validate raw JSON events into bronze rows; route bad records to quarantine.

Shared by the speed layer (Kafka source) and the backfill (NDJSON source) so both write
identical bronze schemas.

Input: a DataFrame with a string column `value` (the raw message) and optionally
`kafka_partition`, `kafka_offset`. Output: (good, bad) DataFrames.
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.common.schemas import CLICK_ACTIONS, REQUIRED_FIELDS, SCHEMAS


def validity(topic: str, j: str = "j", ts: str = "_ts") -> Column:
    """Boolean rule set for one topic; `j` is the parsed struct column, `ts` the parsed timestamp."""
    req = [F.col(f"{j}.{c}").isNotNull() for c in REQUIRED_FIELDS[topic]]
    rules = req + [F.col(ts).isNotNull()]
    if topic == "clicks":
        rules.append(F.col(f"{j}.action").isin(*CLICK_ACTIONS))
    elif topic == "orders":
        rules += [F.col(f"{j}.total") >= 0, F.size(f"{j}.items") > 0]
    elif topic == "reviews":
        rules.append(F.col(f"{j}.rating").between(1, 5))
    return reduce(lambda a, b: a & b, rules)


def parse(raw: DataFrame, topic: str, source: str) -> tuple[DataFrame, DataFrame]:
    schema = SCHEMAS[topic]
    for c in ("kafka_partition", "kafka_offset"):
        if c not in raw.columns:
            raw = raw.withColumn(c, F.lit(None).cast("int" if c == "kafka_partition" else "long"))
    df = (raw.withColumn("j", F.from_json("value", schema))
             .withColumn("_ts", F.to_timestamp("j.event_time"))
             .withColumn("_ok", F.coalesce(validity(topic), F.lit(False))))

    fields = [f.name for f in schema.fields if f.name != "event_time"]
    good = (df.where("_ok")
              .select(*[F.col(f"j.{f}").alias(f) for f in fields],
                      F.col("_ts").alias("event_time"),
                      F.to_date("_ts").alias("event_date"),
                      F.current_timestamp().alias("ingest_time"),
                      F.lit(source).alias("source"),
                      "kafka_partition", "kafka_offset"))
    bad = (df.where(~F.col("_ok"))
             .select(F.lit(topic).alias("topic"),
                     F.col("value").alias("raw_value"),
                     F.when(F.col("j.event_id").isNull(), "unparseable_or_missing_id")
                      .when(F.col("_ts").isNull(), "bad_event_time")
                      .otherwise("failed_validation").alias("reason"),
                     F.current_timestamp().alias("ingest_time"),
                     F.current_date().alias("event_date"),
                     F.lit(source).alias("source"),
                     "kafka_partition", "kafka_offset"))
    return good, bad
