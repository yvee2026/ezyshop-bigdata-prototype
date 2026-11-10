"""GDPR Art. 17 right to erasure for one customer.

    python -m src.gdpr.erase_customer --customer-id C00042 [--vacuum-now]

1. The raw customer ID is hashed with the secret salt; the raw ID itself is never stored.
2. DELETE every row with that customer_pid from every Delta table in the lake (bronze,
   silver, gold, including quarantined raw messages that contain the pseudonym), from MongoDB
   serving collections, and from the Postgres orders table. Counts are printed before and after.
3. The pseudonym goes on a suppression list; the speed layer drops new events for it at
   ingestion, so the data does not flow straight back in.
4. An audit record (pseudonym, time, counts; no raw ID) goes to Mongo `gdpr_audit`.

Why VACUUM matters: a Delta DELETE writes new files without the rows and marks the old files
as removed in the transaction log. The old Parquet files stay on storage so time travel
still works, which means the "deleted" rows can still be read with VERSION AS OF. The script
demonstrates this. Only VACUUM physically removes the old files, once they are older than
the retention period (default 7 days: delta.deletedFileRetentionDuration). So the erasure
SLA is "DELETE now + VACUUM within the retention window", and time travel cannot reach back
past an erasure. --vacuum-now forces RETAIN 0 HOURS for the demo; never do that while other
jobs read or write the table (it can delete files a concurrent reader still needs).

Kafka: messages are keyed by the pseudonym, and topics keep 24 h (KAFKA_LOG_RETENTION_HOURS),
so the broker copy ages out within a day. That bound is documented as part of the erasure SLA.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common import mongo
from src.common.config import Config, get_config
from src.common.pseudonymise import pseudonymise
from src.common.spark_session import build_spark

DELTA_TABLES = [
    "bronze/clicks", "bronze/orders", "bronze/reviews",
    "silver/clicks", "silver/orders", "silver/order_items", "silver/reviews", "silver/reviews_scored",
    "gold/at_risk_customers",
]
QUARANTINE = "bronze/_quarantine"
MONGO_COLLECTIONS = ["at_risk_customers"]


def _count(spark: SparkSession, path: str, cond) -> int:
    return spark.read.format("delta").load(path).where(cond).count()


def erase(spark: SparkSession, cfg: Config, customer_id: str, vacuum_now: bool = False, quiet: bool = False) -> dict:
    pid = pseudonymise(customer_id, cfg.pseudo_salt)
    report: dict = {"customer_pid": pid, "requested_at": dt.datetime.now(dt.timezone.utc).isoformat(), "delta": {}}
    log = (lambda *a: None) if quiet else print
    log(f"erasing customer_pid={pid[:16]}… (raw ID is hashed with the salt and never stored)")

    # suppression first, so the stream stops adding new rows while we delete
    mongo.db().gdpr_suppressed.update_one({"_id": pid}, {"$set": {"since": dt.datetime.now(dt.timezone.utc)}}, upsert=True)

    tables = [(t, F.col("customer_pid") == pid) for t in DELTA_TABLES]
    tables.append((QUARANTINE, F.col("raw_value").contains(pid)))
    for t, cond in tables:
        path = cfg.path(*t.split("/"))
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        dtab = DeltaTable.forPath(spark, path)
        before = _count(spark, path, cond)
        version_before = dtab.history(1).first()["version"]
        if before:
            dtab.delete(cond)
        after = _count(spark, path, cond)
        # time travel to the version before the delete still sees the rows until VACUUM
        tt = (spark.read.format("delta").option("versionAsOf", version_before).load(path).where(cond).count()
              if before else 0)
        report["delta"][t] = {"before": before, "after": after, "time_travel_before_vacuum": tt}
        log(f"  {t:28s} rows before {before:5d}  after {after:3d}  still readable via time travel: {tt}")

    report["mongo"] = {}
    for c in MONGO_COLLECTIONS:
        before = mongo.db()[c].count_documents({"customer_pid": pid})
        mongo.db()[c].delete_many({"customer_pid": pid})
        report["mongo"][c] = {"before": before, "after": mongo.db()[c].count_documents({"customer_pid": pid})}
        log(f"  mongo.{c:22s} docs before {before:5d}  after {report['mongo'][c]['after']:3d}")

    import psycopg2
    with psycopg2.connect(cfg.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM orders WHERE customer_pid = %s", (pid,))
        report["postgres_orders_deleted"] = cur.rowcount
    log(f"  postgres.orders               rows deleted {report['postgres_orders_deleted']}")

    if vacuum_now:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        for t, cond in tables:
            path = cfg.path(*t.split("/"))
            if t in report["delta"] and report["delta"][t]["before"]:
                DeltaTable.forPath(spark, path).vacuum(0)
        log("  VACUUM RETAIN 0 HOURS done: old files physically removed; time travel before the delete now fails")
        report["vacuumed"] = True
    else:
        log("  VACUUM not forced: old files are removed by the scheduled VACUUM after the 7-day retention window")
        report["vacuumed"] = False

    report["remaining_rows"] = sum(v["after"] for v in report["delta"].values()) + sum(v["after"] for v in report["mongo"].values())
    mongo.db().gdpr_audit.insert_one(json.loads(json.dumps(report, default=str)))
    log(f"erasure complete: {sum(v['before'] for v in report['delta'].values())} Delta rows removed, "
        f"{report['remaining_rows']} remaining")
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--customer-id", required=True, help="raw customer ID, e.g. C00042")
    ap.add_argument("--vacuum-now", action="store_true", help="demo only: VACUUM RETAIN 0 HOURS")
    a = ap.parse_args()
    cfg = get_config()
    spark = build_spark("gdpr-erasure", cfg, master="local[2]")
    erase(spark, cfg, a.customer_id, a.vacuum_now)
    spark.stop()


if __name__ == "__main__":
    main()
