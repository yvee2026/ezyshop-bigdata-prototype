"""Nightly batch layer: bronze -> silver -> sentiment -> forecast -> at-risk -> optimize.

    python -m src.batch.run_nightly [--as-of YYYY-MM-DD] [--skip-optimize]

as_of defaults to yesterday (UTC): the nightly job processes complete days only; today's
live events are picked up by the next night's run (they are already visible in the speed layer).
A run summary with per-step timings goes to Mongo `batch_metrics` and gold/_runs.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import time

from src.batch import at_risk, bronze_to_silver, forecast, optimize, sentiment
from src.common import mongo
from src.common.config import get_config
from src.common.spark_session import build_spark


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--as-of", type=dt.date.fromisoformat,
                    default=dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1))
    ap.add_argument("--skip-optimize", action="store_true")
    a = ap.parse_args()

    cfg = get_config()
    spark = build_spark("ezyshop-nightly", cfg)
    t0 = time.time()
    summary: dict = {"as_of": str(a.as_of), "started_at": dt.datetime.now(dt.timezone.utc).isoformat()}
    print(f"nightly batch as of {a.as_of} on {spark.sparkContext.master}", flush=True)

    summary["silver"] = bronze_to_silver.run(spark, cfg)
    summary["sentiment"] = sentiment.run(spark, cfg)
    summary["forecast"] = forecast.run(spark, cfg, a.as_of)
    summary["at_risk"] = at_risk.run(spark, cfg, a.as_of)
    if not a.skip_optimize:
        summary["optimize"] = optimize.run(spark, cfg)

    # review_sentiment to Mongo (per product, for the Customer tab)
    rs = spark.read.format("delta").load(cfg.path("gold", "review_sentiment")).toPandas()
    mongo.replace_all("review_sentiment", [dict(r, _id=r["product_id"]) for r in rs.to_dict("records")])
    cw = spark.read.format("delta").load(cfg.path("gold", "category_week_sentiment")).toPandas()
    mongo.replace_all("category_week_sentiment",
                      [dict(r, _id=f"{r['category']}|{r['week_start']}", week_start=str(r["week_start"])) for r in cw.to_dict("records")])

    summary["total_seconds"] = round(time.time() - t0, 1)
    doc = json.loads(json.dumps(summary, default=str))
    mongo.db().batch_metrics.replace_one({"_id": "latest"}, {"_id": "latest", **doc}, upsert=True)
    mongo.db().batch_metrics.insert_one({"type": "run", **doc})
    print(f"nightly batch finished in {summary['total_seconds']} s", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
