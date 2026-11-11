"""End-to-end smoke test (needs the running stack, `make init` and `make backfill` done).

Generates N events, runs the speed layer for ~2 minutes on isolated topics / lake prefix /
Mongo DB, then checks bronze and Mongo; finally runs the nightly batch on the main lake and
checks the gold tables and Mongo batch outputs.

    make test-integration
"""
import os
import subprocess
import sys
import time

import pytest
from pyspark.sql import functions as F

from src.common.config import get_config
from tests.bench.common import Isolated, stop

pytestmark = pytest.mark.integration
N_EVENTS = 6000


def test_stream_end_to_end():
    with Isolated("smoke", 3) as iso:
        summ = iso.produce(200, N_EVENTS / 200)                      # ~30 s of traffic
        proc = iso.start_stream("local[3]", 15)
        try:
            iso.wait_for_first_batch()
            deadline = time.time() + 150
            while time.time() < deadline:
                consumed = sum(p["num_input_rows"] for p in iso.progress())
                if consumed >= summ["sent"]:
                    break
                time.sleep(5)
            time.sleep(20)
        finally:
            stop(proc)
        consumed = sum(p["num_input_rows"] for p in iso.progress())
        assert consumed == summ["sent"], f"stream consumed {consumed} of {summ['sent']} messages"
        totals = list(iso.db.live_metrics.aggregate([{"$match": {"category": "__all__"}},
                                                     {"$group": {"_id": None, "orders": {"$sum": "$orders"},
                                                                 "views": {"$sum": "$views"}}}]))[0]
        # duplicates (~1%) are removed, malformed (~0.05%) quarantined: Mongo totals sit just below the raw counts
        assert 0.97 * summ["orders"] <= totals["orders"] <= summ["orders"]
        assert totals["views"] > 0.7 * summ["clicks"]
        assert iso.db.live_sentiment.count_documents({}) > 0
        assert iso.db.pipeline_metrics.count_documents({"type": "batch"}) > 0

        # bronze rows (+ quarantine) account for every Kafka message
        code = ("from src.common.config import get_config; from src.common.spark_session import build_spark;"
                "c=get_config(); s=build_spark('smoke-check', master='local[2]');"
                "n=sum(s.read.format('delta').load(c.path('bronze',t)).count() for t in ('clicks','orders','reviews'));"
                "q=s.read.format('delta').load(c.path('bronze','_quarantine')).count(); print('COUNTS', n, q)")
        out = subprocess.run([sys.executable, "-c", code], env=iso.env, capture_output=True, text=True, check=True).stdout
        n, q = (int(x) for x in [ln for ln in out.splitlines() if ln.startswith("COUNTS")][0].split()[1:])
        assert n + q == summ["sent"]
        assert q >= summ.get("malformed", 0)


def test_nightly_batch_outputs():
    cfg = get_config()
    subprocess.run([sys.executable, "-m", "src.batch.run_nightly", "--skip-optimize"] +
                   (["--as-of", os.environ["SMOKE_AS_OF"]] if os.getenv("SMOKE_AS_OF") else []),
                   check=True, capture_output=True)
    from src.common import mongo
    from src.common.spark_session import build_spark
    spark = build_spark("smoke-gold", master="local[2]")
    for t in ("daily_category_sales", "review_sentiment", "category_week_features", "forecast", "at_risk_customers",
              "reorder_signals"):
        assert spark.read.format("delta").load(cfg.path("gold", t)).count() > 0, f"gold/{t} empty"
    fc = spark.read.format("delta").load(cfg.path("gold", "forecast"))
    assert fc.select("category").distinct().count() == 8
    assert fc.groupBy("category").count().where(F.col("count") != 4).count() == 0   # 4 weeks per category
    m = mongo.db().forecasts.find_one({"_id": "__metrics__"})
    assert m and 0 < m["one_step"][m["best_model"]]["accuracy"] <= 1
    assert mongo.db().forecasts.count_documents({}) == 9
    assert mongo.db().at_risk_customers.count_documents({}) > 0
    assert mongo.db().review_sentiment.count_documents({}) == 200
    spark.stop()
