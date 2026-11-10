"""GDPR erasure removes a customer from every Delta table and Mongo (needs the running stack).

    make test-integration     (or: docker compose exec -T spark python -m pytest -q tests/integration -k gdpr)
"""
import pytest
from pyspark.sql import functions as F

from src.common import mongo
from src.common.config import get_config
from src.common.pseudonymise import pseudonymise
from src.common.spark_session import build_spark
from src.gdpr.erase_customer import DELTA_TABLES, QUARANTINE, erase

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def lake_spark():
    s = build_spark("test-gdpr", master="local[2]")
    yield s
    s.stop()


def _pick_customer_with_data(spark, cfg):
    """Choose a customer who has orders in silver AND reviews, so erasure has real work to do."""
    from src.generator.catalogue import customer_ids
    orders = spark.read.format("delta").load(cfg.path("silver", "orders")).select("customer_pid").distinct()
    reviews = spark.read.format("delta").load(cfg.path("silver", "reviews")).select("customer_pid").distinct()
    both = {r.customer_pid for r in orders.join(reviews, "customer_pid").limit(2000).collect()}
    for cid in reversed(customer_ids()):  # from the end, so the demo customer C00042 is left alone
        if pseudonymise(cid, cfg.pseudo_salt) in both:
            return cid
    pytest.skip("no customer with both orders and reviews; run make backfill && make nightly first")


def test_erasure_removes_customer_everywhere(lake_spark):
    cfg = get_config()
    cid = _pick_customer_with_data(lake_spark, cfg)
    pid = pseudonymise(cid, cfg.pseudo_salt)
    report = erase(lake_spark, cfg, cid, quiet=True)

    assert sum(v["before"] for v in report["delta"].values()) > 0, "customer had no rows to erase"
    for t in DELTA_TABLES:
        n = lake_spark.read.format("delta").load(cfg.path(*t.split("/"))).where(F.col("customer_pid") == pid).count()
        assert n == 0, f"{t} still has {n} rows"
    q = lake_spark.read.format("delta").load(cfg.path(*QUARANTINE.split("/")))
    assert q.where(F.col("raw_value").contains(pid)).count() == 0
    assert mongo.db().at_risk_customers.count_documents({"customer_pid": pid}) == 0
    assert mongo.db().gdpr_suppressed.count_documents({"_id": pid}) == 1
    audit = mongo.db().gdpr_audit.find_one({"customer_pid": pid})
    assert audit and cid not in str(audit), "audit must not contain the raw customer ID"
    # without VACUUM the old version is still readable: the documented reason VACUUM is required
    assert any(v["time_travel_before_vacuum"] > 0 for v in report["delta"].values())
