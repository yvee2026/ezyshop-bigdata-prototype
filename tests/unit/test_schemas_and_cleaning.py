"""Event schema validation and dirty-data routing (src.common.bronze.parse)."""
import json

from src.common.bronze import parse

GOOD_CLICK = {"event_id": "e1", "event_time": "2026-11-10T10:00:00.123Z", "customer_pid": "a" * 64,
              "session_id": "s", "page": "/p/P0001", "product_id": "P0001", "action": "view",
              "device": "mobile", "referrer": "google"}
GOOD_ORDER = {"event_id": "o1", "event_time": "2026-11-10T10:00:00Z", "customer_pid": "b" * 64,
              "order_id": "x", "items": [{"product_id": "P0001", "qty": 2, "unit_price": 10.5}],
              "total": 21.0, "payment_method": "card", "campaign_id": None}


def _df(spark, values):
    return spark.createDataFrame([(v,) for v in values], "value string")


def test_valid_click_parsed_and_typed(spark):
    good, bad = parse(_df(spark, [json.dumps(GOOD_CLICK)]), "clicks", "test")
    rows = good.collect()
    assert bad.count() == 0 and len(rows) == 1
    r = rows[0]
    assert str(r.event_time) == "2026-11-10 10:00:00.123000" and str(r.event_date) == "2026-11-10"
    assert r.source == "test" and r.action == "view"


def test_malformed_and_invalid_records_go_to_quarantine(spark):
    values = [
        json.dumps(GOOD_CLICK)[:25],                                  # truncated JSON
        json.dumps(dict(GOOD_CLICK, event_time="not-a-timestamp")),   # bad timestamp
        json.dumps(dict(GOOD_CLICK, action="teleport")),              # invalid enum
        json.dumps({k: v for k, v in GOOD_CLICK.items() if k != "product_id"}),  # missing field
        json.dumps(GOOD_CLICK),
    ]
    good, bad = parse(_df(spark, values), "clicks", "test")
    assert good.count() == 1
    reasons = sorted(r.reason for r in bad.collect())
    assert reasons == ["bad_event_time", "failed_validation", "failed_validation", "unparseable_or_missing_id"]


def test_order_rules(spark):
    neg = dict(GOOD_ORDER, event_id="o2", total=-5.0)
    empty = dict(GOOD_ORDER, event_id="o3", items=[])
    good, bad = parse(_df(spark, [json.dumps(GOOD_ORDER), json.dumps(neg), json.dumps(empty)]), "orders", "test")
    assert [r.event_id for r in good.collect()] == ["o1"] and bad.count() == 2


def test_review_rating_range(spark):
    base = {"event_id": "r", "event_time": "2026-11-10T10:00:00Z", "customer_pid": "c" * 64,
            "product_id": "P0001", "title": "t", "text": "fine"}
    vals = [json.dumps(dict(base, event_id=f"r{i}", rating=i)) for i in (0, 1, 5, 6)]
    good, bad = parse(_df(spark, vals), "reviews", "test")
    assert sorted(r.rating for r in good.collect()) == [1, 5] and bad.count() == 2


def test_duplicates_survive_bronze_but_are_identifiable(spark):
    # bronze keeps data as received; dedup happens in silver / the stream on event_id
    good, _ = parse(_df(spark, [json.dumps(GOOD_CLICK)] * 2), "clicks", "test")
    assert good.count() == 2 and good.select("event_id").distinct().count() == 1
