"""Speed-layer fact normalisation (src.streaming.speed_layer.facts) on a static micro-batch."""
import json
from types import SimpleNamespace

from pyspark.sql import functions as F

from src.streaming.speed_layer import ALL, facts

CFG = SimpleNamespace(topic_clicks="clicks", topic_orders="orders", topic_reviews="reviews")
PID = "a" * 64


def _batch(spark, records):
    return spark.createDataFrame([(t, json.dumps(v), 0, i) for i, (t, v) in enumerate(records)],
                                 "topic string, value string, kafka_partition int, kafka_offset long")


def test_facts_per_category_and_all(spark):
    cat_map = F.create_map(F.lit("P1"), F.lit("phones"), F.lit("P2"), F.lit("home"))
    ts = "2026-11-10T10:00:00Z"
    records = [
        ("clicks", {"event_id": "c1", "event_time": ts, "customer_pid": PID, "session_id": "s", "page": "/",
                    "product_id": "P1", "action": "view", "device": "m", "referrer": "g"}),
        ("clicks", {"event_id": "c2", "event_time": ts, "customer_pid": PID, "session_id": "s", "page": "/",
                    "product_id": "P2", "action": "add_to_cart", "device": "m", "referrer": "g"}),
        ("orders", {"event_id": "o1", "event_time": ts, "customer_pid": PID, "order_id": "x",
                    "items": [{"product_id": "P1", "qty": 2, "unit_price": 10.0},
                              {"product_id": "P1", "qty": 1, "unit_price": 5.0},
                              {"product_id": "P2", "qty": 1, "unit_price": 7.0}],
                    "total": 32.0, "payment_method": "card", "campaign_id": None}),
        ("reviews", {"event_id": "r1", "event_time": ts, "customer_pid": PID, "product_id": "P2",
                     "rating": 5, "title": "t", "text": "Excellent, I love it!"}),
        ("clicks", "{broken json"),  # malformed: must not produce facts
    ]
    rows = facts(_batch(spark, records), CFG, cat_map).collect()
    by = {(r.event_id, r.category): r for r in rows}

    assert by[("c1", "phones")].views == 1 and by[("c1", ALL)].views == 1
    assert by[("c2", "home")].add_to_cart == 1 and by[("c2", ALL)].add_to_cart == 1
    # order counted once per category it contains, revenue split by category, total in __all__
    assert by[("o1", "phones")].orders == 1 and by[("o1", "phones")].revenue == 25.0
    assert by[("o1", "home")].orders == 1 and by[("o1", "home")].revenue == 7.0
    assert by[("o1", ALL)].orders == 1 and by[("o1", ALL)].revenue == 32.0
    assert by[("r1", "home")].reviews == 1 and by[("r1", "home")].sent_sum > 0.5
    assert len(rows) == 2 + 2 + 3 + 1
