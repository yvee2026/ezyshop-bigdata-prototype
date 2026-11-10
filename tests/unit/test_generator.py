import datetime as dt
import json
import random
from collections import Counter

from src.generator import catalogue as cat
from src.generator.events import World, session_events
from src.generator.produce import dirty

SALT = "unit-test-salt"


def test_catalogue_size_and_determinism():
    p1, p2 = cat.build_products(), cat.build_products()
    assert len(p1) == 200 and len({p.category for p in p1}) == 8
    assert p1 == p2  # same IDs and prices every run


def test_black_friday_dates():
    assert cat.black_friday(2025) == dt.date(2025, 11, 28)
    assert cat.black_friday(2026) == dt.date(2026, 11, 27)


def test_events_have_schema_fields_and_no_raw_ids():
    w = World(SALT, 200, seed=1)
    rng = random.Random(3)
    now = dt.datetime(2026, 11, 10, 12, 0, tzinfo=dt.timezone.utc)
    events = [e for _ in range(500) for e in session_events(w, now, rng)]
    topics = Counter(t for t, _ in events)
    assert topics["clicks"] > topics["orders"] > 0 and topics["reviews"] > 0
    raw_ids = {c.customer_id for c in w.customers}
    for t, e in events:
        assert {"event_id", "event_time", "customer_pid"} <= e.keys()
        assert len(e["customer_pid"]) == 64
        assert not (set(json.dumps(e).split('"')) & raw_ids), "raw customer_id leaked"
        if t == "orders":
            assert abs(sum(i["qty"] * i["unit_price"] for i in e["items"]) - e["total"]) < 0.01
        if t == "reviews":
            assert 1 <= e["rating"] <= 5 and e["text"]


def test_session_events_never_in_the_future():
    w = World(SALT, 100, seed=2)
    rng = random.Random(5)
    now = dt.datetime(2026, 11, 10, 12, 0, tzinfo=dt.timezone.utc)
    for _ in range(300):
        for _t, e in session_events(w, now, rng):
            ts = dt.datetime.fromisoformat(e["event_time"].replace("Z", "+00:00"))
            assert ts <= now


def test_dirt_rates_roughly_as_configured():
    w = World(SALT, 100, seed=4)
    rng = random.Random(9)
    stats = Counter()
    now = dt.datetime.now(dt.timezone.utc)
    n = 0
    while n < 40000:
        for t, e in session_events(w, now, rng):
            dirty(t, e, rng, stats)
            n += 1
    assert 0.007 < stats["duplicates"] / n < 0.013
    assert 0.003 < stats["late"] / n < 0.007
    assert 0 < stats["malformed"] / n < 0.0015
