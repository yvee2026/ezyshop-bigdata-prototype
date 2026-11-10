"""Live event stream to Kafka.

    python -m src.generator.produce --eps 200 [--burst] [--duration 120] [--diurnal]

* --eps        target events per second across all three topics (default EVENTS_PER_SEC)
* --burst      Black Friday mode: rate x BURST_MULTIPLIER (default 10)
* --duration   seconds to run; 0 = until Ctrl-C
* --diurnal    modulate the rate by the hour-of-day curve (off by default so demos and
               benchmarks get a predictable rate; the backfill always uses the curve)

Deliberate dirt (for the cleaning logic and the test log):
  ~1%   duplicate event_id (the same message is sent twice, as with at-least-once retries)
  ~0.5% late events (event_time 2-10 minutes in the past)
  ~0.05% malformed messages (truncated JSON or an unparseable timestamp)

Messages are keyed by customer_pid so one customer's events stay ordered in one partition.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import random
import signal
import sys
import time
from collections import Counter, deque

from confluent_kafka import Producer

from src.common.config import get_config
from src.generator.events import World, hour_factor, iso, session_events, to_json

DUP_RATE = 0.01
LATE_RATE = 0.005
MALFORMED_RATE = 0.0005


def make_producer(bootstrap: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,          # broker de-dupes producer retries
        "compression.type": "lz4",
        "linger.ms": 20,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 1_000_000,
        "queue.buffering.max.kbytes": 512 * 1024,
    })


def dirty(topic: str, ev: dict, rng: random.Random, stats: Counter) -> list[tuple[str, bytes, bytes]]:
    """Return 1-2 (topic, key, value) messages for this event, applying dirt injection."""
    key = ev["customer_pid"].encode()
    r = rng.random()
    if r < MALFORMED_RATE:
        stats["malformed"] += 1
        if rng.random() < 0.5:
            return [(topic, key, to_json(ev)[: rng.randint(5, 40)])]          # truncated JSON
        bad = dict(ev, event_time="not-a-timestamp")
        return [(topic, key, to_json(bad))]                                  # bad timestamp
    if r < MALFORMED_RATE + LATE_RATE:
        stats["late"] += 1
        late = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=rng.randint(120, 600))
        ev = dict(ev, event_time=iso(late))
    msg = (topic, key, to_json(ev))
    if rng.random() < DUP_RATE:
        stats["duplicates"] += 1
        return [msg, msg]
    return [msg]


def run(eps: float, duration: float, burst: bool, diurnal: bool, max_events: int | None = None,
        quiet: bool = False) -> Counter:
    cfg = get_config()
    rng = random.Random(cfg.random_seed + int(time.time()))
    world = World(cfg.pseudo_salt, cfg.num_customers, cfg.random_seed)
    producer = make_producer(cfg.kafka_bootstrap)
    topics = {"clicks": cfg.topic_clicks, "orders": cfg.topic_orders, "reviews": cfg.topic_reviews}
    rate = eps * (cfg.burst_multiplier if burst else 1)

    stats: Counter = Counter()
    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    if not quiet:
        print(f"producing to {cfg.kafka_bootstrap}: target {rate:,.0f} events/s"
              f"{' (BURST x' + str(cfg.burst_multiplier) + ')' if burst else ''}"
              f"{', diurnal' if diurnal else ''}; duration={'inf' if not duration else duration}s",
              flush=True)

    buf: deque = deque()
    start = last_report = time.monotonic()
    sent_at_report = 0
    while not stop["flag"]:
        now_m = time.monotonic()
        elapsed = now_m - start
        if duration and elapsed >= duration:
            break
        if max_events and stats["sent"] >= max_events:
            break
        eff = rate * (hour_factor(dt.datetime.now(dt.timezone.utc).hour) if diurnal else 1.0)
        due = int(eff * elapsed) - stats["sent"] if not diurnal else int(eff * 0.05)
        n_this_tick = 0
        while due > 0 and (not max_events or stats["sent"] < max_events):
            if not buf:
                buf.extend(session_events(world, dt.datetime.now(dt.timezone.utc), rng))
            topic, ev = buf.popleft()
            for t, k, v in dirty(topic, ev, rng, stats):
                while True:
                    try:
                        producer.produce(topics[t], key=k, value=v)
                        break
                    except BufferError:
                        producer.poll(0.05)
                stats["sent"] += 1
                stats[t] += 1
                due -= 1
                n_this_tick += 1
        producer.poll(0)
        if now_m - last_report >= 5 and not quiet:
            r = (stats["sent"] - sent_at_report) / (now_m - last_report)
            print(f"[{elapsed:6.0f}s] sent={stats['sent']:>9,} rate={r:8,.0f}/s  clicks={stats['clicks']:,} "
                  f"orders={stats['orders']:,} reviews={stats['reviews']:,} dup={stats['duplicates']} "
                  f"late={stats['late']} bad={stats['malformed']}", flush=True)
            last_report, sent_at_report = now_m, stats["sent"]
        if n_this_tick == 0 or diurnal:
            time.sleep(0.05 if diurnal else 0.005)

    producer.flush(30)
    stats["elapsed_s"] = round(time.monotonic() - start, 2)
    if not quiet:
        print("summary " + json.dumps(dict(stats)), flush=True)
    return stats


def main(argv: list[str] | None = None) -> None:
    cfg = get_config()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--eps", type=float, default=cfg.events_per_sec)
    ap.add_argument("--duration", type=float, default=0)
    ap.add_argument("--burst", action="store_true")
    ap.add_argument("--diurnal", action="store_true")
    ap.add_argument("--max-events", type=int, default=None)
    a = ap.parse_args(argv)
    run(a.eps, a.duration, a.burst, a.diurnal, a.max_events)


if __name__ == "__main__":
    sys.exit(main())
