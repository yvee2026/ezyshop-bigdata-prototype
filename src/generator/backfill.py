"""Backfill 18 months of historical orders and reviews into bronze Delta.

    python -m src.generator.backfill [--days 548] [--orders-per-day 1000]

Demand model per category and day (so the forecast has real structure to learn):
    orders = base_c * trend * weekday * seasonal(Nov/Dec, BF, EOFY) * campaign uplift
             * quality-shock dip * log-normal noise
Two further signals are planted on purpose and documented in the README:
* quality shocks: per category, 1-2 periods where review mood drops, and demand dips
  about a week later (gives "prior-week sentiment" a genuine lead over demand);
* a churn cohort (~8% of customers) whose purchase rate collapses in the final 45 days and
  whose reviews turn negative (gives the at-risk model something real to find).

Events are generated as raw JSON lines, then parsed by the same src.common.bronze code as the
live stream. Re-running is idempotent: rows with source='backfill' are deleted first.
"""
from __future__ import annotations

import argparse
import datetime as dt
import math
import os
import random
import shutil
import time

from src.common.config import get_config
from src.generator import catalogue as cat
from src.generator.events import HOURLY, World, order_event, review_event, seasonal_factor, to_json

WEEKDAY = [0.95, 0.90, 0.95, 1.00, 1.05, 1.12, 1.03]  # Mon..Sun
CHURN_SHARE = 0.08
CHURN_DAYS = 45
TMP = "/tmp/ezyshop_backfill"


def quality_shocks(rng: random.Random, start: dt.date, days: int) -> dict[str, list[tuple[dt.date, dt.date]]]:
    shocks = {}
    for c in cat.CATEGORIES:
        periods = []
        for _ in range(rng.randint(1, 2)):
            s = start + dt.timedelta(days=rng.randint(60, days - 120))
            periods.append((s, s + dt.timedelta(days=rng.randint(21, 42))))
        shocks[c] = periods
    return shocks


def in_shock(day: dt.date, periods, lag_days: int = 0) -> bool:
    return any(s + dt.timedelta(days=lag_days) <= day <= e + dt.timedelta(days=lag_days) for s, e in periods)


def generate(days: int, orders_per_day: float, out_dir: str, seed: int) -> dict:
    cfg = get_config()
    rng = random.Random(seed)
    world = World(cfg.pseudo_salt, cfg.num_customers, cfg.random_seed)
    end = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)
    start = end - dt.timedelta(days=days - 1)
    churn_cut = end - dt.timedelta(days=CHURN_DAYS)
    churners = {c.customer_pid for c in rng.sample(world.customers, int(CHURN_SHARE * len(world.customers)))}
    shocks = quality_shocks(rng, start, days)
    total_w = sum(world.cat_weights)
    hour_cum = list(__import__("itertools").accumulate(HOURLY))

    os.makedirs(out_dir, exist_ok=True)
    fo = open(os.path.join(out_dir, "orders.jsonl"), "wb")
    fr = open(os.path.join(out_dir, "reviews.jsonl"), "wb")
    n_orders = n_reviews = 0
    for i in range(days):
        day = start + dt.timedelta(days=i)
        trend = 1.0 + 0.25 * i / days
        base_day = orders_per_day * trend * WEEKDAY[day.weekday()] * seasonal_factor(day)
        for c_name, w in zip(world.cat_names, world.cat_weights):
            camp = [x.discount_pct for x in world.active_on(day) if x.category == c_name]
            uplift = 1 + 0.012 * max(camp) if camp else 1.0
            dip = 0.80 if in_shock(day, shocks[c_name], lag_days=7) else 1.0
            lam = base_day * w / total_w * uplift * dip * rng.lognormvariate(0, 0.06)
            n = max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
            for _ in range(n):
                cust = world.pick_customer(rng)
                if day > churn_cut and cust.customer_pid in churners and rng.random() < 0.9:
                    cust = world.pick_customer(rng)  # churner mostly does not buy any more
                    if cust.customer_pid in churners:
                        continue
                hour = rng.choices(range(24), cum_weights=hour_cum)[0]
                ts = dt.datetime(day.year, day.month, day.day, hour, rng.randint(0, 59), rng.randint(0, 59),
                                 rng.randint(0, 999) * 1000, tzinfo=dt.timezone.utc)
                p = world.pick_product(rng, c_name)
                fo.write(to_json(order_event(world, cust, p, ts, rng)) + b"\n")
                n_orders += 1
                if rng.random() < 0.07:
                    rts = ts + dt.timedelta(days=rng.randint(2, 20), hours=rng.randint(0, 12))
                    if rts.date() <= end:
                        shift = -0.9 if in_shock(rts.date(), shocks[c_name]) else 0.0
                        if cust.customer_pid in churners and rts.date() > end - dt.timedelta(days=90):
                            shift -= 0.8
                        fr.write(to_json(review_event(cust, p, rts, rng, mood_shift=shift)) + b"\n")
                        n_reviews += 1
    fo.close()
    fr.close()
    return {"start": str(start), "end": str(end), "orders": n_orders, "reviews": n_reviews,
            "churners": len(churners), "shocks": {k: [(str(s), str(e)) for s, e in v] for k, v in shocks.items()}}


def load_to_bronze(out_dir: str) -> dict:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    from src.common.bronze import parse
    from src.common.spark_session import build_spark

    cfg = get_config()
    spark = build_spark("backfill")
    counts = {}
    for topic in ("orders", "reviews"):
        raw = spark.read.text(f"file://{out_dir}/{topic}.jsonl")
        good, bad = parse(raw, topic, source="backfill")
        path = cfg.path("bronze", topic)
        if DeltaTable.isDeltaTable(spark, path):
            DeltaTable.forPath(spark, path).delete("source = 'backfill'")   # idempotent re-run
        (good.repartition("event_date").write.format("delta").mode("append")
             .partitionBy("event_date").option("mergeSchema", "true").save(path))
        counts[topic] = spark.read.format("delta").load(path).where("source='backfill'").count()
        counts[f"{topic}_bad"] = bad.count()
        d = spark.read.format("delta").load(path).agg(F.min("event_date"), F.max("event_date")).first()
        print(f"bronze/{topic}: {counts[topic]:,} backfill rows, {d[0]} .. {d[1]}, rejected={counts[f'{topic}_bad']}")
    spark.stop()
    return counts


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=548)           # ~18 months
    ap.add_argument("--orders-per-day", type=float, default=1000)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    t0 = time.time()
    shutil.rmtree(TMP, ignore_errors=True)
    info = generate(a.days, a.orders_per_day, TMP, a.seed)
    print(f"generated {info['orders']:,} orders and {info['reviews']:,} reviews "
          f"({info['start']} .. {info['end']}) in {time.time() - t0:.0f}s; churn cohort={info['churners']}")
    for c, periods in info["shocks"].items():
        print(f"  quality shock {c:12s} {periods}")
    t1 = time.time()
    load_to_bronze(TMP)
    print(f"loaded to bronze in {time.time() - t1:.0f}s (total {time.time() - t0:.0f}s)")
    shutil.rmtree(TMP, ignore_errors=True)


if __name__ == "__main__":
    main()
