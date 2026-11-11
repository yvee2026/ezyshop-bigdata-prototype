"""Latency benchmark: event time -> visible in MongoDB, at normal load and in burst mode.

    python -m tests.bench.latency [--normal-eps 200] [--phase 300]

Production settings (3 partitions, local[3], 30 s trigger, 1-min windows, 10-min watermark) on
isolated topics / lake prefix / Mongo DB. Two phases: normal load, then Black Friday burst
(rate x BURST_MULTIPLIER). Late-event injection is switched off here, because those events are
back-dated on purpose and would measure the generator rather than the pipeline.

For every live micro-batch the StreamingQueryListener records the event-time min/avg/max of
the batch's input and when the batch (including its Mongo upsert) finished, giving per batch:
  newest = finish - max(event_time)   (best case: freshness of the newest event)
  avg    = finish - avg(event_time)   (typical event)
  oldest = finish - min(event_time)   (worst event in the batch, an upper bound per event)
We report p50 / p95 / max over batches and compare them with the 5-minute (300 s) target.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import time

import numpy as np

from tests.bench.common import RESULTS, SERIES, INK, Isolated, stop, style_axes, utcnow

TRIGGER_S = 30
TARGET_S = 300


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--normal-eps", type=int, default=200)
    ap.add_argument("--phase", type=int, default=300)
    a = ap.parse_args()

    phases = []
    with Isolated("lat", 3) as iso:
        proc = iso.start_stream("local[3]", TRIGGER_S, max_offsets=int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "200000")),
                                log=os.path.join(RESULTS, "latency_stream.log"))
        try:
            iso.produce(a.normal_eps, 20, late=False)
            iso.wait_for_first_batch()
            for name, burst in (("normal", False), ("burst", True)):
                s = utcnow()
                summ = iso.produce(a.normal_eps, a.phase, burst=burst, late=False)
                phases.append({"phase": name, "start": s, "end": utcnow(),
                               "offered_eps": round(summ["sent"] / summ["elapsed_s"])})
                print(f"{name}: offered {phases[-1]['offered_eps']}/s for {a.phase}s", flush=True)
            time.sleep(2 * TRIGGER_S + 15)  # drain: the batches holding the burst's tail
        finally:
            stop(proc)
        prog = iso.progress()

    rows = []
    for p in prog:
        if not p.get("num_input_rows"):
            continue
        ts = p["ts"].replace(tzinfo=phases[0]["start"].tzinfo)
        phase = next((ph["phase"] for ph in phases if ph["start"] < ts <= ph["end"] + __import__("datetime").timedelta(seconds=2 * TRIGGER_S)), None)
        if phase is None or ts <= phases[0]["start"]:
            continue
        rows.append({"phase": phase, "ts": ts.isoformat(), "batch_id": p["batch_id"], "num_input_rows": p["num_input_rows"],
                     "trigger_s": round(p["duration_ms"].get("triggerExecution", 0) / 1000, 2),
                     "latency_newest_s": round(p["latency_newest_s"], 1), "latency_avg_s": round(p["latency_avg_s"], 1),
                     "latency_oldest_s": round(p["latency_oldest_s"], 1)})
    with open(os.path.join(RESULTS, "latency.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    summary = []
    for ph in phases:
        rr = [r for r in rows if r["phase"] == ph["phase"]]
        avg = np.array([r["latency_avg_s"] for r in rr])
        old = np.array([r["latency_oldest_s"] for r in rr])
        new = np.array([r["latency_newest_s"] for r in rr])
        summary.append({
            "phase": ph["phase"], "offered_eps": ph["offered_eps"], "batches": len(rr),
            "events_per_batch_avg": round(float(np.mean([r["num_input_rows"] for r in rr]))),
            "batch_time_s_max": max(r["trigger_s"] for r in rr),
            "newest_p50_s": round(float(np.percentile(new, 50)), 1),
            "avg_p50_s": round(float(np.percentile(avg, 50)), 1), "avg_p95_s": round(float(np.percentile(avg, 95)), 1),
            "oldest_p50_s": round(float(np.percentile(old, 50)), 1), "oldest_p95_s": round(float(np.percentile(old, 95)), 1),
            "max_s": round(float(old.max()), 1), "target_s": TARGET_S, "meets_target": bool(old.max() <= TARGET_S)})
    with open(os.path.join(RESULTS, "latency_summary.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)
    print(json.dumps(summary, indent=2))
    chart(rows, phases, os.path.join(RESULTS, "latency.png"))


def chart(rows, phases, path) -> None:
    import datetime as dt

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    t0 = phases[0]["start"]
    fig, ax = plt.subplots(figsize=(8, 4.6), dpi=150)
    fig.patch.set_facecolor("#fcfcfb")
    x = [(dt.datetime.fromisoformat(r["ts"]) - t0).total_seconds() / 60 for r in rows]
    for i, (col, label) in enumerate([("latency_oldest_s", "oldest event in batch (worst case)"),
                                      ("latency_avg_s", "average event"), ("latency_newest_s", "newest event")]):
        ax.plot(x, [r[col] for r in rows], marker="o", markersize=4, linewidth=2, color=SERIES[[1, 0, 2][i]], label=label)
    ax.axhline(TARGET_S, color=INK, linestyle=":", linewidth=1.5)
    ax.text(0.2, TARGET_S + 6, "5-minute target", color=INK, fontsize=9)
    burst = next(p for p in phases if p["phase"] == "burst")
    bs, be = ((burst["start"] - t0).total_seconds() / 60, (burst["end"] - t0).total_seconds() / 60)
    ax.axvspan(bs, be, color="#e1e0d9", alpha=0.5, linewidth=0)
    ax.text(bs + 0.1, TARGET_S * 0.9, f"burst x10 ({burst['offered_eps']:,} ev/s)", color="#52514e", fontsize=9)
    style_axes(ax, "End-to-end latency: event time → visible in MongoDB", "minutes since start", "seconds")
    ax.set_ylim(0, TARGET_S * 1.15)
    ax.legend(frameon=False, fontsize=9, loc="upper left", bbox_to_anchor=(0, 0.85))
    fig.tight_layout()
    fig.savefig(path)
    print(f"saved {path}")


if __name__ == "__main__":
    main()
