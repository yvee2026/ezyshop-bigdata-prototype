"""Load sweep: offered load vs. events/s actually processed by Spark (secondary benchmark).

    python -m tests.bench.load_sweep [--configs 1,3,6] [--rates 500,1000,...] [--step 60]

For each configuration (Kafka partitions = Spark local cores = N) the full speed layer runs
(bronze Delta query + live aggregation query to Mongo, sharing the cores) on isolated topics,
lake prefix and Mongo DB. The generator offers increasing rates in steps. Per step we record:

  offered   events produced per second (generator summary)
  processed events per second the live query consumed during the step
            (sum of numInputRows of the micro-batches that ended in the step / step length)
  lag       produced - processed, cumulatively, at the end of the step

When processed < offered and the lag keeps growing, that configuration is saturated; its
capacity is the highest processed rate observed. Writes results/load_sweep.csv (after every
step, so a crash keeps earlier rows) and results/load_sweep.png.

Caveat (TEST_LOG T11/T12): the generator runs in the same container as Spark and competes for
CPU, and the shared Docker VM limits memory, so this is NOT a clean scaling test. The controlled
capacity test is tests/bench/throughput.py.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import time

from tests.bench.common import RESULTS, SERIES, Isolated, MUTED, stop, style_axes, utcnow

TRIGGER_S = 10
MAX_OFFSETS = int(os.getenv("BENCH_MAX_OFFSETS", "100000"))


def run_config(n: int, rates: list[int], step: int) -> list[dict]:
    rows = []
    with Isolated(f"p{n}", n) as iso:
        print(f"== config {n} partition(s) / local[{n}]: topics {list(iso.topics.values())[0]}…", flush=True)
        # maxOffsetsPerTrigger bounds every micro-batch. The first full run used 1,000,000 and the
        # Spark JVM was OOM-killed on the backlog batches (TEST_LOG T12); the configured cap is used now.
        proc = iso.start_stream(f"local[{n}]", TRIGGER_S, max_offsets=MAX_OFFSETS,
                                log=os.path.join(RESULTS, f"sweep_p{n}.log"))
        try:
            iso.produce(50, 10)                # warm-up traffic so the first batches compile/JIT
            iso.wait_for_first_batch()
            time.sleep(15)
            base = sum(p["num_input_rows"] for p in iso.progress())
            cum_prod, cum_proc = 0, 0
            for rate in rates:
                if proc.poll() is not None:
                    raise RuntimeError(f"speed layer exited with code {proc.returncode} (see sweep_p{n}.log)")
                # steps are back to back (no drain pause): a pause inside the window under-counted
                # the processed rate in the dry run (347/s "processed" at 500/s offered, lag only ~2 s)
                s = utcnow()
                summ = iso.produce(rate, step)
                e = utcnow()
                prog = [p for p in iso.progress() if s < p["ts"].replace(tzinfo=s.tzinfo) <= e]
                bronze = [p for p in iso.progress("bronze_events") if s < p["ts"].replace(tzinfo=s.tzinfo) <= e]
                processed = sum(p["num_input_rows"] for p in prog)
                cum_prod += summ["sent"]
                cum_proc = sum(p["num_input_rows"] for p in iso.progress()) - base
                spark_rate = [p["processed_rows_per_s"] for p in prog if p["num_input_rows"] > 0]
                row = {"partitions": n, "master": f"local[{n}]", "offered_eps": round(summ["sent"] / summ["elapsed_s"]),
                       "target_eps": rate, "processed_eps": round(processed / (e - s).total_seconds()),
                       "bronze_processed_eps": round(sum(p["num_input_rows"] for p in bronze) / (e - s).total_seconds()),
                       "lag_events": max(0, cum_prod - cum_proc), "batches": len(prog),
                       "max_batch_s": round(max([p["duration_ms"].get("triggerExecution", 0) for p in prog] or [0]) / 1000, 1),
                       "spark_processed_rows_per_s_avg": round(sum(spark_rate) / len(spark_rate)) if spark_rate else 0}
                rows.append(row)
                _append_csv(row)
                print(f"   offered {row['offered_eps']:>6}/s  processed {row['processed_eps']:>6}/s  "
                      f"bronze {row['bronze_processed_eps']:>6}/s  lag {row['lag_events']:>8}  "
                      f"max batch {row['max_batch_s']} s", flush=True)
                if row["lag_events"] > 3 * rate * step:   # hopelessly behind: stop climbing
                    break
        finally:
            stop(proc)
    return rows


def _append_csv(row: dict) -> None:
    path = os.path.join(RESULTS, "load_sweep.csv")
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row))
        if new:
            w.writeheader()
        w.writerow(row)


def chart(rows: list[dict], path: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=150)
    fig.patch.set_facecolor("#fcfcfb")
    configs = sorted({r["partitions"] for r in rows})
    top = max(r["offered_eps"] for r in rows)
    ax.plot([0, top], [0, top], color=MUTED, linestyle="--", linewidth=1.2, label="processed = offered")
    for i, n in enumerate(configs):
        rr = [r for r in rows if r["partitions"] == n]
        good = [r for r in rr if not r.get("note")]
        ax.plot([r["offered_eps"] for r in good], [r["processed_eps"] for r in good], marker="o", markersize=6,
                linewidth=2, color=SERIES[i], label=f"{n} partition{'s' if n > 1 else ''} / local[{n}]")
        for r in rr:
            if r.get("note"):   # the step during which the JVM was OOM-killed
                ax.plot([r["offered_eps"]], [r["processed_eps"]], marker="x", markersize=10, color=SERIES[i])
                ax.annotate("OOM-killed", (r["offered_eps"], r["processed_eps"]), textcoords="offset points",
                            xytext=(6, 4), fontsize=9, color="#52514e")
    style_axes(ax, "Load sweep (generator co-located): offered vs processed events/s", "offered load (events/s)",
               "processed by Spark (events/s)")
    ax.set_xlim(0, top * 1.05)
    ax.set_ylim(0, top * 1.05)
    ax.legend(frameon=False, fontsize=9, loc="upper left")
    fig.tight_layout()
    fig.savefig(path)
    print(f"saved {path}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--configs", default="1,3,6")
    ap.add_argument("--rates", default="500,1000,2000,4000,8000")
    ap.add_argument("--step", type=int, default=90)
    a = ap.parse_args()
    rows = []
    for n in [int(x) for x in a.configs.split(",")]:
        try:
            rows += run_config(n, [int(x) for x in a.rates.split(",")], a.step)
        except RuntimeError as exc:   # e.g. JVM OOM-killed: keep the rows measured so far
            print(f"config {n} aborted: {exc}", flush=True)
    summary = {}
    for n in sorted({r["partitions"] for r in rows}):
        rr = [r for r in rows if r["partitions"] == n]
        keeps_up = [r["offered_eps"] for r in rr if r["lag_events"] <= 2 * TRIGGER_S * r["offered_eps"]]
        summary[f"p{n}"] = {"capacity_eps": max(r["processed_eps"] for r in rr),
                            "max_offered_without_backlog_eps": max(keeps_up) if keeps_up else 0}
    summary["measured_at"] = utcnow().isoformat()
    json.dump(summary, open(os.path.join(RESULTS, "load_sweep_summary.json"), "w"), indent=2)
    print(json.dumps(summary, indent=2))
    chart(rows, os.path.join(RESULTS, "load_sweep.png"))


if __name__ == "__main__":
    main()
