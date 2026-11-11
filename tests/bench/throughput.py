"""Throughput (capacity) benchmark: how fast Spark drains a fixed backlog, 1 vs 3 vs 6 cores.

    python -m tests.bench.throughput [--configs 1,3,6] [--events 400000] [--batch 50000]

Controlled design (after the co-located load sweep proved confounded, TEST_LOG T11/T12):
1. Pre-load a fixed backlog of N events into fresh topics with P partitions. The generator
   finishes BEFORE Spark starts, so it does not compete for CPU.
2. Start the speed layer with local[P] (P partitions = P cores) and maxOffsetsPerTrigger = batch.
   With a backlog, micro-batches run back to back, so time is pure processing capacity.
3. Throughput = events in batches 2..k / (end of batch k - end of batch 1). Batch 1 is
   excluded because it includes JVM warm-up and query compilation.
Two variants:
  live_only  the live aggregation query alone (parse, dedup, windows, VADER, Mongo upserts)
  full       live query + bronze Delta writer sharing the cores (the real speed layer)
Writes results/throughput.csv, throughput.png and throughput_summary.json.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import time

from tests.bench.common import INK2, RESULTS, SERIES, Isolated, stop, style_axes, utcnow


def drain(n: int, events: int, batch: int, bronze: bool) -> dict:
    variant = "full" if bronze else "live_only"
    with Isolated(f"cap{n}_{variant}", n) as iso:
        summ = iso.produce(50_000, events / 50_000 + 1, late=True)          # preload, generator then exits
        sent = summ["sent"]
        proc = iso.start_stream(f"local[{n}]", 1, max_offsets=batch, bronze=bronze,
                                log=os.path.join(RESULTS, f"throughput_p{n}_{variant}.log"))
        t0, status = time.time(), "ok"
        try:
            while True:
                if proc.poll() is not None:
                    status = f"stream exited with code {proc.returncode}"
                    break
                live = sum(p["num_input_rows"] for p in iso.progress())
                done_b = (not bronze) or sum(p["num_input_rows"] for p in iso.progress("bronze_events")) >= sent
                if live >= sent and done_b:
                    break
                if time.time() - t0 > 900:
                    status = "timeout"
                    break
                time.sleep(2)
            profile = stage_profile(n, variant)
        finally:
            stop(proc)

        def rate(query: str):
            bs = [p for p in iso.progress(query) if p["num_input_rows"] > 0]
            if len(bs) < 3:
                return None, len(bs)
            span = (bs[-1]["ts"] - bs[0]["ts"]).total_seconds()
            return round(sum(p["num_input_rows"] for p in bs[1:]) / span), len(bs)

        live_rate, nb = rate("live_metrics")
        bronze_rate, _ = rate("bronze_events") if bronze else (None, 0)
        row = {"partitions": n, "master": f"local[{n}]", "variant": variant, "events": sent, "batch_cap": batch,
               "batches": nb, "live_events_per_s": live_rate, "bronze_events_per_s": bronze_rate,
               "pipeline_events_per_s": min(x for x in (live_rate, bronze_rate) if x) if live_rate else None,
               "task_time_s": profile.get("task_time_s"), "stage_wall_s": profile.get("stage_wall_s"),
               "tasks": profile.get("tasks"), "jvm_cpu_s": profile.get("jvm_cpu_s"), "gc_s": profile.get("gc_s"),
               "status": status}
        print(f"   {variant:9s} local[{n}]: live {live_rate}/s  bronze {bronze_rate}/s  batches {nb}  ({status})", flush=True)
        return row


def stage_profile(n: int, variant: str) -> dict:
    """Snapshot completed stages from the Spark UI REST API (driver UI on :4040 inside the container):
    total task run time vs. stage wall time shows whether extra cores add parallelism or overhead."""
    import urllib.request
    try:
        base = "http://localhost:4040/api/v1/applications"
        app = json.load(urllib.request.urlopen(base, timeout=5))[0]["id"]
        stages = json.load(urllib.request.urlopen(f"{base}/{app}/stages?status=complete", timeout=10))
    except Exception as exc:  # UI not reachable: profile is optional
        return {"error": str(exc)}
    by_name: dict = {}
    for s in stages:
        k = s["name"].split(" at ")[0]
        d = by_name.setdefault(k, {"stages": 0, "tasks": 0, "task_time_s": 0.0, "cpu_s": 0.0, "gc_s": 0.0, "wall_s": 0.0})
        d["stages"] += 1
        d["tasks"] += s["numTasks"]
        d["task_time_s"] += s.get("executorRunTime", 0) / 1000
        d["cpu_s"] += s.get("executorCpuTime", 0) / 1e9
        d["gc_s"] += s.get("jvmGcTime", 0) / 1000
        if s.get("submissionTime") and s.get("completionTime"):
            import datetime as _dt
            f = "%Y-%m-%dT%H:%M:%S.%fGMT"
            d["wall_s"] += (_dt.datetime.strptime(s["completionTime"], f) - _dt.datetime.strptime(s["submissionTime"], f)).total_seconds()
    with open(os.path.join(RESULTS, f"throughput_stages_p{n}_{variant}.json"), "w") as fh:
        json.dump(by_name, fh, indent=1)
    return {"task_time_s": round(sum(d["task_time_s"] for d in by_name.values()), 1),
            "stage_wall_s": round(sum(d["wall_s"] for d in by_name.values()), 1),
            "tasks": sum(d["tasks"] for d in by_name.values()),
            "jvm_cpu_s": round(sum(d["cpu_s"] for d in by_name.values()), 1),
            "gc_s": round(sum(d["gc_s"] for d in by_name.values()), 1)}


def chart(rows: list[dict], path: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    configs = sorted({r["partitions"] for r in rows})
    fig, ax = plt.subplots(figsize=(8, 4.6), dpi=150)
    fig.patch.set_facecolor("#fcfcfb")
    width = 0.36
    for i, (variant, label) in enumerate([("live_only", "live aggregation only"),
                                          ("full", "full speed layer (live + bronze Delta)")]):
        vals = [next((r["pipeline_events_per_s"] or 0 for r in rows if r["partitions"] == n and r["variant"] == variant), 0)
                for n in configs]
        xs = [c + (i - 0.5) * width for c in range(len(configs))]
        ok = [next((r["status"] == "ok" for r in rows if r["partitions"] == n and r["variant"] == variant), True)
              for n in configs]
        bars = ax.bar(xs, vals, width=width * 0.94, color=SERIES[i], label=label)
        for b, v, good in zip(bars, vals, ok):
            if not good:  # run crashed (JVM OOM-killed): show the partial figure as invalid
                b.set_hatch("///")
                b.set_alpha(0.45)
            ax.annotate(f"{v:,.0f}" if good else f"OOM-killed\n({v:,.0f} before crash)", (b.get_x() + b.get_width() / 2, v),
                        textcoords="offset points", xytext=(0, 3), ha="center", fontsize=9, color=INK2)
    ax.set_xticks(range(len(configs)))
    ax.set_xticklabels([f"{n} partition{'s' if n > 1 else ''}\nlocal[{n}]" for n in configs])
    style_axes(ax, "Speed-layer capacity: events/s drained from a fixed Kafka backlog", "", "events per second")
    ax.legend(frameon=False, fontsize=9, loc="upper left")
    ax.set_ylim(0, max((r["pipeline_events_per_s"] or 0) for r in rows) * 1.2)
    fig.tight_layout()
    fig.savefig(path)
    print(f"saved {path}")


def chart_from_csv() -> None:
    with open(os.path.join(RESULTS, "throughput.csv")) as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["partitions"] = int(r["partitions"])
        r["pipeline_events_per_s"] = int(r["pipeline_events_per_s"]) if r["pipeline_events_per_s"] else None
    chart(rows, os.path.join(RESULTS, "throughput.png"))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--configs", default="1,3,6")
    ap.add_argument("--events", type=int, default=400_000)
    ap.add_argument("--batch", type=int, default=50_000)
    a = ap.parse_args()
    rows = []
    path = os.path.join(RESULTS, "throughput.csv")
    if os.path.exists(path):
        os.remove(path)
    for bronze in (False, True):
        for n in [int(x) for x in a.configs.split(",")]:
            row = drain(n, a.events, a.batch, bronze)
            rows.append(row)
            new = not os.path.exists(path)
            with open(path, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(row))
                if new:
                    w.writeheader()
                w.writerow(row)
    summary = {f"{r['variant']}_p{r['partitions']}": r["pipeline_events_per_s"] for r in rows}
    summary["measured_at"] = utcnow().isoformat()
    json.dump(summary, open(os.path.join(RESULTS, "throughput_summary.json"), "w"), indent=2)
    print(json.dumps(summary, indent=2))
    chart(rows, os.path.join(RESULTS, "throughput.png"))


if __name__ == "__main__":
    main()
