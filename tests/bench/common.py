"""Shared helpers for the benchmarks: isolated topics / lake prefix / Mongo DB per run, and
subprocess control of the speed layer and the generator. Run inside the spark container."""
from __future__ import annotations

import datetime as dt
import json
import os
import signal
import subprocess
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic
from pymongo import MongoClient

from src.common.config import get_config

RESULTS = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS, exist_ok=True)

# reference palette (fixed order) for the charts
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
INK, INK2, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#fcfcfb"


class Isolated:
    """Topics, lake prefix and Mongo database that only this benchmark run uses."""

    def __init__(self, name: str, partitions: int):
        self.cfg = get_config()
        self.run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")
        self.name = name
        self.partitions = partitions
        self.topics = {t: f"bench_{self.run_id}_{name}_{t}" for t in ("clicks", "orders", "reviews")}
        self.mongo_db = f"ezyshop_bench_{name}"
        self.env = dict(os.environ, TOPIC_CLICKS=self.topics["clicks"], TOPIC_ORDERS=self.topics["orders"],
                        TOPIC_REVIEWS=self.topics["reviews"], LAKE_PREFIX=f"bench/{self.run_id}/{name}",
                        MONGO_DB=self.mongo_db, KAFKA_PARTITIONS=str(partitions))
        self.admin = AdminClient({"bootstrap.servers": self.cfg.kafka_bootstrap})
        self.db = MongoClient(self.cfg.mongo_uri)[self.mongo_db]

    def __enter__(self):
        self.db.client.drop_database(self.mongo_db)
        fs = self.admin.create_topics([NewTopic(t, num_partitions=self.partitions, replication_factor=1)
                                       for t in self.topics.values()])
        for f in fs.values():
            f.result()
        return self

    def __exit__(self, *exc):
        fs = self.admin.delete_topics(list(self.topics.values()))
        for f in fs.values():
            try:
                f.result()
            except Exception:
                pass

    def start_stream(self, master: str, trigger: int, max_offsets: int = 200_000, log: str | None = None,
                     bronze: bool = True):
        out = open(log or os.devnull, "w")
        cmd = [sys.executable, "-m", "src.streaming.speed_layer", "--master", master, "--trigger", str(trigger),
               "--max-offsets", str(max_offsets), "--starting-offsets", "earliest"]
        if not bronze:
            cmd.append("--no-bronze")
        return subprocess.Popen(cmd, env=self.env, stdout=out, stderr=subprocess.STDOUT)

    def produce(self, eps: float, duration: float, burst: bool = False, late: bool = True) -> dict:
        cmd = [sys.executable, "-m", "src.generator.produce", "--eps", str(eps), "--duration", str(duration)]
        if burst:
            cmd.append("--burst")
        if not late:
            cmd.append("--no-late")
        out = subprocess.run(cmd, env=self.env, capture_output=True, text=True, check=True).stdout
        line = [ln for ln in out.splitlines() if ln.startswith("summary ")][-1]
        return json.loads(line[len("summary "):])

    def wait_for_first_batch(self, timeout: int = 240) -> None:
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self.db.pipeline_metrics.count_documents({"type": "progress", "query": "live_metrics"}) > 0:
                return
            time.sleep(2)
        raise TimeoutError("stream produced no progress")

    def progress(self, query: str = "live_metrics") -> list[dict]:
        return list(self.db.pipeline_metrics.find({"type": "progress", "query": query}, {"_id": 0}).sort("ts", 1))


def stop(proc: subprocess.Popen, timeout: int = 90) -> None:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout)
    except subprocess.TimeoutExpired:
        proc.kill()


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def style_axes(ax, title: str, xlabel: str, ylabel: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=INK, fontsize=12, fontweight="bold")
    ax.set_xlabel(xlabel, color=INK2)
    ax.set_ylabel(ylabel, color=INK2)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.tick_params(colors=MUTED)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color("#c3c2b7")
