"""Batch step 5: Delta OPTIMIZE (bin-packing compaction) and Z-ORDER.

Streaming appends one small file per partition per micro-batch (the "small-files problem"),
which slows every later read with per-file open/list overhead on object storage. OPTIMIZE
rewrites them into fewer large files; ZORDER BY clusters rows by category so filters on
category skip files (data skipping via per-file min/max statistics).

Records file counts and a probe-query timing before and after for TEST_LOG.
"""
from __future__ import annotations

import time

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import Config

# table -> (zorder column or None, probe filter)
TABLES = {
    ("bronze", "clicks"): (None, "action = 'add_to_cart'"),
    ("silver", "order_items"): ("category", "category = 'phones'"),
    ("silver", "reviews_scored"): ("category", "category = 'phones'"),
}


def _files(spark: SparkSession, path: str) -> int:
    return spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()["numFiles"]


def _probe(spark: SparkSession, path: str, cond: str, runs: int = 5) -> tuple[float, int]:
    """Median of several runs: single timings at this scale were too noisy (TEST_LOG T08)."""
    times, n = [], 0
    for _ in range(runs):
        t = time.time()
        n = spark.read.format("delta").load(path).where(cond).agg(F.count("*")).first()[0]
        times.append(time.time() - t)
    return round(sorted(times)[runs // 2], 3), n


def run(spark: SparkSession, cfg: Config) -> dict:
    out = {}
    for (layer, name), (zcol, cond) in TABLES.items():
        path = cfg.path(layer, name)
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        before_files = _files(spark, path)
        before_s, n = _probe(spark, path, cond)
        t = time.time()
        opt = DeltaTable.forPath(spark, path).optimize()
        (opt.executeZOrderBy(zcol) if zcol else opt.executeCompaction())
        opt_s = round(time.time() - t, 1)
        after_files = _files(spark, path)
        after_s, n2 = _probe(spark, path, cond)
        out[f"{layer}/{name}"] = {"files_before": before_files, "files_after": after_files, "optimize_s": opt_s,
                                  "probe_s_before": before_s, "probe_s_after": after_s, "probe_rows": n,
                                  "rows_unchanged": n == n2, "zorder": zcol}
        print(f"[optimize] {layer}/{name}: files {before_files} -> {after_files} in {opt_s}s; "
              f"probe '{cond}' {before_s}s -> {after_s}s ({n:,} rows)", flush=True)
    return out
