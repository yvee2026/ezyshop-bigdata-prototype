#!/usr/bin/env bash
# End-to-end demo, run inside the spark container by `make demo` (after up, init, backfill).
# 1. starts the speed layer in the background
# 2. streams normal traffic, then a short Black Friday burst
# 3. runs the nightly batch
# The dashboard (http://localhost:8501) fills while this runs.
set -euo pipefail
cd /app
EPS=${EVENTS_PER_SEC:-200}
LOG=/tmp/ezyshop_demo_stream.log

echo "== [1/4] starting speed layer (log: $LOG)"
python -m src.streaming.speed_layer --run-seconds 420 > "$LOG" 2>&1 &
STREAM=$!
trap 'kill -TERM $STREAM 2>/dev/null || true' EXIT
sleep 40

echo "== [2/4] normal traffic: ${EPS} events/s for 120 s  (open http://localhost:8501 now)"
python -m src.generator.produce --eps "$EPS" --duration 120

echo "== [3/4] Black Friday burst: x${BURST_MULTIPLIER:-10} for 90 s"
python -m src.generator.produce --eps "$EPS" --burst --duration 90

echo "   letting the speed layer drain ..."
sleep 75
kill -TERM $STREAM 2>/dev/null || true
wait $STREAM 2>/dev/null || true
trap - EXIT
python scripts/verify_counts.py 2>/dev/null | tail -1

echo "== [4/4] nightly batch layer"
python -m src.batch.run_nightly

echo "== demo finished. Dashboard: http://localhost:8501  ·  try: make erase CUSTOMER=C00042"
