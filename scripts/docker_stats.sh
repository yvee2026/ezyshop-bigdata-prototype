#!/usr/bin/env bash
# Sample `docker stats` for the EzyShop services N times (default 6, every 10 s) while the
# pipeline runs in steady state, and write tests/bench/results/docker_stats.csv + a summary.
# Usage: scripts/docker_stats.sh [samples] [interval_s] [label]
set -euo pipefail
SAMPLES=${1:-6}
INTERVAL=${2:-10}
LABEL=${3:-steady_200eps}
OUT=tests/bench/results/docker_stats_${LABEL}.csv
echo "sample,service,cpu_pct,mem_usage,mem_pct" > "$OUT"
for i in $(seq 1 "$SAMPLES"); do
  docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}' \
    | grep '^ezyshop-' | sed "s/^ezyshop-\([a-z-]*\)-1/\1/; s/%//g; s/^/$i,/" >> "$OUT"
  sleep "$INTERVAL"
done
python3 - "$OUT" <<'EOF'
import csv, sys, statistics as st, re
rows = list(csv.DictReader(open(sys.argv[1])))
def mib(s):
    v, u = re.match(r"([\d.]+)\s*([KMG]i?B)", s.split("/")[0].strip()).groups()
    return float(v) * {"KiB": 1/1024, "MiB": 1, "GiB": 1024, "kB": 1/1000, "MB": 1, "GB": 1000}.get(u, 1)
print(f"{'service':12s} {'cpu% mean':>9s} {'cpu% max':>8s} {'mem MiB mean':>12s}")
for svc in sorted({r['service'] for r in rows}):
    rr = [r for r in rows if r['service'] == svc]
    cpu = [float(r['cpu_pct']) for r in rr]; mem = [mib(r['mem_usage']) for r in rr]
    print(f"{svc:12s} {st.mean(cpu):9.1f} {max(cpu):8.1f} {st.mean(mem):12.0f}")
EOF
