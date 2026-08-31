#!/usr/bin/env bash
#
# Reproduces the throughput numbers quoted in the README.
#
# Builds the server, runs it on a scratch port, drives it with redis-benchmark
# and prints a summary. Pass --compare to run the identical workload against a
# real redis-server for a side-by-side figure.
#
#   ./bench/bench.sh
#   ./bench/bench.sh --compare
#
set -euo pipefail

PORT="${PORT:-7999}"
CLIENTS="${CLIENTS:-50}"
REQUESTS="${REQUESTS:-100000}"
RUNS="${RUNS:-3}"
COMPARE=0
[[ "${1:-}" == "--compare" ]] && COMPARE=1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="$(mktemp -d)"
BIN="$WORKDIR/redisgo"

cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  [[ -n "${REDIS_PID:-}" ]] && kill "$REDIS_PID" 2>/dev/null || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

command -v redis-benchmark >/dev/null || {
  echo "redis-benchmark not found; install redis-tools" >&2
  exit 1
}

echo "building..."
(cd "$ROOT" && go build -o "$BIN" .)

# Run from a scratch directory so the benchmark never loads or overwrites a
# real appendonly.aof.
(cd "$WORKDIR" && "$BIN" --port "$PORT" >"$WORKDIR/server.log" 2>&1) &
SERVER_PID=$!

for _ in $(seq 30); do
  redis-cli -p "$PORT" PING >/dev/null 2>&1 && break
  sleep 0.1
done
redis-cli -p "$PORT" PING >/dev/null 2>&1 || { echo "server did not come up" >&2; exit 1; }

# One warm-up pass: the first run pays for connection setup and for the Go
# runtime reaching a steady state, and is not representative.
redis-benchmark -p "$PORT" -t set,get -c "$CLIENTS" -n 10000 -q >/dev/null 2>&1

bench_target() {
  local label="$1" port="$2"
  echo
  echo "=== $label  (-c $CLIENTS -n $REQUESTS) ==="
  for run in $(seq "$RUNS"); do
    printf 'run %d: ' "$run"
    # redis-benchmark draws its progress meter with carriage returns, so split
    # on those first and keep only the final summary lines.
    redis-benchmark -p "$port" -t set,get -c "$CLIENTS" -n "$REQUESTS" -q 2>/dev/null |
      tr '\r' '\n' |
      awk '/requests per second/ {printf "%-5s %10.0f ops/sec  %s   ", $1, $2, $(NF-1)} END {print ""}'
  done
}

bench_target "redis-go" "$PORT"

if [[ "$COMPARE" == "1" ]]; then
  if command -v redis-server >/dev/null; then
    redis-server --port $((PORT + 1)) --save '' --appendonly no --daemonize no >/dev/null 2>&1 &
    REDIS_PID=$!
    sleep 1
    bench_target "real redis-server" "$((PORT + 1))"
  else
    echo
    echo "redis-server not installed; skipping the comparison"
  fi
fi

echo
echo "note: p50 is reported by redis-benchmark itself; the numbers in the"
echo "README were taken on an otherwise idle machine."
