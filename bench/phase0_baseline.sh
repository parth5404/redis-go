#!/usr/bin/env bash
# Phase 0 — baseline capture.
#
# Runs the identical redis-benchmark workload against redis-go and against
# real redis-server, sequentially (never concurrently — they would compete
# for the same 8 CPUs and both numbers would be garbage).
#
# GODEBUG=asyncpreemptoff=1 is required for redis-go until Known Issue 1.0
# (EINTR treated as fatal) is fixed. Every number below is therefore taken
# under a non-default runtime configuration — that caveat ships with the table.
cd "$(dirname "$0")/.." || exit 1

N=10000
C=10
OUT=bench/phase0_raw.txt
: >"$OUT"

kill_by_cmdline() { # $1 = substring to match in /proc/*/cmdline
  for pid in /proc/[0-9]*; do
    if tr '\0' ' ' <"$pid/cmdline" 2>/dev/null | grep -q "$1"; then
      kill -9 "${pid#/proc/}" 2>/dev/null
    fi
  done
}

section() { echo | tee -a "$OUT"; echo "=== $* ===" | tee -a "$OUT"; }

# ---------- redis-go ----------
section "redis-go @ 7378  (GODEBUG=asyncpreemptoff=1)"
GODEBUG=asyncpreemptoff=1 ./bench/redisgo >bench/phase0_server.log 2>&1 &
GO_PID=$!
sleep 1.5

for depth in 1 8 64; do
  if [ "$depth" = "1" ]; then P=""; else P="-P $depth"; fi
  echo "--- redis-go pipeline depth $depth ---" | tee -a "$OUT"
  # shellcheck disable=SC2086
  redis-benchmark -p 7378 -t set,get -n "$N" -c "$C" $P -q 2>&1 | tee -a "$OUT"
  if ! kill -0 "$GO_PID" 2>/dev/null; then
    echo "!! redis-go DIED at depth $depth" | tee -a "$OUT"
    break
  fi
done

# pprof snapshots while the server is still up and warm
if kill -0 "$GO_PID" 2>/dev/null; then
  echo "--- capturing pprof (10s cpu profile under load) ---" | tee -a "$OUT"
  ( sleep 0.5; redis-benchmark -p 7378 -t set,get -n 200000 -c 10 -q >/dev/null 2>&1 ) &
  LOAD=$!
  curl -s -o bench/cpu.pprof "http://localhost:6060/debug/pprof/profile?seconds=10"
  curl -s -o bench/heap.pprof "http://localhost:6060/debug/pprof/heap"
  curl -s -o bench/goroutine.pprof "http://localhost:6060/debug/pprof/goroutine?debug=1"
  wait "$LOAD" 2>/dev/null
  ls -la bench/*.pprof | tee -a "$OUT"
fi

kill -9 "$GO_PID" 2>/dev/null
kill_by_cmdline 'bench/redisgo'
sleep 1

# ---------- real redis ----------
section "real redis-server 8.8.0 @ 6399"
redis-server --port 6399 --daemonize yes --save '' --appendonly no >/dev/null 2>&1
sleep 1

for depth in 1 8 64; do
  if [ "$depth" = "1" ]; then P=""; else P="-P $depth"; fi
  echo "--- redis pipeline depth $depth ---" | tee -a "$OUT"
  # shellcheck disable=SC2086
  redis-benchmark -p 6399 -t set,get -n "$N" -c "$C" $P -q 2>&1 | tee -a "$OUT"
done

redis-cli -p 6399 SHUTDOWN NOSAVE >/dev/null 2>&1
sleep 0.5

echo | tee -a "$OUT"
echo "raw output written to $OUT" | tee -a "$OUT"
