#!/usr/bin/env bash
# Known Issue 1 (EINTR) — isolated reproduction.
#
# Uses PING only: PING never touches the store, so the 200-key eviction
# deadlock cannot interfere. Anything that kills the server here is the
# EpollWait/EINTR path in server/async_tcp.go:60 and nothing else.
cd "$(dirname "$0")/.." || exit 1

run_trial() {
  local label="$1" godebug="$2" logfile="$3" n="$4"
  if [ -n "$godebug" ]; then
    GODEBUG="$godebug" ./bench/redisgo >"$logfile" 2>&1 &
  else
    ./bench/redisgo >"$logfile" 2>&1 &
  fi
  local srv=$!
  sleep 1

  local cnt=0
  for i in $(seq 1 "$n"); do
    # one fresh connection per command: maximises epoll wakeups, and each
    # redis-cli invocation gives the Go runtime a chance to preempt.
    if redis-cli -p 7378 -t 2 PING >/dev/null 2>&1; then
      cnt=$((cnt + 1))
    else
      break
    fi
  done

  local state="DEAD"
  if kill -0 "$srv" 2>/dev/null; then
    state="ALIVE"
    kill -9 "$srv" 2>/dev/null
  fi
  wait "$srv" 2>/dev/null
  printf '%-26s pings_ok=%-5s server=%s\n' "$label" "$cnt" "$state"
  sleep 1
}

echo "### Group A: no GODEBUG  (hypothesis: dies early)"
run_trial "A1 default-runtime" "" bench/eintr_a1.log 300
run_trial "A2 default-runtime" "" bench/eintr_a2.log 300
run_trial "A3 default-runtime" "" bench/eintr_a3.log 300

echo
echo "### Group B: GODEBUG=asyncpreemptoff=1  (hypothesis: survives)"
run_trial "B1 preempt-off" "asyncpreemptoff=1" bench/eintr_b1.log 300
run_trial "B2 preempt-off" "asyncpreemptoff=1" bench/eintr_b2.log 300

echo
echo "### Fatal line from each group-A log"
for f in bench/eintr_a1.log bench/eintr_a2.log bench/eintr_a3.log; do
  printf '%-22s %s\n' "$(basename "$f")" "$(tail -1 "$f")"
done
