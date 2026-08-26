#!/usr/bin/env bash
# Phase 1.1 — deadlock reproduction with proof.
#
# config.KeyLimit = 200. On the insert that crosses that limit:
#   core/store.go:29  Put()                 -> RWmutex.Lock()      (held)
#   core/store.go:32  evict()
#   core/eviction.go  evictAllkeysRandom()
#   core/eviction.go  Del(k)
#   core/store.go:53  Del()                 -> RWmutex.Lock()      (blocks forever)
#
# sync.RWMutex is not reentrant, so the goroutine waits on a lock it already
# holds. Proof comes from the pprof goroutine dump: pprof runs on its own
# net/http goroutine and stays responsive while the epoll loop is wedged.
cd "$(dirname "$0")/.." || exit 1

rm -f appendonly.aof            # start from an empty keyspace
GODEBUG=asyncpreemptoff=1 ./bench/redisgo >bench/deadlock_server.log 2>&1 &
SRV=$!
sleep 1.5

echo "KeyLimit is 200 — inserting 260 distinct keys, reporting where it wedges."
LAST_OK=0
for i in $(seq 1 260); do
  if redis-cli -p 7378 -t 2 SET "dl:$i" "v$i" >/dev/null 2>&1; then
    LAST_OK=$i
  else
    echo "  -> first non-responding SET at key #$i (last successful: #$LAST_OK)"
    break
  fi
done

echo
if kill -0 "$SRV" 2>/dev/null; then
  echo "process state: ALIVE (not a crash — it is a hang)"
else
  echo "process state: DEAD (unexpected for this bug)"
fi

echo
echo "--- is the server still answering anything? (2s timeout) ---"
if timeout 3 redis-cli -p 7378 -t 2 PING 2>&1 | grep -q PONG; then
  echo "PING answered — NOT deadlocked"
else
  echo "PING did not answer — event loop is wedged"
fi

echo
echo "--- pprof goroutine dump: main epoll goroutine ---"
curl -s "http://localhost:6060/debug/pprof/goroutine?debug=2" -o bench/deadlock_goroutine.txt
if [ -s bench/deadlock_goroutine.txt ]; then
  # the wedged goroutine is the one sitting in semacquire under Lock()
  awk '/^goroutine 1 /,/^$/' bench/deadlock_goroutine.txt | head -30
  echo
  echo "--- every goroutine blocked on a mutex ---"
  grep -cE 'semacquire|sync\.\(\*RWMutex\)\.Lock' bench/deadlock_goroutine.txt \
    | xargs -I{} echo "frames matching semacquire/RWMutex.Lock: {}"
else
  echo "pprof did not respond"
fi

kill -9 "$SRV" 2>/dev/null
wait "$SRV" 2>/dev/null
echo
echo "full dump saved to bench/deadlock_goroutine.txt"
