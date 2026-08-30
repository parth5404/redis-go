#!/usr/bin/env bash
# verify.sh -- self-grading harness for EXERCISE.md
#
# Usage:  ./exercise/verify.sh [path-to-minired-dir]
# Default path: exercise/minired
#
# Builds your server, starts it on :7380, runs 17 tests, prints a score.
# Nothing here reads your source except test T1 (the `go` statement count).

set -uo pipefail

DIR="${1:-exercise/minired}"
PORT=7380
BIN=/tmp/minired_test
LOG=/tmp/minired_test.log
PID=""

PASS=0
FAIL=0
declare -a FAILED

green() { printf '\033[32m%s\033[0m' "$1"; }
red()   { printf '\033[31m%s\033[0m' "$1"; }
dim()   { printf '\033[2m%s\033[0m'  "$1"; }

ok()   { PASS=$((PASS+1)); printf '  [%s] %s\n' "$(green PASS)" "$1"; }
no()   { FAIL=$((FAIL+1)); FAILED+=("$1"); printf '  [%s] %s\n' "$(red FAIL)" "$1"
         [ $# -gt 1 ] && printf '        %s\n' "$(dim "$2")"; return 0; }

cleanup() {
  [ -n "$PID" ] && kill "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  rm -f "$BIN"
}
trap cleanup EXIT

# ---------------------------------------------------------------- helpers
# raw <hex-free printf string> <bytes-to-read> -- one connection, send, read N bytes
raw() {
  local payload="$1" nbytes="$2"
  exec 3<>"/dev/tcp/127.0.0.1/$PORT" || return 1
  printf '%b' "$payload" >&3
  timeout 3 head -c "$nbytes" <&3
  exec 3<&- 2>/dev/null
  exec 3>&- 2>/dev/null
}

cli() { timeout 3 redis-cli -p "$PORT" "$@" 2>&1; }

# ---------------------------------------------------------------- build
printf '\n== minired verify ==\n\n'

if [ ! -d "$DIR" ]; then
  printf '%s: %s nahi mila\n' "$(red ABORT)" "$DIR"; exit 1
fi

printf 'building %s\n' "$DIR"
if ! (cd "$DIR" && go build -o "$BIN" . 2>&1); then
  printf '\n%s: build fail. Pehle compile karao.\n' "$(red ABORT)"; exit 1
fi
go vet "./$DIR/..." >/dev/null 2>&1 && VET=yes || VET=no

# T1 -- R1: zero goroutines
GOCOUNT=$(cat "$DIR"/*.go 2>/dev/null | grep -vE '^[[:space:]]*//' \
  | grep -cE '(^|[^[:alnum:]_.])go[[:space:]]+([[:alnum:]_.]+\(|func[[:space:]]*\()' || true)
printf '\nstarting server on :%d\n\n' "$PORT"
"$BIN" >"$LOG" 2>&1 &
PID=$!
sleep 0.6
if ! kill -0 "$PID" 2>/dev/null; then
  printf '%s: server turant mar gaya. Log:\n' "$(red ABORT)"; sed 's/^/    /' "$LOG"; exit 1
fi

printf -- '-- correctness --\n'

[ "$GOCOUNT" -eq 0 ] \
  && ok "T1  R1: zero \`go\` statements (single-goroutine reactor)" \
  || no "T1  R1: zero \`go\` statements (single-goroutine reactor)" "$GOCOUNT goroutine spawn mile -- yeh reactor nahi, thread pool hai"

[ "$(cli ping)" = "PONG" ] \
  && ok "T2  PING -> PONG" \
  || no "T2  PING -> PONG" "mila: $(cli ping)"

cli set k1 v1 >/dev/null
[ "$(cli get k1)" = "v1" ] \
  && ok "T3  SET/GET roundtrip" \
  || no "T3  SET/GET roundtrip" "GET k1 = $(cli get k1)"

[ -z "$(cli get nope_missing)" ] \
  && ok "T4  GET missing key -> nil (\$-1\\r\\n)" \
  || no "T4  GET missing key -> nil (\$-1\\r\\n)" "mila: '$(cli get nope_missing)' -- empty bulk aur nil alag hain"

[ "$(cli del k1)" = "1" ] \
  && ok "T5  DEL -> :1" \
  || no "T5  DEL -> :1" "mila: $(cli del k1)"

[ "$(cli echo hello)" = "hello" ] \
  && ok "T6  ECHO" \
  || no "T6  ECHO" "mila: $(cli echo hello)"

case "$(cli frobnicate x)" in
  *ERR*) ok "T7  unknown command -> -ERR" ;;
  *)     no "T7  unknown command -> -ERR" "mila: $(cli frobnicate x)" ;;
esac

[ "$(cli PiNg)" = "PONG" ] \
  && ok "T8  command name case-insensitive" \
  || no "T8  command name case-insensitive" "'PiNg' kaam nahi kiya"

printf -- '\n-- the four things TCP does not promise --\n'

# T9 -- R4: command split across two segments
SPLIT=$(
  exec 3<>"/dev/tcp/127.0.0.1/$PORT" || exit 1
  printf '*1\r\n$4\r\nPIN' >&3
  sleep 0.4
  printf 'G\r\n' >&3
  timeout 3 head -c 7 <&3
)
[ "$(printf '%s' "$SPLIT" | tr -d '\r\n')" = "+PONG" ] \
  && ok "T9  R4: split command (\`*1\\r\\n\$4\\r\\nPIN\` + 400ms + \`G\\r\\n\`)" \
  || no "T9  R4: split command (\`*1\\r\\n\$4\\r\\nPIN\` + 400ms + \`G\\r\\n\`)" "inbound accumulator nahi hai, ya incomplete ko error maan rahe ho"

# T10 -- R5: three commands in one write
PIPE=$(raw '*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n' 21 | tr -d '\r\n')
[ "$PIPE" = "+PONG+PONG+PONG" ] \
  && ok "T10 R5: pipeline -- 3 commands ek write mein -> 3 replies" \
  || no "T10 R5: pipeline -- 3 commands ek write mein -> 3 replies" "mila: '$PIPE' -- parse loop nahi hai, sirf pehla command execute hua"

# T11 -- reactor sanity: ek idle connection doosre ko block na kare
IDLE_OK=no
(
  exec 3<>"/dev/tcp/127.0.0.1/$PORT" || exit 1
  printf '*1\r\n$4\r\nPING\r\n' >&3
  timeout 3 head -c 7 <&3 >/dev/null
  sleep 2.5            # connection khula, chup -- Model A yahin mar jaata hai
) &
BGPID=$!
sleep 0.5
[ "$(cli ping)" = "PONG" ] && IDLE_OK=yes
wait $BGPID 2>/dev/null
[ "$IDLE_OK" = yes ] \
  && ok "T11 2 concurrent clients (ek idle-open, doosra serve hua)" \
  || no "T11 2 concurrent clients (ek idle-open, doosra serve hua)" "sequential server hai -- pehla client doosre ko block kar raha hai"

# T12 -- client gayab (RST), server zinda rahe
(
  exec 3<>"/dev/tcp/127.0.0.1/$PORT" || exit 1
  printf '*3\r\n$3\r\nSET\r\n$1\r\nz\r\n$1\r\n9\r\n' >&3
) 2>/dev/null
sleep 0.3
[ "$(cli ping)" = "PONG" ] \
  && ok "T12 client abrupt disconnect -> server zinda" \
  || no "T12 client abrupt disconnect -> server zinda" "server mar gaya. log: $(tail -2 "$LOG" | tr '\n' ' ')"

# T13 -- garbage frame: connection band ho, server na mare
raw 'GARBAGE~!@#$%^&*\r\n' 1 >/dev/null 2>&1
sleep 0.3
[ "$(cli ping)" = "PONG" ] \
  && ok "T13 malformed frame -> server zinda" \
  || no "T13 malformed frame -> server zinda" "server mar gaya. log: $(tail -2 "$LOG" | tr '\n' ' ')"

printf -- '\n-- performance / hardening --\n'

# T14 -- no busy polling: idle CPU must be ~0
jiffies() { awk '{print $14+$15}' "/proc/$PID/stat" 2>/dev/null || echo 0; }
J0=$(jiffies); sleep 3; J1=$(jiffies)
BURN=$((J1-J0))               # jiffies over 3s; 300 = one full core
[ "$BURN" -lt 15 ] \
  && ok "T14 idle par ~0% CPU (${BURN}/300 jiffies over 3s)" \
  || no "T14 idle par ~0% CPU (${BURN}/300 jiffies over 3s)" "busy-polling kar rahe ho -- EpollWait ko -1 timeout do, 0 nahi"

# T15 -- R7+R6: Nagle wall. 200 sequential round trips.
T0=$(date +%s%N)
timeout 30 redis-cli -p "$PORT" -r 200 ping >/dev/null 2>&1
T1=$(date +%s%N)
MS=$(( (T1-T0)/1000000 ))
[ "$MS" -lt 2000 ] \
  && ok "T15 R7: 200 sequential round trips = ${MS}ms (Nagle wall nahi)" \
  || no "T15 R7: 200 sequential round trips = ${MS}ms (Nagle wall nahi)" "~8000ms matlab per-request 40ms stall -> TCP_NODELAY missing"

# T16 -- R3: load ke andar zinda rahe (EINTR ka asli test)
if timeout 60 redis-benchmark -p "$PORT" -t set,get -n 20000 -c 50 -q >/tmp/minired_bench.txt 2>&1; then
  if kill -0 "$PID" 2>/dev/null && [ "$(cli ping)" = "PONG" ]; then
    RPS=$(grep -oE '[0-9.]+ requests per second' /tmp/minired_bench.txt | head -1)
    ok "T16 R3: 20k requests / 50 clients ke baad zinda ($RPS)"
  else
    no "T16 R3: 20k requests / 50 clients ke baad zinda" "server load mein mar gaya -- EINTR ko fatal maan rahe ho. log: $(tail -2 "$LOG" | tr '\n' ' ')"
  fi
else
  no "T16 R3: 20k requests / 50 clients ke baad zinda" "benchmark complete nahi hua. log: $(tail -3 "$LOG" | tr '\n' ' ')"
fi

# T17 -- R8: slow reader. Server ka write() EAGAIN dega; usko sambhaalna hai.
head -c 262144 /dev/zero | tr '\0' 'x' > /tmp/minired_big
timeout 5 redis-cli -p "$PORT" -x set big < /tmp/minired_big >/dev/null 2>&1
R8=$(timeout 60 python3 - "$PORT" <<'PY' 2>&1
import socket, sys, time
port = int(sys.argv[1])
N, VAL = 60, 262144
s = socket.create_connection(("127.0.0.1", port))
s.sendall(b'*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n' * N)     # ~15 MB of replies maango
time.sleep(1.5)                                        # aur padho mat
try:                                                   # doosra client abhi bhi chale?
    o = socket.create_connection(("127.0.0.1", port), timeout=3)
    o.sendall(b'*1\r\n$4\r\nPING\r\n')
    if o.recv(16) != b'+PONG\r\n':
        print("OTHER_CLIENT_STALLED"); sys.exit()
    o.close()
except Exception as e:
    print("OTHER_CLIENT_STALLED:%s" % e); sys.exit()
want = N * (len(b'$262144\r\n') + VAL + 2)
got = 0
s.settimeout(10)
try:
    while got < want:
        b = s.recv(1 << 16)
        if not b: break
        got += len(b)
except socket.timeout:
    pass
print("OK" if got == want else "SHORT:%d/%d" % (got, want))
PY
)
case "$R8" in
  OK) ok "T17 R8: slow reader -- 15MB pipeline pura pahuncha, doosra client chalu raha" ;;
  OTHER_CLIENT_STALLED*) no "T17 R8: slow reader" "ek slow client ne poora server rok diya -- blocking write ya loop-till-done" ;;
  SHORT*) no "T17 R8: slow reader" "$R8 -- EAGAIN par bacha hua data drop ho gaya, EPOLLOUT register nahi kiya" ;;
  *) no "T17 R8: slow reader" "$R8" ;;
esac

printf -- '\n-- score --\n\n'
TOTAL=$((PASS+FAIL))
printf '  %d / %d tests pass\n' "$PASS" "$TOTAL"
[ "$VET" = yes ] && printf '  go vet: %s\n' "$(green clean)" || printf '  go vet: %s\n' "$(red 'complaints hain')"

if [ "$FAIL" -gt 0 ]; then
  printf '\n  fail hue:\n'
  for f in "${FAILED[@]}"; do printf '    - %s\n' "$f"; done
fi

printf '\n  '
if   [ "$PASS" -ge 17 ]; then green "17/17 -- production-shaped reactor. Ab EXERCISE.md ka SPOILER section kholo."
elif [ "$PASS" -ge 15 ]; then green "15-16 -- strong. R8 ya ek edge case bacha hai."
elif [ "$PASS" -ge 12 ]; then printf '12-14 -- solid reactor, hardening baaki. 45 min mein yeh accha score hai.'
elif [ "$PASS" -ge 8 ];  then printf '8-11 -- reactor chal raha hai, TCP ki reality baaki hai. T9/T10 pehle theek karo.'
else printf 'basics pehle: T2-T8 clear karo, phir T9-T13.'
fi
printf '\n\n  server log: %s\n\n' "$LOG"

exit 0

