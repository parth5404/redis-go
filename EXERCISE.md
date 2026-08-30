# Exercise — Reactor From Scratch (45–50 min, timed)

> Yeh ek **closed-book timed exercise** hai. Goal: `REACTOR.md` Part 1 ka poora
> concept apne haath se code karke prove karna.
>
> Timer 50 min ka lagao. Rukna nahi hai — jo nahi hua woh nahi hua, end mein
> self-grade karna hai.

## Rules

**Band karo (dekhna cheating hai):**
- `server/async_tcp.go`
- `server/client.go`
- `core/resp.go`
- `REACTOR.md` ke code blocks

**Khula rakh sakte ho:**
- `man 2 epoll_create1`, `man 2 epoll_ctl`, `man 2 epoll_wait`, `man 2 accept4`
- `go doc syscall` (`go doc syscall.EpollEvent`, `go doc syscall.Accept4`, …)
- RESP spec: https://redis.io/docs/latest/develop/reference/protocol-spec/
- Apna khaali paper (design sketch ke liye)

**Constraints:**
- Ek file: `exercise/minired/main.go`, `package main`
- Zero external dependencies — sirf Go stdlib (`syscall`, `errors`, `fmt`, `log`, `os`, `strconv`, `strings`)
- `net` package **listener ke liye use nahi karna** (`net.Listen` banned). Raw
  `syscall.Socket` se banao. `net.ParseIP` allowed hai.
- Port: **7380** (7379 par asli project chalta hai, clash na ho)

```bash
mkdir -p exercise/minired && cd exercise/minired && go mod init minired
```

---

## Problem Statement

> **Build `minired`: ek single-goroutine, epoll-based TCP server jo `redis-cli` se
> baat kar sakta hai.**
>
> Ek OS thread, ek event loop, N concurrent clients. Kernel ko poll mat karo —
> kernel se batwao. Har connection ka apna inbound aur outbound buffer ho. Ek
> event = ek `write()` syscall.
>
> Server ko in chaar cheezon se **survive** karna hai, kyunki asli TCP ismein se
> koi guarantee nahi deta:
>
> 1. Ek command **do TCP segment** mein aa sakta hai (`*1\r\n$4\r\nPIN` … `G\r\n`)
> 2. **Teen command ek** segment mein aa sakte hain (pipelining)
> 3. Client beech mein **gayab** ho sakta hai (`Ctrl-C`, `RST`)
> 4. Client reply **padhna band** kar sakta hai — kernel ka send buffer bhar jaata
>    hai aur aapka `write()` `EAGAIN` de deta hai

### Commands jo support karne hain

| Command | Request (RESP array) | Reply |
|---|---|---|
| `PING` | `*1\r\n$4\r\nPING\r\n` | `+PONG\r\n` |
| `ECHO x` | `*2\r\n$4\r\nECHO\r\n$1\r\nx\r\n` | `$1\r\nx\r\n` |
| `SET k v` | `*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n` | `+OK\r\n` |
| `GET k` | `*2\r\n$3\r\nGET\r\n$1\r\nk\r\n` | `$1\r\nv\r\n` ya `$-1\r\n` agar nahi hai |
| `DEL k [k…]` | `*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n` | `:1\r\n` (kitne delete hue) |
| kuch aur | — | `-ERR unknown command 'foo'\r\n` |

Store: plain `map[string]string`. **Koi mutex nahi** — aur yeh galti nahi hai, yeh
poora point hai: ek hi goroutine usko touch karti hai. Agar aapko mutex lagane ka
mann kar raha hai, matlab kahin `go` statement likh diya hai.

Command name **case-insensitive** hona chahiye (`redis-cli` `PING` bhejta hai, par
`ping` bhi chalna chahiye).

---

## Hard Requirements (yeh graded hain)

Har requirement ka ek number hai. End mein rubric mein wapas milenge.

| # | Requirement | Kyu (ek line) |
|---|---|---|
| **R1** | Poore program mein **zero `go` statements**. Ek goroutine, sab kaam. | Yeh reactor hai, thread pool nahi. `grep -c '\bgo ' main.go` → 0 |
| **R2** | Listening socket **aur** accepted socket dono non-blocking | Blocking client socket = ek slow client poora server wedge kar dega |
| **R3** | `EpollWait` aur `Read`/`Write` par `EINTR` → **retry**, fatal nahi | Go runtime har ~10 ms SIGURG bhejta hai; ignore kiya to server seconds mein marega |
| **R4** | Per-connection **inbound accumulator** (`in []byte`) | Aadha command aane par usko sambhaal ke rakhna hai next event tak |
| **R5** | Parse ek **loop** mein — ek read se jitne complete command nikle, sab execute | Pipelining. Ek command parse karke ruk gaye to baaki chup-chaap kho jaayenge |
| **R6** | Per-connection **outbound buffer** (`out []byte`), ek event = **ek `write()`** | Reply coalescing. Yahi 100×+ pipelined throughput deta hai |
| **R7** | Accepted socket par `TCP_NODELAY` | Nagle × delayed-ACK = fixed ~40 ms stall per request |
| **R8** | `write()` ka `EAGAIN` / short write handle: bacha hua data rakho, `EPOLLOUT` register karo, likh jaane par wapas `EPOLLIN` par aao | Warna reply ka aadha hissa drop → protocol stream permanently desync |

R8 sabse tough hai aur akhir mein aata hai. R1–R7 pehle karo.

---

## Timetable — 50 min

Ghadi dekhte raho. Agar ek milestone slip ho raha hai, aage badho, wapas aana.

| Time | Milestone | Deliverable |
|---|---|---|
| **0–5** | **M0: Paper design.** Code likhna shuru mat karo. Paper par likho: (a) `reactor` struct ke fields, (b) `conn` struct ke fields, (c) event loop ka switch — kitne case?, (d) `handleReadable()` ke andar ke 5 steps ka order | Ek paper sketch |
| **5–15** | **M1: Socket up.** `socket` → `SO_REUSEADDR` → `bind` → `listen` → `epoll_create1` → `epoll_ctl(ADD, listenFd)` → `epoll_wait` loop jo accept karke `log.Printf` kare | `redis-cli -p 7380 ping` **hang** hoga (koi reply nahi) — par server log mein "accepted fd 8" dikhega |
| **15–25** | **M2: Echo server.** Per-conn `in`/`out` buffers, `read` → `out` mein append → `write`. EOF (`n == 0`) par close + map se delete | `nc 127.0.0.1 7380` mein type karo, wapas aana chahiye. Do terminal se ek saath karo — dono chalein |
| **25–38** | **M3: RESP + commands.** `parseOne(buf) (cmd []string, consumed int, err error)`, drain loop, `execute(cmd) []byte`, map store | `redis-cli -p 7380 ping` → `PONG`. `set a 1` / `get a` / `del a` / `get a` chale |
| **38–45** | **M4: Hardening.** `TCP_NODELAY`, `EINTR` retry, `MaxRequestSize` cap (64 MB), unknown command error | `./exercise/verify.sh` ke pehle 8 test pass |
| **45–50** | **M5: R8 (stretch).** `EPOLLOUT` state machine | `verify.sh` ka slow-reader test pass |

**M3 ka trickiest hissa** — `parseOne` ko **teen** cheezein distinguish karni hain:

```go
// return contract — isko M0 mein paper par likh lo, warna M3 mein phasoge:
//   (cmd, n, nil)              -> ek pura command mila, n bytes consume karo
//   (nil, 0, errIncomplete)    -> data adhoora hai, aur bhejo (yeh ERROR NAHI hai)
//   (nil, 0, someOtherErr)     -> frame kharab hai, connection band karo
func parseOne(b []byte) ([]string, int, error)
```

"Incomplete" aur "malformed" ko alag rakhna R4 aur R5 dono ki jaan hai. Agar
incomplete ko error maana → split command par connection drop hoga. Agar malformed
ko incomplete maana → server us connection par hamesha ke liye wait karega.

---

## Starter skeleton — sirf signatures, bodies aapke

Yeh copy kar sakte ho. Ismein **koi logic nahi** hai, sirf woh shape hai jo aapko
M0 mein khud derive karna chahiye tha. Agar aap M0 khud kar sakte ho, isko skip karo.

```go
package main

import (
	"errors"
	"log"
	"strconv"
	"strings"
	"syscall"
)

const (
	port           = 7380
	maxEvents      = 1024
	readBufSize    = 16 * 1024
	maxRequestSize = 64 * 1024 * 1024
)

var errIncomplete = errors.New("incomplete frame")

// conn = ek connection ki poori state. R4 + R6 yahan rehte hain.
type conn struct {
	fd  int
	in  []byte // TODO: R4
	out []byte // TODO: R6
}

type reactor struct {
	listenFd int
	epfd     int
	conns    map[int]*conn
	events   []syscall.EpollEvent
	scratch  []byte // ek buffer, poore reactor ke liye — per-read allocate mat karo
	store    map[string]string
}

// M1
func newReactor() (*reactor, error)          { panic("TODO") }
func (r *reactor) add(fd int, ev uint32) error { panic("TODO") }
func (r *reactor) mod(fd int, ev uint32) error { panic("TODO") }

// M1 + M4 (EINTR)
func (r *reactor) loop() error { panic("TODO") }

// M1 + M4 (TCP_NODELAY, EAGAIN se bahar niklo)
func (r *reactor) acceptAll() { panic("TODO") }

// M2 + M3 + M5. Yeh sabse important function hai — iske andar ke steps ka order
// hi poora design hai.
func (r *reactor) handleEvent(fd int, events uint32) { panic("TODO") }

func (r *reactor) drop(c *conn) { panic("TODO") }

// M2. Returns false agar peer chala gaya.
func (c *conn) readInto(scratch []byte) (alive bool, err error) { panic("TODO") }

// M2 + M5. done == false matlab kernel buffer bhar gaya, baaki bacha hai.
func (c *conn) flush() (done bool, err error) { panic("TODO") }

// M3. Contract upar diya hua hai — teen possible outcomes.
func parseOne(b []byte) (cmd []string, consumed int, err error) { panic("TODO") }

// M3
func (r *reactor) execute(cmd []string) []byte { panic("TODO") }

func main() {
	r, err := newReactor()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("minired listening on :%d", port)
	log.Fatal(r.loop())
}
```

---

## Self-grading

Timer khatam. Ab yeh chalao:

```bash
./exercise/verify.sh
```

Woh aapka code build karega, `:7380` par start karega, **17 test** chalayega aur
score dega. Har fail hone par ek hint bhi deta hai ki kaunsa requirement toota.

Tests kya karte hain:

| Test | Kya check karta hai | Requirement |
|---|---|---|
| T1 | source mein zero `go` statements | R1 |
| T2–T8 | PING / SET / GET / GET-missing / DEL / ECHO / unknown / case-insensitive | — |
| T9 | `*1\r\n$4\r\nPIN` … 400 ms … `G\r\n` → `+PONG` | **R4** |
| T10 | teen `PING` ek `write()` mein → teen `+PONG` | **R5** |
| T11 | ek connection idle khula, doosra phir bhi serve ho | reactor hona hi |
| T12 | client abrupt disconnect → server zinda | — |
| T13 | garbage frame → server zinda | — |
| T14 | idle par CPU ~0 (`/proc/pid/stat` jiffies) | busy-poll nahi |
| T15 | 200 sequential round trips < 2 s (Nagle wall ~8 s hoti) | **R7** |
| T16 | `redis-benchmark -n 20000 -c 50` ke baad zinda | **R3** |
| T17 | 15 MB pipeline, client 1.5 s tak padhta nahi; doosra client chale, aur poore 15 MB aayein | **R8** |

Harness real Redis par verify kiya gaya hai — 16/17 pass karta hai (T1 obviously
fail, kyunki Redis C mein hai). Matlab tests aapke code ki galti pakadte hain,
apni nahi.

### Score kaise padhein

| Pass | Matlab |
|---|---|
| **17/17** | Production-shaped reactor. Neeche SPOILER section kholo. |
| **15–16** | Strong. R8 ya ek edge case bacha. |
| **12–14** | Solid reactor, hardening baaki. **45 min mein yeh accha score hai.** |
| **8–11** | Reactor chal raha hai, TCP ki reality baaki. T9/T10 pehle. |
| **< 8** | Basics: T2–T8 clear karo phir aage. |

### Agar time bacha ho — stretch

1. `EXPIRE k sec` + lazy expiry on `GET` (state machine mein time aa gaya)
2. Graceful shutdown: `SIGINT` handler → **self-pipe trick**. Sochna: `EpollWait`
   `-1` timeout par blocked hai, signal handler use kaise jagayega? Channel se
   nahi jaga sakte — epoll channel ko watch nahi kar sakta. (Yeh production code
   mein `reactor.wakeFd` hai.)
3. Do reactors, `SO_REUSEPORT` ke saath. Phir `redis-benchmark -P 64` chala ke dekho
   ek reactor se better ya worse (jawab depth par depend karta hai — Part 9)

---

# 🔒 SPOILER — timer khatam hone ke baad kholo

> Neeche woh **saat bugs** hain jo is exact exercise mein log likhte hain. Yeh
> guess nahi hai — inme se **chhe is repo ke `main` branch mein actually the**.
> Apne code ke against ek-ek check karo. Do se zyada mile to normal hai; yahi
> exercise ka point hai.

**Bug 1 — `O_NONBLOCK` vs `SOCK_NONBLOCK`.**
```go
syscall.Socket(syscall.AF_INET, syscall.O_NONBLOCK|syscall.SOCK_STREAM, 0)  // ❌
syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)  // ✅
```
Linux par dono constants `0x800` hain, to galat wala **kaam kar jaata hai** — pure
coincidence se. Kisi doosre platform par socket blocking ban jaata aur poora event
loop pehle `accept` par atak jaata.

**Bug 2 — `Accept4(fd, 0)`.** Listener non-blocking hai, par accepted socket
**blocking** hai. Ek slow client `read()` ke andar poora server wedge kar dega. `verify.sh`
ka T17 ismein `OTHER_CLIENT_STALLED` dega. Fix: `Accept4(fd, SOCK_NONBLOCK|SOCK_CLOEXEC)`.

**Bug 3 — `EpollWait` ka error fatal maan liya.**
```go
n, err := syscall.EpollWait(epfd, events, -1)
if err != nil { return err }        // ❌ server seconds mein mar jayega
```
Go runtime goroutines ko preempt karne ke liye thread ko `SIGURG` bhejta hai, roughly
har 10 ms. Blocked syscall + signal = **`EINTR`**. `EINTR` error nahi hai, woh
"dobara call karo" hai. Ek `if errors.Is(err, syscall.EINTR) { continue }` poore
server ke zinda rehne aur mar jaane ka farak hai. (Is repo ka `main` branch isi
wajah se `GODEBUG=asyncpreemptoff=1` ke saath chalta tha — woh workaround nahi tha,
bug ko chhupana tha.)

**Bug 4 — per-event fresh buffer, koi accumulator nahi.**
```go
buf := make([]byte, 512)            // ❌ har event par naya, aur 512 bhi chhota hai
n, _ := syscall.Read(fd, buf)
cmd := parse(buf[:n])               // ❌ maan liya ki ek read = ek pura command
```
Do failure directions: **under-read** (aadha command → garbage parse) aur
**over-read** (10 commands aaye, parse hue, par trailing aadha 11th chup-chaap gaya).
T9 aur T10 exactly yahi pakadte hain. Fix: `c.in = append(c.in, buf[:n]...)` + drain loop.

**Bug 5 — `consume` ke liye re-slicing.**
```go
c.in = c.in[n:]                     // ❌ backing array bina bound grow karega
c.in = append(c.in[:0], c.in[n:]...) // ✅ copy-and-truncate
```
Re-slicing consumed prefix ko zinda rakhta hai. Ek long-lived pipelined connection
par yeh slow memory leak hai — ghanton mein dikhta hai, minutes mein nahi. Isliye
sabse khatarnak wala hai.

**Bug 6 — short write ka tail drop.**
```go
n, _ := syscall.Write(fd, out)      // ❌ n < len(out) ho sakta hai!
// ...aur out ko bhool gaye
```
Non-blocking socket par short write **normal** hai. Bacha hua hissa drop kiya to
client ka RESP stream permanently desync ho jaata hai — woh aapke agle reply ke
bytes ko pichhle reply ka hissa samjhega. T17 `SHORT:x/y` dega.

**Bug 7 — `EPOLLOUT` register kar diya, hataya nahi.**
```go
r.mod(fd, syscall.EPOLLIN|syscall.EPOLLOUT)   // ...aur permanently aisa hi chhod diya
```
Ek drained socket **hamesha** writable hota hai. Level-triggered epoll har `epoll_wait`
par usko ready batayega → 100% CPU busy loop, par sirf tab jab kabhi ek partial write
hua ho. T14 idle par pass hoga, load ke baad fail. Fix: `done == true` par wapas
`EPOLLIN` par aao.

## Ek aur cheez check karo

Aapke `handleEvent` ke andar steps ka **order**. Sahi order:

```
1. EPOLLOUT set hai?  -> pehle flush karo (pichhla udhaar chukao),
                          done ho gaya to EPOLLIN par wapas aao
2. EPOLLIN set hai?   -> read -> parse loop -> execute -> out mein append
3. flush karo         -> agar done nahi hua to EPOLLOUT register karo
4. EPOLLHUP/ERR hai aur kuch pending nahi -> drop
```

Step 1 ko step 2 ke *baad* rakha? To ek slow client ke replies out-of-order jaa
sakte hain: naya reply purane pending bytes ke aage lag jaayega. Step 4 ko sabse
pehle rakha? To woh client jisne `SET x 1` bheja aur turant `shutdown(SHUT_WR)`
kiya, apna `+OK` kabhi nahi paayega — `EPOLLHUP` aur `EPOLLIN` saath aate hain.

---

## Iske baad

Ab `server/async_tcp.go` aur `server/client.go` kholo aur apne code se compare karo.
Aapko woh 500 lines ab **choti** lagengi, kyunki har line ek problem ka jawab hai jo
aapne khud 50 min mein jhela hai.

Phir `REACTOR.md` **Part 2** (`epoll` level vs edge triggered) — woh sawaal ab aapko
theoretical nahi lagega.

