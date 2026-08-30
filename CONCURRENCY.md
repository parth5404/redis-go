# Concurrency + Distributed Systems — Ground Up

Ye doc do hisson mein hai:

**Part 1 — Core concurrency (ek machine ke andar).** Race condition, mutex, deadlock, contention. Ye foundation hai — iske bina Part 2 ka kuch matlab nahi.

**Part 2 — Distributed systems (kai machines).** Replication, consistency, sharding, failover, consensus. Yahan wo cheezein hain jo design discussions aur architecture reviews mein actually aati hain.

Anchor Redis hai — kyunki Redis ek aisa system hai jo dono levels pe real hai, aur tera project uska core already implement kar raha hai.

> Go runtime ke internals (GMP scheduler, signals, netpoller) **Appendix** mein daal diye hain. Wo interesting hai par tere raaste mein nahi. Zarurat pade tab padh.

---

# Part 1 — Core Concurrency

## 1.1 Problem kya hai: race condition

Ek line se shuru karte hain:

```go
counter++
```

Ye ek statement lagti hai. CPU ke liye ye **teen** kaam hain:

```
1. LOAD   counter memory se register mein
2. ADD    register mein 1 jodo
3. STORE  register se wapas memory mein
```

Ab do threads ye saath chalayein, `counter = 5` se shuru:

```
Thread A              Thread B            counter
--------              --------            -------
LOAD  (5)                                    5
                      LOAD  (5)              5
ADD   (6)                                    5
                      ADD   (6)              5
STORE (6)                                    6
                      STORE (6)              6   <- do increment, result 6 (7 hona chahiye tha)
```

Ek increment **gayab**. Isko **lost update** kehte hain.

Yahi **race condition** hai: do ya zyada threads same data ko access karein, aur kam se kam ek likh raha ho, aur unka order guaranteed na ho.

> **Yaad rakhne wali line:** race condition ka matlab crash nahi hai. Matlab hai **result order pe depend karta hai**, aur order pe teri koi control nahi. 99% baar sahi chalega. Wo 1% production mein aayega.

### Tere code mein ye kahan hai

[core/eval.go:120-145](/home/parth-lahoti/Desktop/redis-go/core/eval.go), `evalINCR`:

```go
obj := Get(key)                              // lock andar liya, aur chhod diya
val, _ := strconv.ParseInt(obj.Value.(string), 10, 64)
val++
obj.Value = strconv.FormatInt(val, 10)       // ye lock ke BAHAR ho raha hai
```

`Get` apne andar lock leta hai aur return karte waqt chhod deta hai. Uske baad tu `obj` ko lock ke bahar modify kar raha hai. Exactly upar wala lost-update scenario.

<details>
<summary>To ye abhi tak bug kyun nahi bana?</summary>

Kyunki tera server practically ek hi thread pe commands process karta hai — ek reactor loop hai, ek time pe ek command. To do INCR kabhi overlap nahi karte. Race **dormant** hai.

Par `--mcp` flag ke saath [main.go:38](/home/parth-lahoti/Desktop/redis-go/main.go):

```go
go server.RunAsyncTCP()      // TCP commands — ek goroutine
server.StartMCPServer()      // MCP se aane wale commands — doosri goroutine
```

**Do independent goroutines, ek hi store.** Ab race live hai. Same code, ek flag ka farak.

Ye pattern yaad rakh — "single threaded hai to safe hai" ek assumption hai, guarantee nahi. Jis din koi ek `go` keyword add karega, saare dormant races jaag jaayenge.

</details>

---

## 1.2 Mutex: ilaaj

**Mutex** = MUTual EXclusion. Ek taala. Rule: ek time pe **sirf ek** goroutine andar.

```go
var mu sync.Mutex
var counter int

func increment() {
    mu.Lock()           // taala lo. koi aur andar hai to yahin ruk jao
    counter++           // <- critical section
    mu.Unlock()         // taala chhodo
}
```

Beech ka hissa — **critical section**. Guarantee: ek time pe ek hi goroutine yahan hogi.

### Teen rules jo yaad rakhne hain

**Rule 1 — `defer` use karo.**

```go
mu.Lock()
defer mu.Unlock()     // function kaise bhi khatam ho, unlock hoga
```

Bina `defer`, agar beech mein `return` ya panic aa gaya to lock **kabhi unlock nahi hoga**, aur poora program hamesha ke liye ruk jayega. Tera [core/store.go](/home/parth-lahoti/Desktop/redis-go/core/store.go) ye sahi kar raha hai.

**Rule 2 — mutex data ki raksha karta hai, code ki nahi.**

Ye sabse common galatfehmi hai. Mutex ka matlab "ye function safe hai" nahi hai. Matlab hai "**jo bhi is data ko touch karega, wo pehle YE mutex lega**". Ek bhi jagah bhool gaya — protection khatam. Tera `evalINCR` exactly yahi bhool hai: `Get`/`Put` lock lete hain, par `obj.Value = ...` nahi leta.

**Rule 3 — critical section chhota rakho.**

```go
// GALAT — lock pakad ke slow kaam
mu.Lock()
data := readFromDisk()        // 10ms — poora server ruka hua
cache[key] = data
mu.Unlock()

// SAHI — slow kaam lock ke bahar
data := readFromDisk()        // koi lock nahi
mu.Lock()
cache[key] = data             // microseconds
mu.Unlock()
```

Lock ke andar disk, network, ya koi lambi loop — kabhi nahi.

---

## 1.3 RWMutex: reads ke liye

Insight: **do goroutines ek saath read kar sakti hain, koi problem nahi.** Problem sirf tab hai jab koi likh raha ho.

```go
var mu sync.RWMutex

mu.RLock()          // read lock — kai goroutines saath le sakti hain
value := store[k]
mu.RUnlock()

mu.Lock()           // write lock — EXCLUSIVE, akela
store[k] = value
mu.Unlock()
```

| | Kitne saath? | Kab block? |
|---|---|---|
| `RLock` (read) | Unlimited | Jab koi writer ho |
| `Lock` (write) | Ek | Jab koi bhi reader ya writer ho |

Read-heavy workload mein ye bada farak hai. Cache ka typical ratio 90% reads hota hai.

### Tere code ka bug

[core/store.go:41-50](/home/parth-lahoti/Desktop/redis-go/core/store.go):

```go
func Get(k string) *Obj {
    RWmutex.Lock()              // <- ye WRITE lock hai, ek read function mein
    defer RWmutex.Unlock()
    v := store[k]
    if v != nil && v.ExpiresAt != -1 && time.Now().UnixMilli() >= v.ExpiresAt {
        delete(store, k)        // <- aur yahi reason hai
        return nil
    }
    return v
}
```

Tune `RWMutex` declare kiya par `Get` mein `Lock` use kiya. To read concurrency **zero** hai.

Aur ye poori tarah teri galti nahi — dekh line 46, `delete(store, k)`. Ye **lazy expiry** hai: read ke waqt agar key expired mili to wahin delete kar do. Matlab tera "read" function kabhi-kabhi **write** karta hai. Aur write ke liye write lock chahiye.

<details>
<summary>To fix kaise kare? (socho pehle)</summary>

Standard pattern — **fast path / slow path**:

```
RLock lo
   key expired nahi hai?  → value return karo, RUnlock. (99% cases, fully concurrent)
   key expired hai?       → RUnlock karo, phir Lock lo, dobara check karo, delete karo
```

Us "dobara check karo" pe dhyan de. `RUnlock` aur `Lock` ke beech mein koi doosri goroutine already delete kar chuki ho sakti hai. Ise **lock upgrade race** kehte hain, aur `RLock` → `Lock` ka koi atomic upgrade Go mein nahi hai (deliberately — wo apne aap mein deadlock ka source hai).

Isliye rule: **lock chhod ke dobara lene ke baad, apna assumption phir se verify karo.** Tera pehla check basi ho chuka hai.

</details>

> **Honest note:** ye fix karne se abhi perf **nahi** badhega, kyunki tera reactor single-threaded hai — ek time pe ek hi command hai, to read concurrency ka koi customer hi nahi. Payoff Phase 4 mein aayega jab multiple threads honge. Ye khud ek lesson hai: **optimization ka fayda tab hota hai jab uska consumer maujood ho.**

---

## 1.4 Deadlock

Deadlock = do ya zyada goroutines ek doosre ka intezaar kar rahi hain, hamesha ke liye. Koi crash nahi, koi error nahi. Sab **chup-chaap ruk jaata hai** — jo debug karne mein crash se zyada bura hai.

### Classic form: lock ordering

```go
// Goroutine A            // Goroutine B
mu1.Lock()                mu2.Lock()
mu2.Lock()   <- ruka      mu1.Lock()   <- ruka
```

A ke paas mu1 hai, mu2 chahiye. B ke paas mu2 hai, mu1 chahiye. Dono hamesha ke liye khade.

**Ilaaj: consistent lock ordering.** Poore codebase mein locks hamesha ek hi order mein lo (jaise naam ke alphabetical order mein). Ye rule follow karo to ye deadlock ho hi nahi sakta.

### Tera deadlock: reentrancy

Tera deadlock isse zyada dhoka-dene wala hai — **ek hi goroutine, ek hi mutex.**

[core/store.go:28-39](/home/parth-lahoti/Desktop/redis-go/core/store.go):

```go
func Put(k string, obj *Obj) {
    RWmutex.Lock()                        // (1) lock liya
    defer RWmutex.Unlock()
    if len(store) >= config.KeyLimit {
        evict()                           // (2) evict bulaya
    }
    ...
}
```

[core/eviction.go:12-21](/home/parth-lahoti/Desktop/redis-go/core/eviction.go):

```go
func evictAllkeysRandom() {
    for k := range store {
        Del(k)                            // (3) Del bulaya
        ...
    }
}
```

[core/store.go:52](/home/parth-lahoti/Desktop/redis-go/core/store.go):

```go
func Del(k string) bool {
    RWmutex.Lock()                        // (4) WAHI lock DOBARA -> 💀
```

Ek hi goroutine, wahi lock do baar. Aur:

> **Go ka `Mutex`/`RWMutex` reentrant NAHI hai.**
>
> Matlab: lock ko *tu* pakde ho ya *koi doosra* — dobara `Lock()` maara to ruk jayega. Mutex ko farak nahi padta ki maangne wala kaun hai. Aur kyunki tu khud hi pakde baitha hai, tu khud ko hi unlock nahi kar sakta. Permanent.

Java ka `synchronized` reentrant hota hai, isliye Java se aane walon ko ye pakadta hai. Go ne deliberately nahi rakha — reentrancy lock ke scope ko dhundhla kar deti hai.

### Ye reproduce ho chuka hai

Maine 300 keys bheji (`KeyLimit` = 200). Exactly **key 200** pe server jawab dena band kar diya. pprof ka goroutine dump:

```
sync.(*RWMutex).Lock
core.Del()                   store.go:53
core.evictAllkeysRandom()    eviction.go:15
core.evict()
```

Stack trace mein poora raasta dikh raha hai. Aur expiry goroutine bhi usi lock pe phansi mili — poora server frozen, background cron bhi mar gaya.

### Fix pattern: locked shell, unlocked core

Ye industry-standard pattern hai, yaad rakh:

```go
// PUBLIC — lock leta hai. Bahar se log ye bulaate hain.
func Del(k string) bool {
    RWmutex.Lock()
    defer RWmutex.Unlock()
    return delLocked(k)
}

// PRIVATE — lock NAHI leta. Caller ko lock pakde hona chahiye.
// Naming convention se hi pata chal jaata hai.
func delLocked(k string) bool {
    ...
}
```

Ab `evictAllkeysRandom` (jise `Put` ne lock pakad ke bulaya hai) `delLocked` bulayega, `Del` nahi.

Aur us private function ke upar comment likhna — "caller must hold RWmutex" — optional nahi hai. Wo compiler nahi check karta, sirf comment hi batayega.

### Task 1.1 — tera kaam

**Files:** [core/store.go](/home/parth-lahoti/Desktop/redis-go/core/store.go), [core/eviction.go](/home/parth-lahoti/Desktop/redis-go/core/eviction.go)

Upar wala pattern lagao. Do cheezein dekhna:
- `evictAllkeysRandom` **aur** `evictFirst` dono ko dekh — `evictFirst` seedha `delete(store, k)` karta hai (lock nahi leta), `evictAllkeysRandom` `Del` bulata hai (leta hai). Inconsistency khud ek clue hai.
- `expireSample` ([core/expire.go:8](/home/parth-lahoti/Desktop/redis-go/core/expire.go)) bhi lock leta hai — check kar wo kisi locked path se call hota hai ya nahi.

**Success criteria:** 300 keys bhejo, server 201ve pe zinda rahe aur jawab de.

---

## 1.5 Lock contention: jab lock hi bottleneck ban jaye

Lock correctness de deta hai. Par **scale nahi deta**.

Tere paas ek global lock hai poore store ke liye ([core/store.go:10](/home/parth-lahoti/Desktop/redis-go/core/store.go)):

```go
var store map[string]*Obj
var RWmutex sync.RWMutex        // ek lock, saari keys ke liye
```

8 cores hain. 8 threads ek saath likhna chahein:

```
Thread 1: lock liya, likha, chhoda      ✅
Thread 2..8: line mein khade            ⏳
```

Ek lock = ek time pe ek writer = **effectively 1 core**. Baaki 7 intezaar mein. Isko **lock contention** kehte hain.

### Ilaaj: sharding (partitioning)

Ek bade map + ek lock ki jagah, **N chhote maps, har ek ka apna lock**:

```
key "user:42"  → hash("user:42") % 16 = shard 7  → shard[7] ka lock lo
key "user:99"  → hash("user:99") % 16 = shard 2  → shard[2] ka lock lo
                                                    ye dono SAATH chal sakte hain ✅
```

```go
type shard struct {
    mu    sync.RWMutex
    items map[string]*Obj
}
var shards [16]shard

func shardFor(key string) *shard {
    return &shards[fnv32(key) % 16]
}
```

Ab do keys agar alag shards mein hain to zero contention. 16 shards ≈ 16x write parallelism (theoretically).

> **Ye concept yaad rakhna — ye Part 2 ka bridge hai.** Ek machine ke andar map ko shards mein todna, aur kai machines mein data ko partitions mein todna — **exact same idea** hai. Redis Cluster 16384 "hash slots" use karta hai, wahi `hash(key) % N`, bas shards machines pe baithe hain. Part 2 mein ye wapas aayega.

<details>
<summary>Sharding ka trade-off kya hai?</summary>

**Multi-key operations toot jaate hain.** `MGET a b c` — teen keys, teen shards. Ab teen locks lene padenge, aur multiple locks = lock ordering problem = deadlock ka risk (1.4).

Aur "poore store" pe koi operation — jaise `KEYS *`, ya total key count — ab saare shards ghoomne padenge, aur us dauraan koi consistent snapshot nahi milta.

Yahi trade-off Redis Cluster mein bhi hai: cross-slot multi-key commands **allowed nahi** hain. Tujhe `{}` hash tags use karke keys ko zabardasti ek slot mein daalna padta hai.

**Sharding parallelism deta hai aur atomicity chheen leta hai.** Distributed systems mein ye same baat wapas milegi.

</details>

---

## 1.6 Atomics: mutex ka halka version

Sirf ek number badhana hai? Mutex zaroorat se zyada hai.

```go
import "sync/atomic"

var counter int64
atomic.AddInt64(&counter, 1)        // ek CPU instruction, koi lock nahi
```

CPU khud "load-modify-store" ko ek atomic operation ke roop mein support karta hai (`LOCK XADD`). Koi lock, koi waiting nahi.

| Kab kya use kare | |
|---|---|
| Ek number/flag/pointer | **Atomic** |
| Kai fields ek saath consistent rakhne hain | **Mutex** |
| Map ya slice | **Mutex** (atomics kaam nahi karenge) |

Tere code mein candidate: `con_clients` ([server/sync_tcp.go:13](/home/parth-lahoti/Desktop/redis-go/server/sync_tcp.go)) — ek plain `int` hai jo `con_clients++` se badhta hai, koi protection nahi. Abhi single-threaded hai to bach raha hai, par ye textbook atomic case hai.

---

## 1.7 Channels: share karne ke bajaye bhejo

Ab tak ka approach: **memory share karo, lock se protect karo.** Go ek doosra approach deta hai: **memory share hi na karo, message bhejo.**

```go
ch := make(chan string, 100)         // buffered channel, 100 ki capacity

go func() {                           // ek hi writer goroutine
    for cmd := range ch {             // channel band hone tak padho
        writeToDisk(cmd)
    }
}()

ch <- "SET foo bar"                   // koi lock nahi. bas bhej do.
```

Trick ye hai: disk file ko **sirf ek goroutine** touch karti hai. Isliye kisi lock ki zaroorat hi nahi — sharing hi nahi ho rahi.

| | Buffered `make(chan T, N)` | Unbuffered `make(chan T)` |
|---|---|---|
| Sender rukta hai? | Sirf jab buffer bhara ho | Hamesha, jab tak receiver na le le |
| Use | Producer/consumer, decoupling | Handoff, synchronization |

Buffer bhar jaane pe sender ka rukna feature hai, bug nahi — usko **backpressure** kehte hain. Producer ko force karta hai ki slow consumer ka intezaar kare, bajaye ki unbounded memory kha jaye.

### Ye tere Phase 3 mein banega

Abhi tera AOF ([core/aof.go](/home/parth-lahoti/Desktop/redis-go/core/aof.go)) sirf `BGREWRITE` pe poora snapshot likhta hai. Real AOF banana hai:

```
commands  →  buffered channel  →  ek writer goroutine  →  disk
```

Plus `select` se timer laga ke har second fsync, aur `context` se clean shutdown. Wo Phase 3 mein detail se.

---

## Part 1 ka summary

| Concept | Ek line | Tera code |
|---|---|---|
| Race condition | Result order pe depend karta hai, order pe control nahi | `evalINCR` |
| Mutex | Ek time pe ek. Data ko protect karta hai, code ko nahi | `store.go` |
| RWMutex | Kai readers ya ek writer | `Get` galat lock leta hai |
| Deadlock | Chup-chaap phans jaana. Go mein reentrancy nahi | `Put` → `evict` → `Del` |
| Contention | Lock correct hai par scale nahi karta | ek global `RWmutex` |
| Sharding | Lock todo, parallelism badhao, atomicity kho do | Phase 2 |
| Atomics | Ek number ke liye mutex se sasta | `con_clients` |
| Channels | Share karne ke bajaye bhejo | Phase 3 AOF |

---

# Part 2 — Distributed Systems

Part 1 ek machine ke andar tha. Ab kai machines. **Ek nayi cheez badalti hai, aur wahi sab kuch badal deti hai: network fail ho sakta hai.**

Ek machine ke andar function call kabhi "gum" nahi hoti. Network pe request **gum ho sakti hai, late aa sakti hai, do baar aa sakti hai, ya aa sakti hai par reply gum ho jaye.** Distributed systems ki saari complexity is ek fact se nikalti hai.

## 2.1 Distribute kyun karte hain

| Reason | Matlab |
|---|---|
| **Capacity** | Data ek machine ki RAM mein nahi aata |
| **Throughput** | Ek machine ke CPU/network se zyada load hai |
| **Availability** | Ek machine mar gayi to service band nahi honi chahiye |
| **Latency** | Users duniya bhar mein hain, data unke paas hona chahiye |

Ye char alag problems hain aur **alag solutions** maangti hain. Yahi Part 2 ka structure hai:

- Capacity/throughput → **partitioning** (2.4)
- Availability → **replication** (2.2) + **failover** (2.5)

Pehla scaling ka sawaal hamesha ye hota hai:

| | Vertical (scale up) | Horizontal (scale out) |
|---|---|---|
| Kya | Badi machine lo | Zyada machines lo |
| Complexity | Zero | Bahut |
| Limit | Sabse badi machine | Practically koi nahi |
| Availability | Machine gayi, sab gaya | Ek gayi, baaki chalti hain |

> **Practical advice:** vertical scaling ko under-rate kiya jaata hai. Aaj ek machine 1 TB RAM aur 128 cores le sakti hai. Distributed banane se **pehle** ye poochna chahiye — kya ek badi machine kaafi nahi hai? Kai companies ne distributed complexity le li jiski zaroorat nahi thi.

## 2.2 Replication

Same data kai machines pe rakho.

```
        ┌─────────────┐
        │   LEADER    │  <- saare writes yahan
        │  (primary)  │
        └──────┬──────┘
               │ changes bhejta hai
        ┌──────┴──────┬─────────────┐
        ▼             ▼             ▼
   ┌─────────┐  ┌─────────┐  ┌─────────┐
   │FOLLOWER │  │FOLLOWER │  │FOLLOWER │   <- reads yahan se ho sakte hain
   └─────────┘  └─────────┘  └─────────┘
```

Ye **leader-follower** (ya primary-replica) model hai. Redis, PostgreSQL, MySQL, MongoDB — sab yahi karte hain.

Isse milta kya hai: read throughput (reads followers pe baant do), aur availability (leader mara to follower promote karo).

### Asli sawaal: leader kab "OK" bole?

Yahi poore distributed systems ka **central trade-off** hai.

**Async replication** — leader turant OK bol deta hai, followers ko baad mein bhejta hai.

```
Client → Leader: SET x=5
Leader → Client: OK          ✅ fast
Leader → Followers: x=5      (baad mein, background mein)
```

Fast hai. Par: **leader OK bolne ke turant baad mar gaya, to wo write hamesha ke liye gaya.** Client ko laga save ho gaya. Nahi hua.

**Sync replication** — leader followers ka confirmation aane tak wait karta hai.

```
Client → Leader: SET x=5
Leader → Followers: x=5
Followers → Leader: got it
Leader → Client: OK          🐢 slow, par data safe
```

Safe hai. Par slow, aur — **ek follower slow ho gaya to saare writes ruk jaate hain.** Ek machine ka problem poore system ka problem ban gaya.

| | Async | Sync |
|---|---|---|
| Latency | Kam | Zyada (sabse slow follower ke barabar) |
| Data loss | Failover pe possible | Nahi |
| Ek follower slow ho | Farak nahi | Saare writes ruk jaate hain |

**Redis by default async karta hai.** Isliye Redis fast hai, aur isliye Redis failover pe writes kho sakta hai. Ye Redis ka bug nahi — ye ek **conscious design choice** hai (cache ke liye theek hai, bank ledger ke liye nahi).

<details>
<summary>Redis ka WAIT command</summary>

Redis `WAIT numreplicas timeout` deta hai — "jab tak N replicas ne mere writes acknowledge na kar diye, wait karo".

Ye async ke upar **per-operation opt-in sync** hai. Normal writes fast, critical writes ke baad `WAIT 2 1000` maar do.

Design lesson: consistency ko binary choice banane ki zaroorat nahi. Per-operation knob de sakte ho.

</details>

## 2.3 Consistency models

Replication ka natural sawaal: **follower se padhne pe purana data mil sakta hai?**

```
t=0   Leader: x=5
t=1   Client A: SET x=10  →  Leader ne OK bola
t=2   Client B: GET x     →  Follower se padha  →  5 mila 😬
```

Ye "bug" nahi hai. Ye **eventual consistency** hai.

| Model | Guarantee | Cost |
|---|---|---|
| **Strong (linearizable)** | Har read latest likha hua data dega. System ek hi machine jaisa behave karta hai | Coordination chahiye, slow |
| **Eventual** | Likhna band karo to sab replicas *kabhi* match kar lenge. Beech mein kuch bhi mil sakta hai | Sabse fast |
| **Read-your-writes** | Tu apne likhe ko turant padh sakta hai. Doosron ka purana mil sakta hai | Beech ka |
| **Monotonic reads** | Naya padh liya to purana wapas nahi dikhega (time peeche nahi jayegi) | Beech ka |

Beech wale models kam famous hain par practically sabse zyada useful hain. "Mera comment post karne ke baad mujhe dikhna chahiye, doosron ko 2 second baad dikhe to chalega" — ye read-your-writes hai, aur ye strong consistency se kaafi sasta hai.

> **Interview/design discussion ke liye:** "strong consistency chahiye" default answer nahi hona chahiye. Sahi sawaal hai — *stale data se kya toot jayega?* Cache pe 2 second purana view chalta hai. Account balance pe nahi.

## 2.4 Partitioning (sharding) — Part 1 ka echo

Part 1 (1.5) mein ek map ko 16 shards mein toda tha, contention kam karne ke liye. Bilkul wahi idea, machines pe:

```
                    hash(key) % 3
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
   ┌─────────┐       ┌─────────┐       ┌─────────┐
   │ Node A  │       │ Node B  │       │ Node C  │
   │ keys 0-3│       │keys 4-7 │       │keys 8-11│
   └─────────┘       └─────────┘       └─────────┘
```

**Redis Cluster** ye exactly karta hai, **16384 hash slots** ke saath:

```
slot = CRC16(key) % 16384
```

Har node kuch slots ka owner hai. Client `hash(key)` calculate karke seedha sahi node se baat karta hai.

<details>
<summary>16384 kyun? Aur 3 nodes ke liye seedha % 3 kyun nahi?</summary>

Kyunki **`% 3` nodes ki count pe depend karta hai**, aur nodes badalti hain. 4th node add kiya → `% 4` → **almost saari keys ka slot badal gaya** → poora dataset move karna padega.

Slots is problem ko solve karte hain, ek **indirection layer** daal ke:

```
key  →  slot  (ye hamesha fixed hai, 16384 pe)
slot →  node  (ye badal sakta hai, sasta hai)
```

Node add karne pe sirf *slot→node* mapping badalti hai. Kuch slots naye node pe move karo — 1/4 data, na ki saara.

Ye idea generally **consistent hashing** ke naam se jaana jaata hai (Redis ka slot-based version usi family ka ek simple, explicit variant hai). Har us system mein milega jo rebalance karta hai — Cassandra, DynamoDB, Kafka partitions.

**Ye generalize karne wala lesson hai: jab direct mapping badalti rehti ho, ek stable indirection layer daal do.**

</details>

Aur 1.5 wala trade-off yahan bhi wapas aata hai: Redis Cluster mein **cross-slot multi-key commands allowed nahi** hain. `MGET user:1 user:2` fail ho jayega agar wo alag slots mein hain. Solve karne ke liye hash tags: `user:{42}:name` aur `user:{42}:email` — `{}` ke andar wala hissa hash hota hai, to dono guaranteed same slot mein.

## 2.5 Failure detection aur failover

Leader mar gaya. Ab?

Pehla problem — **pata kaise chalega ki wo mara hai?**

> **Distributed systems ka sabse important sawaal:** "wo mar gaya" aur "wo slow hai / network beech mein toot gaya" — inme **farak karna namumkin hai.**

Ye theoretical nitpick nahi hai, ye practical dukh ka main source hai. Tu sirf itna keh sakta hai "maine 5 second se jawab nahi suna". Ye ho sakta hai:
- Wo actually mar gaya
- Wo GC pause mein hai
- Network partition hai, aur **wo abhi bhi apne aap ko leader samajh raha hai**

Ye last case sabse khatarnak hai — **split brain**. Do nodes dono apne aap ko leader samajh rahe hain, dono writes le rahe hain. Data diverge ho gaya.

### Quorum: ilaaj

Ek node akela decide na kare. **Majority se poocho.**

```
5 nodes. Leader ke maut ka faisla karne ke liye 3 (majority) ka agreement chahiye.

Network partition:
   [A, B, C]  |  [D, E]

   A-B-C side: 3 nodes hain, majority hai → naya leader chun sakte hain ✅
   D-E side:   2 nodes hain, majority nahi → khud ko step down karna padega ❌
```

Kyunki `N/2 + 1` ke **do** groups ho hi nahi sakte, sirf ek side aage badh sakti hai. Split brain mathematically impossible.

Isliye cluster sizes **odd** hote hain (3, 5, 7) — 4 nodes 3 nodes se better availability nahi dete (dono 1 failure tolerate karte hain), bas ek machine ka paisa zyada.

**Redis Sentinel** yahi karta hai: sentinel processes leader ko monitor karte hain, aur jab quorum agree kare tabhi failover trigger karte hain.

## 2.6 Consensus: Raft ka mental model

**Consensus** = kai machines ek value pe agree karein, kuch machines fail hone ke baad bhi. Ye distributed systems ka core problem hai, aur **Raft** aaj ka standard answer hai (etcd, Consul, CockroachDB, TiKV — sab Raft).

Teen ideas kaafi hain samajhne ke liye:

**1. Ek time pe ek leader.** Saare writes leader se. Isse ordering ka problem gayab ho jaata hai — ek hi jagah decide ho raha hai.

**2. Replicated log.** Leader operations ko ek ordered log mein likhta hai aur followers ko bhejta hai. Sab same log, same order mein → sab same state.

```
Leader log:   [1: SET x=5] [2: SET y=3] [3: DEL x]
Follower log: [1: SET x=5] [2: SET y=3] [3: DEL x]     <- same order = same state
```

**3. Majority commit.** Ek entry "committed" tab hoti hai jab **majority** ne use likh liya. Isliye leader marne ke baad bhi, kisi bhi naye majority mein wo entry rakhne wala kam se kam ek node hoga. Data safe.

Election: leader se heartbeat na aaye → koi node candidate ban jaata hai → vote maangta hai → majority mila to leader ban gaya.

> **Redis Raft use nahi karta.** Redis async replication + Sentinel/Cluster failover karta hai — **speed ke liye consistency choda hai.** Isliye Redis ka failover writes kho sakta hai, aur etcd ka nahi. Aur isliye Redis 100k+ ops/sec karta hai jabki Raft-based systems kaafi kam.
>
> Ye Redis ka defect nahi hai. Ye "cache/queue chahiye ya source of truth chahiye" ka answer hai. **Ye distinction samajhna Raft ka algorithm yaad karne se zyada valuable hai.**

## 2.7 CAP theorem — aur log ise kaise galat samajhte hain

CAP kehta hai: **network partition ke waqt**, in teeno mein se sirf do mil sakte hain:

- **C**onsistency — sab same data dekhte hain
- **A**vailability — har request ka jawab milta hai
- **P**artition tolerance — network toot sakta hai

Common galatfehmi: "hum AP system hain" ya "hum CA hain, partition nahi hota".

**Sach ye hai: P choice nahi hai.** Network *toot ta hai*. Ye fact hai. To asli sawaal sirf ek hai:

> **Partition ho gaya. Ab minority side kya kare — galat jawab de, ya koi jawab na de?**

```
Partition:  [A, B, C]  |  [D, E]

D-E ke paas purana data hai. Client D se GET maar raha hai.

CP choice:  "mujhe nahi pata, main minority mein hoon" → error   (consistent, unavailable)
AP choice:  purana data de do                                    (available, inconsistent)
```

Bas itna hai. Aur ye **poore system ka** faisla hona zaroori nahi:

| Operation | Sahi choice |
|---|---|
| Product page dikhana | **AP** — 30 second purana price chalega, page down hona nahi chalega |
| Payment process karna | **CP** — galat balance se paisa katna, error se bura hai |

**Ek hi system mein dono ho sakte hain, per-operation.** Ye insight CAP ko trivia se actual design tool bana deta hai.

## Part 2 ka summary

| Concept | Ek line |
|---|---|
| Sabse pehla sawaal | Ek badi machine kaafi nahi hai? |
| Replication | Data copy karo — availability aur read scale ke liye |
| Async vs sync | Speed vs durability. Redis async chunta hai, jaan-boojh ke |
| Consistency models | Strong default nahi hai. Poochho: stale data se kya tootega? |
| Partitioning | Part 1 ki sharding, machines pe. Indirection layer (slots) rakho |
| "Mar gaya" detect karna | **Namumkin** hai slow se farak karna. Isliye quorum |
| Quorum | Majority. Split brain ko mathematically rokta hai. Odd counts |
| Raft | Ek leader + replicated log + majority commit |
| CAP | P optional nahi hai. Sawaal: minority galat jawab de ya koi na de? |

---

# Appendix — Go runtime internals

Ye tere raaste mein nahi hai. Zarurat tab padegi jab raw syscalls ya perf tuning karega. **Ek practical rule** jo abhi kaam ka hai:

> **Raw blocking syscall (`EpollWait`, `Read`, `Accept`) ka `EINTR` error nahi hai — wo "dobara try kar" hai.** Go ka runtime goroutines ko rokne ke liye threads ko signals bhejta rehta hai, aur signal blocking syscall ko interrupt kar deti hai. `EINTR` pe retry karna teri zimmedari hai.
>
> Go ke `net` package mein ye problem nahi hai kyunki wo har syscall ko andar se retry loop mein wrap karta hai. Raw `syscall` use karke tu wo safety net bypass kar chuka hai.

Tera pending **Task 1.0** yahi hai — [server/async_tcp.go:59-64](/home/parth-lahoti/Desktop/redis-go/server/async_tcp.go) mein `EpollWait` ka EINTR handle karna. (`Accept4` pe nahi — `EpollWait` mein `-1` timeout hai, server apni poori idle life wahan baitha hai, signal wahin land karega.)

Reproduce ho chuka hai: default mein 3/3 runs mara (14, 38, 40 commands pe). `GODEBUG=asyncpreemptoff=1` ke saath 2/2 runs, 150 commands, zinda.

<details>
<summary>Poori kahani — G/M/P scheduler, preemption, netpoller</summary>

**G / M / P.** `G` = goroutine (~2 KB stack, kernel ko invisible). `M` = ek asli OS thread. `P` = "chalane ka permit" + local run queue, count = `GOMAXPROCS` = 8. Go code chalane ke liye G ko M pe hona chahiye aur M ke paas P hona chahiye. Isliye 10 lakh G, par ek time pe sirf 8 chal rahi hain. Ye trick hai jisse goroutines sasti hain — concurrency kernel se user space mein aa gayi.

**Blocking syscall.** M kernel mein phans jaata hai. `sysmon` (ek special monitoring thread) notice karta hai aur P ko chheen ke doosre M ko de deta hai, taaki 8 permits mein se ek barbaad na ho.

**Preemption.** Go 1.14 se async: `sysmon` 10ms se zyada chalne wali goroutine ke thread ko **SIGURG** bhejta hai, signal handler goroutine ko yield kara deta hai. Isse pehle preemption cooperative thi aur `for {}` poora core hijack kar leta tha (aur GC ko block kar deta tha).

**Aur wahi tera crash hai:** tera reactor `epoll_wait(-1)` mein permanently blocked hai. Runtime SIGURG bhejta hai → POSIX ke rule se syscall `EINTR` return karti hai → tera code `return err` kar deta hai → server dead.

**Netpoller.** Go ka `net` package sockets ko non-blocking banata hai aur runtime ke andar apna epoll chalata hai. Isliye 10,000 goroutines `conn.Read()` pe "block" kar sakti hain bina 10,000 threads banaye. **Go ne tera event loop already likha hua hai, runtime ke andar** — tu use bypass karke apna likh raha hai. Wo galat nahi hai (Redis-style control milta hai), par muft nahi hai.

</details>

---

## Debug reference

```bash
# saari goroutines kahan phansi hain — deadlock ke liye best tool
curl -s 'http://localhost:6060/debug/pprof/goroutine?debug=2'

# race detector
go build -race -o srv . && ./srv

# CPU profile
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=10

# scheduler state
GODEBUG=schedtrace=1000 ./srv
```

> **Build note:** root package abhi build nahi hota — `redis.go` naam ka 9.3 MB ELF binary source samjha jaa raha hai. Library ke liye `go build ./core/... ./server/... ./config/...`, aur poore binary ke liye us file ke bina copy banake build karo.
