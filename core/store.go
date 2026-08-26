package core

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github/redis.go/config"
)

// Store is a sharded, concurrently-accessible keyspace.
//
// # Why shards
//
// A single map behind a single RWMutex serialises every write in the process:
// one lock means one writer, which means one effective core no matter how many
// reactors are running. Splitting the keyspace into N independently locked
// partitions lets keys that hash to different shards be written in parallel.
// This is the same idea Redis Cluster applies across machines (16384 hash
// slots); here it is applied across mutexes inside one machine.
//
// The trade-off is that no operation can see a consistent snapshot of the
// whole keyspace without locking every shard. Commands that need a global view
// (DBSIZE, FLUSHALL) therefore report a value that is only eventually
// consistent, which is documented at each call site.
//
// # Lock discipline
//
// Every exported method takes the shard lock itself. Every unexported method
// whose name ends in `Locked` assumes the caller already holds the appropriate
// lock and must never take it again -- Go's sync.RWMutex is not reentrant, so
// a locked function calling an exported one self-deadlocks. That exact bug
// (Put -> evict -> Del, all wanting the same write lock) froze the original
// server permanently on the 201st key.
type Store struct {
	shards []*shard
	// mask turns a hash into a shard index. Requires len(shards) to be a
	// power of two, which newStore enforces.
	mask uint64
	seed maphash.Seed

	// Counters are process-wide and updated with atomics rather than under a
	// shard lock, so INFO never has to acquire 16 mutexes.
	stats Stats
}

// shard is one independently locked partition of the keyspace.
type shard struct {
	mu   sync.RWMutex
	data map[string]*Obj
	// maxKeys is this shard's slice of the global budget.
	maxKeys int
	// evictRand is a per-shard PRNG for LRU sampling. Kept per-shard and
	// used only under the shard's write lock so it needs no lock of its own
	// and creates no cross-shard contention.
	evictRand uint64
}

// Stats holds server-wide counters. All fields are accessed atomically.
type Stats struct {
	Hits        atomic.Int64
	Misses      atomic.Int64
	Expired     atomic.Int64
	Evicted     atomic.Int64
	CmdsHandled atomic.Int64
	Connections atomic.Int64
	// TotalConnections is cumulative; Connections is the current gauge.
	TotalConnections atomic.Int64
}

// KV is the default store instance used by the command evaluator.
var KV *Store

// init gives KV a value built from the compiled-in defaults so that tests and
// library users never see a nil store. main calls ReinitStore after flag.Parse
// to rebuild it with the requested shard count and key budget.
func init() { KV = NewStore(config.NumShards, config.MaxKeys) }

// ReinitStore rebuilds KV from the current config, discarding any contents.
//
// Necessary because the shard count and per-shard key budget are fixed at
// construction time, and the config they come from is not final until
// flag.Parse has run. Must be called before the server accepts connections and
// before the AOF is replayed.
func ReinitStore() { KV = NewStore(config.NumShards, config.MaxKeys) }

// NewStore builds a store with numShards partitions sharing a maxKeys budget.
// numShards is rounded up to the next power of two so shard selection can use
// a bitmask instead of a modulo.
func NewStore(numShards, maxKeys int) *Store {
	n := 1
	for n < numShards {
		n <<= 1
	}
	perShard := maxKeys / n
	if perShard < 1 {
		perShard = 1
	}
	s := &Store{
		shards: make([]*shard, n),
		mask:   uint64(n - 1),
		seed:   maphash.MakeSeed(),
	}
	for i := range s.shards {
		s.shards[i] = &shard{
			data:    make(map[string]*Obj),
			maxKeys: perShard,
			// Seed each shard's PRNG distinctly and non-zero; xorshift
			// is stuck at zero forever if seeded with zero.
			evictRand: uint64(i)*2862933555777941757 + 3037000493,
		}
	}
	return s
}

// Stats exposes the counter block for INFO.
func (s *Store) Stats() *Stats { return &s.stats }

// shardFor selects the partition owning k.
func (s *Store) shardFor(k string) *shard {
	return s.shards[maphash.String(s.seed, k)&s.mask]
}

// Get returns the object stored at k, or nil if it is absent or expired.
//
// This takes a *read* lock on the fast path, so any number of readers proceed
// concurrently. That matters because expiry makes reads sometimes mutate: a
// naive implementation deletes the expired key inline and therefore needs a
// write lock for every read, which is what the original code did.
//
// Instead this uses the standard fast-path/slow-path split: detect expiry
// under the read lock, then drop it, take the write lock, and *re-verify*
// before deleting. Re-verification is mandatory -- between RUnlock and Lock
// another goroutine may have deleted the key, or replaced it with a fresh
// value carrying a new TTL, and deleting blindly would destroy live data.
func (s *Store) Get(k string) *Obj {
	sh := s.shardFor(k)
	now := time.Now().UnixMilli()

	sh.mu.RLock()
	obj, ok := sh.data[k]
	if !ok {
		sh.mu.RUnlock()
		s.stats.Misses.Add(1)
		return nil
	}
	if !obj.IsExpired(now) {
		// Atomic store under a read lock: see Obj.lastAccess.
		obj.Touch(now)
		sh.mu.RUnlock()
		s.stats.Hits.Add(1)
		return obj
	}
	sh.mu.RUnlock()

	// Slow path: the key looked expired. Re-check under the write lock,
	// because our observation is now stale.
	sh.mu.Lock()
	if cur, still := sh.data[k]; still && cur == obj && cur.IsExpired(time.Now().UnixMilli()) {
		delete(sh.data, k)
		sh.mu.Unlock()
		s.stats.Expired.Add(1)
		s.stats.Misses.Add(1)
		return nil
	}
	// Someone else already handled it, or it was replaced by a live value.
	sh.mu.Unlock()
	s.stats.Misses.Add(1)
	return nil
}

// Put stores obj at k, evicting first if the shard is at capacity.
//
// Returns false only under the noeviction policy when the shard is full, which
// the caller surfaces to the client as an OOM error.
func (s *Store) Put(k string, obj *Obj) bool {
	sh := s.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	// Overwriting an existing key consumes no new capacity, so check the
	// limit only for genuinely new keys. The original code did not, which
	// meant a workload that repeatedly SET the same key still triggered
	// eviction passes forever.
	if _, exists := sh.data[k]; !exists && len(sh.data) >= sh.maxKeys {
		if config.EvictionStrategy == config.EvictNoEviction {
			return false
		}
		// evictLocked is the unlocked-core variant. Calling the exported
		// Del here instead is what caused the original deadlock.
		s.evictLocked(sh)
	}
	sh.data[k] = obj
	return true
}

// Del removes k and reports whether it existed.
func (s *Store) Del(k string) bool {
	sh := s.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.data[k]; ok {
		delete(sh.data, k)
		return true
	}
	return false
}

// Exists reports whether k is present and unexpired, without updating LRU.
func (s *Store) Exists(k string) bool {
	sh := s.shardFor(k)
	now := time.Now().UnixMilli()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	obj, ok := sh.data[k]
	return ok && !obj.IsExpired(now)
}

// SetExpiry attaches an absolute expiry timestamp to an existing key.
// Returns false if the key is absent or already expired.
func (s *Store) SetExpiry(k string, expiresAt int64) bool {
	sh := s.shardFor(k)
	now := time.Now().UnixMilli()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	obj, ok := sh.data[k]
	if !ok || obj.IsExpired(now) {
		return false
	}
	obj.ExpiresAt = expiresAt
	return true
}

// Increment atomically adds delta to the integer value at k, creating it at
// zero if absent, and returns the new value.
//
// The whole read-modify-write happens under one write lock. The original code
// did this outside any lock -- Get returned the pointer, released the lock,
// and the caller mutated obj.Value -- a textbook lost-update race that went
// live the moment a second goroutine touched the store (which --mcp does).
func (s *Store) Increment(k string, delta int64) (int64, error) {
	sh := s.shardFor(k)
	now := time.Now().UnixMilli()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, ok := sh.data[k]
	if ok && obj.IsExpired(now) {
		// An expired key behaves as absent for INCR, and its old TTL must
		// not carry over to the new value.
		delete(sh.data, k)
		ok = false
	}

	if !ok {
		if len(sh.data) >= sh.maxKeys {
			if config.EvictionStrategy == config.EvictNoEviction {
				return 0, ErrOOM
			}
			s.evictLocked(sh)
		}
		obj = &Obj{
			Value:        "0",
			ExpiresAt:    NoExpiry,
			lastAccess:   now,
			typeEncoding: ObjTypeString | ObjEncodingInt,
		}
		sh.data[k] = obj
	}

	if obj.Type() != ObjTypeString {
		return 0, ErrWrongType
	}
	// Checking the encoding is a cheap pre-filter, but the authoritative
	// test is the parse below: a value can be stored as embstr and still be
	// numeric if it was written before an encoding change.
	cur, err := parseInt64(obj.StringValue())
	if err != nil {
		return 0, ErrNotInteger
	}
	// Reject the operation rather than silently wrapping around, matching
	// Redis's "increment or decrement would overflow" error.
	if (delta > 0 && cur > maxInt64-delta) || (delta < 0 && cur < minInt64-delta) {
		return 0, ErrIncrOverflow
	}
	next := cur + delta
	obj.setStringValue(formatInt64(next))
	obj.Touch(now)
	return next, nil
}

// Len returns the total live key count.
//
// This is eventually consistent: it sums the shards one at a time, so a
// concurrent writer may land in a shard already counted. Redis's DBSIZE has
// the same property under its own background jobs, and the alternative --
// holding all 16 locks at once -- would let any DBSIZE stall every writer.
func (s *Store) Len() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += len(sh.data)
		sh.mu.RUnlock()
	}
	return n
}

// Keys returns every live key matching pattern.
//
// O(N) over the keyspace, like Redis's KEYS, and equally unsuitable for
// production use on a large dataset. Locks are taken and released one shard at
// a time so a big keyspace does not block all writers for the whole scan.
func (s *Store) Keys(pattern string) []string {
	out := make([]string, 0, 64)
	now := time.Now().UnixMilli()
	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, obj := range sh.data {
			if obj.IsExpired(now) {
				continue
			}
			if matchGlob(pattern, k) {
				out = append(out, k)
			}
		}
		sh.mu.RUnlock()
	}
	return out
}

// Flush empties the keyspace and returns how many keys were dropped.
func (s *Store) Flush() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		n += len(sh.data)
		sh.data = make(map[string]*Obj)
		sh.mu.Unlock()
	}
	return n
}

// Append concatenates suffix onto the string at k, creating it if absent, and
// returns the resulting length.
//
// Like Increment, the whole read-modify-write is one critical section. Doing it
// as Get-then-Put in the caller would lose concurrent appends.
func (s *Store) Append(k, suffix string) (int64, error) {
	sh := s.shardFor(k)
	now := time.Now().UnixMilli()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, ok := sh.data[k]
	if ok && obj.IsExpired(now) {
		delete(sh.data, k)
		ok = false
	}
	if !ok {
		if len(sh.data) >= sh.maxKeys {
			if config.EvictionStrategy == config.EvictNoEviction {
				return 0, ErrOOM
			}
			s.evictLocked(sh)
		}
		obj = &Obj{ExpiresAt: NoExpiry, lastAccess: now}
		obj.setStringValue(suffix)
		sh.data[k] = obj
		return int64(len(suffix)), nil
	}
	if obj.Type() != ObjTypeString {
		return 0, ErrWrongType
	}
	// APPEND preserves the existing TTL, matching Redis.
	next := obj.StringValue() + suffix
	obj.setStringValue(next)
	obj.Touch(now)
	return int64(len(next)), nil
}

// Rename moves the value at src to dst, preserving its TTL.
// Returns false if src does not exist.
func (s *Store) Rename(src, dst string) bool {
	obj := s.Get(src)
	if obj == nil {
		return false
	}
	// Renaming a key onto itself is a no-op. Without this check the sequence
	// below writes the key and then immediately deletes it, so `RENAME k k`
	// silently destroys the value -- and if the two locks were taken together
	// instead, the same-shard case would deadlock against itself.
	if src == dst {
		return true
	}
	// Copy under the source lock, then write to the destination, then delete
	// the source. Taking both shard locks at once would be atomic but
	// introduces a lock-ordering problem: one goroutine renaming a->b while
	// another renames b->a can each hold the lock the other needs. Holding one
	// lock at a time trades a narrow visibility window -- a concurrent reader
	// can briefly observe both keys -- for a guarantee that RENAME can never
	// deadlock. Redis avoids the question entirely by being single-threaded.
	cp := *obj
	s.Put(dst, &cp)
	s.Del(src)
	return true
}

// Snapshot returns a copy of every live key/object pair, for AOF rewriting.
//
// It copies the map entries rather than handing out the live maps so the AOF
// writer can serialise to disk without holding any store lock -- writing to a
// file while holding a mutex is exactly the "slow work inside the critical
// section" mistake that stalls the whole server.
func (s *Store) Snapshot() map[string]*Obj {
	out := make(map[string]*Obj, s.Len())
	now := time.Now().UnixMilli()
	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, obj := range sh.data {
			if obj.IsExpired(now) {
				continue
			}
			// Shallow copy: Value is an immutable string and the other
			// fields are scalars, so the AOF writer sees a stable view
			// even if the live object is mutated afterwards.
			cp := *obj
			out[k] = &cp
		}
		sh.mu.RUnlock()
	}
	return out
}
