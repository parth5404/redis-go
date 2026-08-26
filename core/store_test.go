package core

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github/redis.go/config"
)

// Store tests.
//
// Two of these are regression tests for bugs that made the original server
// unusable rather than merely wrong: TestPutBeyondMaxKeysDoesNotDeadlock (the
// server froze permanently once the key limit was reached) and
// TestConcurrentIncrementIsAtomic (INCR lost updates under concurrency).

// newTestStore builds an isolated store so tests never interfere with the
// package-level KV or with each other.
func newTestStore(t *testing.T, shards, maxKeys int) *Store {
	t.Helper()
	return NewStore(shards, maxKeys)
}

// withStore swaps the package-level KV for the duration of a test, because the
// command handlers operate on it directly.
func withStore(t *testing.T, s *Store) {
	t.Helper()
	prev := KV
	KV = s
	t.Cleanup(func() { KV = prev })
}

func TestPutAndGet(t *testing.T) {
	s := newTestStore(t, 4, 1000)

	if got := s.Get("missing"); got != nil {
		t.Fatalf("Get on an absent key returned %#v, want nil", got)
	}
	s.Put("k", NewStringObj("v", NoExpiry))

	obj := s.Get("k")
	if obj == nil {
		t.Fatal("Get returned nil for a key just written")
	}
	if obj.StringValue() != "v" {
		t.Fatalf("got %q, want %q", obj.StringValue(), "v")
	}
}

func TestOverwriteDoesNotGrowTheKeyspace(t *testing.T) {
	s := newTestStore(t, 1, 1000)
	for i := 0; i < 100; i++ {
		s.Put("same", NewStringObj(fmt.Sprint(i), NoExpiry))
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("Len = %d after 100 writes to one key, want 1", got)
	}
}

func TestDelAndExists(t *testing.T) {
	s := newTestStore(t, 4, 1000)
	s.Put("k", NewStringObj("v", NoExpiry))

	if !s.Exists("k") {
		t.Fatal("Exists = false for a live key")
	}
	if !s.Del("k") {
		t.Fatal("Del reported no deletion for a live key")
	}
	if s.Del("k") {
		t.Fatal("Del reported a deletion for an already-deleted key")
	}
	if s.Exists("k") {
		t.Fatal("Exists = true after Del")
	}
}

// TestGetOnExpiredKeyDeletesIt covers lazy expiry: a read is what discovers most
// expired keys, since the active sweep only samples.
func TestGetOnExpiredKeyDeletesIt(t *testing.T) {
	s := newTestStore(t, 4, 1000)
	// An expiry already in the past.
	s.Put("k", NewStringObj("v", time.Now().UnixMilli()-1))

	if got := s.Get("k"); got != nil {
		t.Fatalf("Get returned %#v for an expired key, want nil", got)
	}
	// The key must actually be gone, not merely hidden: otherwise memory is
	// never reclaimed and DBSIZE lies.
	if s.Len() != 0 {
		t.Fatalf("Len = %d after reading an expired key, want 0", s.Len())
	}
	if s.Stats().Expired.Load() != 1 {
		t.Fatalf("Expired counter = %d, want 1", s.Stats().Expired.Load())
	}
}

func TestExpiryIsAbsoluteNotRelative(t *testing.T) {
	s := newTestStore(t, 4, 1000)
	deadline := time.Now().UnixMilli() + 50
	s.Put("k", NewStringObj("v", deadline))

	if s.Get("k") == nil {
		t.Fatal("key expired immediately")
	}
	time.Sleep(80 * time.Millisecond)
	if got := s.Get("k"); got != nil {
		t.Fatalf("key survived its deadline: %#v", got)
	}
}

func TestSetExpiryAndPersist(t *testing.T) {
	s := newTestStore(t, 4, 1000)
	s.Put("k", NewStringObj("v", NoExpiry))

	if !s.SetExpiry("k", time.Now().UnixMilli()+10_000) {
		t.Fatal("SetExpiry failed on a live key")
	}
	if s.Get("k").ExpiresAt == NoExpiry {
		t.Fatal("expiry was not recorded")
	}
	if !s.SetExpiry("k", NoExpiry) {
		t.Fatal("clearing the expiry failed")
	}
	if s.Get("k").ExpiresAt != NoExpiry {
		t.Fatal("expiry was not cleared")
	}
	if s.SetExpiry("absent", 1) {
		t.Fatal("SetExpiry succeeded on an absent key")
	}
}

// TestPutBeyondMaxKeysDoesNotDeadlock is the regression test for the bug that
// froze the original server permanently.
//
// Put held the shard's write lock, then called the exported Del to evict, which
// tried to take the same write lock. Go's sync.RWMutex is not reentrant, so the
// goroutine blocked on a lock it already held -- with no timeout, no panic and no
// log line. Every subsequent request queued behind it.
//
// A test that merely called Put would hang forever rather than fail, so the work
// runs in a goroutine and the test asserts on a deadline.
func TestPutBeyondMaxKeysDoesNotDeadlock(t *testing.T) {
	// One shard so every key lands in the same map and the limit is reached
	// deterministically.
	s := newTestStore(t, 1, 200)

	done := make(chan int, 1)
	go func() {
		for i := 0; i < 1000; i++ {
			s.Put(fmt.Sprintf("k%d", i), NewStringObj("v", NoExpiry))
		}
		done <- s.Len()
	}()

	select {
	case n := <-done:
		if n > 200 {
			t.Fatalf("keyspace grew to %d with a limit of 200; eviction did not run", n)
		}
		if n == 0 {
			t.Fatal("eviction emptied the keyspace entirely")
		}
		if s.Stats().Evicted.Load() == 0 {
			t.Fatal("Evicted counter is zero although the limit was exceeded")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Put deadlocked past the key limit: this is the " +
			"Put -> evict -> Del self-deadlock (sync.RWMutex is not reentrant)")
	}
}

func TestNoEvictionPolicyRejectsWrites(t *testing.T) {
	prev := config.EvictionStrategy
	config.EvictionStrategy = config.EvictNoEviction
	t.Cleanup(func() { config.EvictionStrategy = prev })

	s := newTestStore(t, 1, 10)
	accepted := 0
	for i := 0; i < 50; i++ {
		if s.Put(fmt.Sprintf("k%d", i), NewStringObj("v", NoExpiry)) {
			accepted++
		}
	}
	if accepted != 10 {
		t.Fatalf("accepted %d writes with maxkeys=10 and noeviction, want 10", accepted)
	}
	if s.Len() != 10 {
		t.Fatalf("Len = %d, want 10", s.Len())
	}
}

// TestLRUEvictionPrefersColdKeys checks the approximated-LRU policy actually
// looks at access time. It is approximate by design -- it samples rather than
// scanning -- so the assertion is statistical: the key touched on every
// iteration must survive, since it can never be the oldest in any sample.
func TestLRUEvictionPrefersColdKeys(t *testing.T) {
	prev := config.EvictionStrategy
	config.EvictionStrategy = config.EvictAllKeysLRU
	t.Cleanup(func() { config.EvictionStrategy = prev })

	s := newTestStore(t, 1, 100)

	s.Put("hot", NewStringObj("v", NoExpiry))
	for i := 0; i < 500; i++ {
		s.Put(fmt.Sprintf("cold%d", i), NewStringObj("v", NoExpiry))
		// Touch the hot key so its LRU timestamp keeps advancing. The sleep
		// keeps the millisecond-resolution timestamps distinguishable.
		if i%10 == 0 {
			time.Sleep(time.Millisecond)
		}
		s.Get("hot")
	}
	if s.Get("hot") == nil {
		t.Fatal("the continuously-accessed key was evicted before colder keys")
	}
}

func TestKeysPattern(t *testing.T) {
	s := newTestStore(t, 4, 1000)
	for _, k := range []string{"user:1", "user:2", "session:1", "other"} {
		s.Put(k, NewStringObj("v", NoExpiry))
	}
	if got := len(s.Keys("user:*")); got != 2 {
		t.Fatalf("KEYS user:* matched %d keys, want 2", got)
	}
	if got := len(s.Keys("*")); got != 4 {
		t.Fatalf("KEYS * matched %d keys, want 4", got)
	}
	if got := len(s.Keys("nope*")); got != 0 {
		t.Fatalf("KEYS nope* matched %d keys, want 0", got)
	}
}

// TestKeysSkipsExpiredKeys matters because KEYS reads under a read lock and
// therefore cannot delete: it must filter instead, or it reports keys that a
// following GET says do not exist.
func TestKeysSkipsExpiredKeys(t *testing.T) {
	s := newTestStore(t, 2, 1000)
	s.Put("live", NewStringObj("v", NoExpiry))
	s.Put("dead", NewStringObj("v", time.Now().UnixMilli()-1))

	keys := s.Keys("*")
	if len(keys) != 1 || keys[0] != "live" {
		t.Fatalf("KEYS returned %#v, want [live]", keys)
	}
}

func TestFlush(t *testing.T) {
	s := newTestStore(t, 8, 1000)
	for i := 0; i < 100; i++ {
		s.Put(fmt.Sprintf("k%d", i), NewStringObj("v", NoExpiry))
	}
	if n := s.Flush(); n != 100 {
		t.Fatalf("Flush reported %d keys removed, want 100", n)
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d after Flush, want 0", s.Len())
	}
}

func TestIncrement(t *testing.T) {
	s := newTestStore(t, 4, 1000)

	// An absent key starts at zero.
	v, err := s.Increment("counter", 1)
	if err != nil || v != 1 {
		t.Fatalf("first increment: got (%d, %v), want (1, nil)", v, err)
	}
	v, _ = s.Increment("counter", 9)
	if v != 10 {
		t.Fatalf("got %d, want 10", v)
	}
	v, _ = s.Increment("counter", -20)
	if v != -10 {
		t.Fatalf("got %d, want -10", v)
	}

	// A non-numeric value must be rejected, not overwritten.
	s.Put("text", NewStringObj("abc", NoExpiry))
	if _, err := s.Increment("text", 1); err == nil {
		t.Fatal("incrementing a non-numeric value succeeded")
	}
	if s.Get("text").StringValue() != "abc" {
		t.Fatal("a failed increment modified the value")
	}
}

func TestIncrementOverflow(t *testing.T) {
	s := newTestStore(t, 4, 1000)
	s.Put("k", NewStringObj(formatInt64(maxInt64), NoExpiry))

	if _, err := s.Increment("k", 1); err == nil {
		t.Fatal("increment past MaxInt64 succeeded; the value silently wrapped negative")
	}
	if s.Get("k").StringValue() != formatInt64(maxInt64) {
		t.Fatal("a rejected increment still modified the value")
	}

	s.Put("min", NewStringObj(formatInt64(minInt64), NoExpiry))
	if _, err := s.Increment("min", -1); err == nil {
		t.Fatal("decrement past MinInt64 succeeded")
	}
}

// TestConcurrentIncrementIsAtomic is the regression test for the lost-update
// bug. The original read the value under one lock, computed the new value, then
// wrote it back under a second lock -- so two goroutines could both read 5 and
// both write 6, losing one increment. The final count is the only reliable
// detector: the race window is small enough that a few iterations usually pass.
func TestConcurrentIncrementIsAtomic(t *testing.T) {
	s := newTestStore(t, 16, 100_000)

	const goroutines = 50
	const perGoroutine = 200

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				if _, err := s.Increment("counter", 1); err != nil {
					t.Errorf("increment failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * perGoroutine)
	got, err := parseInt64(s.Get("counter").StringValue())
	if err != nil {
		t.Fatalf("counter is not numeric: %v", err)
	}
	if got != want {
		t.Fatalf("counter = %d after %d concurrent increments, want %d: "+
			"%d updates were lost to a read-modify-write race",
			got, want, want, want-got)
	}
}

// TestConcurrentMixedWorkload is the race-detector target. It is not asserting a
// specific value -- it is asserting that no combination of reads, writes,
// deletes, expiry, eviction and full-keyspace scans trips the detector or
// deadlocks. Run with -race.
func TestConcurrentMixedWorkload(t *testing.T) {
	s := newTestStore(t, 16, 5_000)

	const workers = 16
	const iterations = 2_000

	var wg sync.WaitGroup
	var errCount atomic.Int64

	deadline := time.Now().Add(20 * time.Second)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if time.Now().After(deadline) {
					return
				}
				k := fmt.Sprintf("k%d", (w*iterations+i)%1000)
				switch i % 7 {
				case 0:
					s.Put(k, NewStringObj("v", NoExpiry))
				case 1:
					s.Get(k)
				case 2:
					s.Del(k)
				case 3:
					// A short TTL so the expiry paths are exercised
					// concurrently with everything else.
					s.Put(k, NewStringObj("v", time.Now().UnixMilli()+5))
				case 4:
					if _, err := s.Increment("shared-counter", 1); err != nil {
						errCount.Add(1)
					}
				case 5:
					s.Exists(k)
				case 6:
					s.SetExpiry(k, time.Now().UnixMilli()+1000)
				}
			}
		}(w)
	}

	// Concurrent whole-keyspace operations, which take every shard lock in turn
	// and are the most likely to expose a lock-ordering mistake.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if time.Now().After(deadline) {
				return
			}
			s.Keys("k1*")
			s.Len()
			s.Snapshot()
			s.ActiveExpireCycle()
		}
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("mixed workload deadlocked")
	}

	if n := errCount.Load(); n != 0 {
		t.Fatalf("%d increments failed unexpectedly", n)
	}
}

func TestAppend(t *testing.T) {
	s := newTestStore(t, 4, 1000)

	// APPEND on an absent key behaves like SET.
	n, err := s.Append("k", "abc")
	if err != nil || n != 3 {
		t.Fatalf("got (%d, %v), want (3, nil)", n, err)
	}
	n, _ = s.Append("k", "de")
	if n != 5 {
		t.Fatalf("got %d, want 5", n)
	}
	if s.Get("k").StringValue() != "abcde" {
		t.Fatalf("got %q, want abcde", s.Get("k").StringValue())
	}
}

func TestRename(t *testing.T) {
	s := newTestStore(t, 8, 1000)
	s.Put("src", NewStringObj("v", NoExpiry))

	if !s.Rename("src", "dst") {
		t.Fatal("Rename failed")
	}
	if s.Exists("src") {
		t.Fatal("source key survived the rename")
	}
	if s.Get("dst").StringValue() != "v" {
		t.Fatal("value did not follow the rename")
	}
	if s.Rename("absent", "x") {
		t.Fatal("Rename succeeded on an absent source key")
	}
}

// TestRenameToSelfIsSafe is worth pinning: renaming a key onto itself is the
// case where a naive two-lock implementation deadlocks against itself.
func TestRenameToSelfIsSafe(t *testing.T) {
	s := newTestStore(t, 8, 1000)
	s.Put("k", NewStringObj("v", NoExpiry))

	done := make(chan bool, 1)
	go func() { done <- s.Rename("k", "k") }()

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("Rename k k reported failure")
		}
		if s.Get("k").StringValue() != "v" {
			t.Fatal("renaming a key onto itself lost the value")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Rename k k deadlocked (both locks are the same lock)")
	}
}

// TestSnapshotDoesNotAliasLiveObjects protects the AOF rewrite path: the
// snapshot is written to disk without holding any lock, so if it handed out the
// live *Obj pointers a concurrent write would be read without synchronisation.
func TestSnapshotDoesNotAliasLiveObjects(t *testing.T) {
	s := newTestStore(t, 4, 1000)
	s.Put("k", NewStringObj("before", NoExpiry))

	snap := s.Snapshot()
	s.Put("k", NewStringObj("after", NoExpiry))

	if snap["k"].StringValue() != "before" {
		t.Fatalf("snapshot changed under us: got %q, want %q",
			snap["k"].StringValue(), "before")
	}
}

func TestShardingDistributesKeys(t *testing.T) {
	s := newTestStore(t, 16, 100_000)
	for i := 0; i < 10_000; i++ {
		s.Put(fmt.Sprintf("key:%d", i), NewStringObj("v", NoExpiry))
	}
	if s.Len() != 10_000 {
		t.Fatalf("Len = %d, want 10000", s.Len())
	}

	// Every shard should hold some keys. A shard function that ignored part of
	// the hash would leave shards empty, silently reducing the parallelism the
	// design exists to provide.
	for i, sh := range s.shards {
		sh.mu.RLock()
		n := len(sh.data)
		sh.mu.RUnlock()
		if n == 0 {
			t.Errorf("shard %d is empty; the hash is not spreading keys", i)
		}
	}
}

func TestShardCountRoundsUpToPowerOfTwo(t *testing.T) {
	for _, tc := range []struct{ in, want int }{
		{1, 1}, {2, 2}, {3, 4}, {5, 8}, {16, 16}, {17, 32},
	} {
		s := NewStore(tc.in, 1000)
		if got := len(s.shards); got != tc.want {
			t.Errorf("NewStore(%d) made %d shards, want %d", tc.in, got, tc.want)
		}
		// The mask must match, or shardFor indexes out of range.
		if s.mask != uint64(tc.want-1) {
			t.Errorf("NewStore(%d): mask = %d, want %d", tc.in, s.mask, tc.want-1)
		}
	}
}

// TestActiveExpireCycleReclaimsMemory covers the background sweep, which is what
// reclaims keys nobody reads again. Without it, a write-only workload with TTLs
// grows without bound.
func TestActiveExpireCycleReclaimsMemory(t *testing.T) {
	s := newTestStore(t, 4, 10_000)

	past := time.Now().UnixMilli() - 1
	for i := 0; i < 500; i++ {
		s.Put(fmt.Sprintf("dead%d", i), NewStringObj("v", past))
	}
	s.Put("live", NewStringObj("v", NoExpiry))

	// The sweep samples, so several cycles are needed to clear 500 keys.
	for i := 0; i < 200 && s.Len() > 1; i++ {
		s.ActiveExpireCycle()
	}

	if s.Len() != 1 {
		t.Fatalf("Len = %d after repeated sweeps, want 1 (only the live key)", s.Len())
	}
	if s.Get("live") == nil {
		t.Fatal("the sweep deleted a key with no expiry")
	}
}

// TestExpireSampleShardOnEmptyShard is a regression test for a division by zero.
// The original computed the expired fraction by dividing by a countdown variable
// that reached zero when no keys were expired.
func TestExpireSampleShardOnEmptyShard(t *testing.T) {
	s := newTestStore(t, 1, 100)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("sampling an empty shard panicked: %v", r)
		}
	}()
	if got := s.expireSampleShard(s.shards[0]); got != 0 {
		t.Fatalf("expired fraction of an empty shard = %v, want 0", got)
	}
}

func TestStartActiveExpiryStops(t *testing.T) {
	s := newTestStore(t, 2, 1000)
	stop := s.StartActiveExpiry()

	s.Put("k", NewStringObj("v", time.Now().UnixMilli()+10))
	time.Sleep(time.Duration(config.ExpiryCronFrequency*4) * time.Millisecond)
	stop()

	// After stop() the ticker must be gone; calling it twice must not panic on
	// a double close.
	stop()
}
