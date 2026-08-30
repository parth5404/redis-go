package core

import (
	"github/redis.go/config"
)

// evictLocked frees capacity in sh. The caller MUST already hold sh.mu for
// writing.
//
// The `Locked` suffix is the contract: this function never touches a mutex.
// The original code's eviction path called the exported Del(), which takes the
// write lock its caller was already holding, and Go's RWMutex is not reentrant
// -- so the goroutine blocked waiting for a lock it owned, forever. That is why
// the split exists, and why the naming convention is worth enforcing.
func (s *Store) evictLocked(sh *shard) {
	// Evict a batch rather than a single key. Freeing one slot per insert
	// means every subsequent write pays an eviction; freeing 10% amortises
	// the scan cost across many inserts.
	target := int(float64(sh.maxKeys) * config.EvictionRatio)
	if target < 1 {
		target = 1
	}

	switch config.EvictionStrategy {
	case config.EvictAllKeysLRU:
		s.evictLRULocked(sh, target)
	default:
		s.evictRandomLocked(sh, target)
	}
}

// evictRandomLocked drops keys in Go's randomised map iteration order.
//
// Go deliberately randomises the starting point of map range loops, so this is
// a genuinely arbitrary selection rather than always hitting the same keys --
// but it is only random *per loop*, so we take the whole batch from one pass.
func (s *Store) evictRandomLocked(sh *shard, target int) {
	freed := 0
	for k := range sh.data {
		delete(sh.data, k)
		freed++
		if freed >= target {
			break
		}
	}
	s.stats.Evicted.Add(int64(freed))
}

// evictLRULocked implements approximated LRU: for each victim needed, sample a
// handful of random keys and drop the one with the oldest access time.
//
// Exact LRU needs an intrusive linked list updated on every read, which turns
// every GET into a write and destroys read concurrency. Redis made the same
// trade and samples 5 keys by default; the approximation is close enough that
// the difference is invisible on real workloads.
func (s *Store) evictLRULocked(sh *shard, target int) {
	samples := config.EvictionSamples
	if samples < 1 {
		samples = 1
	}
	freed := 0
	for freed < target && len(sh.data) > 0 {
		var victim string
		var victimAt int64 = maxInt64
		seen := 0

		// Go gives no way to index into a map, so "sample k random keys"
		// becomes "walk the randomised iteration order and stop after k".
		// Because the starting offset is re-randomised on every range, each
		// pass inspects a different subset.
		for k, obj := range sh.data {
			if at := obj.LastAccess(); at < victimAt {
				victimAt, victim = at, k
			}
			seen++
			if seen >= samples {
				break
			}
		}
		if seen == 0 {
			break
		}
		delete(sh.data, victim)
		freed++
	}
	s.stats.Evicted.Add(int64(freed))
}
