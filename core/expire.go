package core

import (
	"time"

	"github/redis.go/config"
)

// Expiry uses the same two-pronged strategy as Redis:
//
//   - Lazy (passive): Store.Get deletes a key it finds expired. Cheap, but a
//     key nobody ever reads again is never reclaimed, so memory leaks.
//   - Active: the sweep below samples keys periodically and reclaims them
//     regardless of whether anyone is reading.
//
// The active sweep must not scan the whole keyspace, because it holds a shard
// lock while it runs and a full scan of a million keys would stall every client
// for the duration. Instead it samples a fixed number of keys, measures what
// fraction were expired, and only samples again if that fraction was high --
// which adapts the effort to how much garbage actually exists.

// expireSampleShard inspects up to config.ExpirySampleSize keys in one shard
// and returns the fraction of *inspected* keys that were expired.
//
// The original implementation divided by its own countdown variable rather than
// by the number of keys inspected, which produced a division by zero (yielding
// +Inf) whenever the sample was fully expired, and only decremented that
// countdown on expired keys -- so on a large keyspace with few expiring keys it
// walked every single key while holding the write lock.
func (s *Store) expireSampleShard(sh *shard) float64 {
	limit := config.ExpirySampleSize
	if limit < 1 {
		limit = 1
	}
	now := time.Now().UnixMilli()

	sh.mu.Lock()
	defer sh.mu.Unlock()

	inspected, expired := 0, 0
	for k, obj := range sh.data {
		// Count every key we look at, not just the expired ones -- that is
		// what makes this a bounded sample.
		inspected++
		if obj.IsExpired(now) {
			delete(sh.data, k)
			expired++
		}
		if inspected >= limit {
			break
		}
	}
	if inspected == 0 {
		return 0
	}
	s.stats.Expired.Add(int64(expired))
	return float64(expired) / float64(inspected)
}

// ActiveExpireCycle runs one adaptive sweep across all shards.
func (s *Store) ActiveExpireCycle() {
	const maxRounds = 16
	for _, sh := range s.shards {
		for round := 0; round < maxRounds; round++ {
			// Re-sample the same shard only while it keeps yielding a high
			// proportion of expired keys; otherwise move on. This is the
			// adaptive part: idle shards cost one sample, garbage-heavy
			// shards get cleaned aggressively.
			if s.expireSampleShard(sh) < config.ExpiryRetryThreshold {
				break
			}
		}
	}
}

// StartActiveExpiry launches the background sweep and returns a stop function.
//
// Returning a stopper rather than leaking the goroutine matters for tests: a
// ticker goroutine that outlives the test keeps mutating the store under the
// next test's feet.
func (s *Store) StartActiveExpiry() (stop func()) {
	done := make(chan struct{})
	ticker := time.NewTicker(time.Duration(config.ExpiryCronFrequency) * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.ActiveExpireCycle()
			case <-done:
				return
			}
		}
	}()
	var once bool
	return func() {
		if !once {
			once = true
			close(done)
		}
	}
}
