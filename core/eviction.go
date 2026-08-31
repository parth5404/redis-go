package core

import "github/redis.go/config"

// Both eviction strategies run from putLocked, which already holds the store's
// write lock. They therefore delete straight out of the map: routing through
// Del would try to take the same non-reentrant mutex a second time and wedge
// the event-loop goroutine for good.

func evictFirst() {
	for k := range store {
		delete(store, k)
		trackKeyRemoved()
		return
	}
}

// evictAllkeysRandom drops a slice of the keyspace, leaning on Go's randomised
// map iteration order for the "random" part.
func evictAllkeysRandom() {
	cnt := int64(config.EvictionRatio * float32(config.KeyLimit))
	if cnt < 1 {
		cnt = 1
	}
	for k := range store {
		delete(store, k)
		trackKeyRemoved()
		cnt--
		if cnt <= 0 {
			break
		}
	}
}

func evict() {
	switch config.EvictionStrategy {
	case "allkeys-random":
		evictAllkeysRandom()
	default:
		evictFirst()
	}
}
