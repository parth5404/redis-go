package core

import "github/redis.go/config"

func evictFirst() {
	for k := range store {
		delete(store, k)
		return
	}
}

func evictAllkeysRandom() {
	cnt := int64(config.EvictionRatio * float32(config.KeyLimit))
	for k := range store {
		Del(k)
		cnt--
		if cnt < 0 {
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
