package core

import (
	"log"
	"time"
)

func expireSample() float32 {
	var limit int = 20
	var expiredCount int = 0

	for key, obj := range store {
		if obj.ExpiresAt != -1 && time.Now().UnixMilli() >= obj.ExpiresAt {
			delete(store, key)
			expiredCount++
		}
	}
	return float32(expiredCount) / float32(limit)
}

func DelExpireKeys() {
	log.Printf("Sher")
	for {
		frac := expireSample()
		if frac < 0.25 {
			break
		}
	}
	log.Println("deleted the expired but undeleted keys. total keys", len(store))
}
