package core

import (
	"time"
)

func expireSample() float32 {
	RWmutex.Lock()
	defer RWmutex.Unlock()
	var limit int = 20
	var expiredCount int = 0

	for key, obj := range store {
		if obj.ExpiresAt != -1 && time.Now().UnixMilli() >= obj.ExpiresAt {
			delete(store, key)
			expiredCount++
			limit--
		}
		if limit == 0 {
			break
		}
	}
	return float32(expiredCount) / float32(limit)
}

func DelExpireKeys() {
	//log.Printf("Sher")
	itr := 0
	for {
		frac := expireSample()
		if frac < 0.25 {
			break
		}
		if itr > 16 {
			break
		}
		itr++
	}
	//log.Println("deleted the expired but undeleted keys. total keys", len(store))
}
