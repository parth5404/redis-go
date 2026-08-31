package core

// sampleSize is how many keys one sweep round looks at. Redis uses 20 and the
// 0.25 threshold below is calibrated against that number.
const sampleSize = 20

// expireSample deletes the expired keys among a random sample of the keyspace
// and returns the fraction of that sample which turned out to be expired.
func expireSample() float32 {
	RWmutex.Lock()
	defer RWmutex.Unlock()

	sampled, expired := 0, 0
	for key, obj := range store {
		if sampled == sampleSize {
			break
		}
		sampled++
		if isExpired(obj) {
			delete(store, key)
			trackKeyRemoved()
			expired++
		}
	}
	if sampled == 0 {
		return 0
	}
	return float32(expired) / float32(sampled)
}

// DelExpireKeys runs the active half of expiry. Keys are also dropped lazily on
// read, but a key nobody ever reads again would otherwise sit in memory
// forever, so the sweep keeps sampling while hits stay dense — and stops early
// once they thin out, to avoid stalling the event loop.
func DelExpireKeys() {
	for itr := 0; itr < 16; itr++ {
		if expireSample() < 0.25 {
			return
		}
	}
}
