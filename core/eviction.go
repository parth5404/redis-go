package core

func evictFirst() {
	for k := range store {
		delete(store, k)
		return
	}
}
func evict() {
	evictFirst()
}
