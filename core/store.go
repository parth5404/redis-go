package core

import (
	"github/redis.go/config"
	"sync"
	"time"
)

var store map[string]*Obj
var RWmutex sync.RWMutex

type Obj struct {
	Value     interface{}
	ExpiresAt int64
}

func init() {
	store = make(map[string]*Obj)
}

func NewObj(value interface{}, durationMs int64) *Obj {
	var expiresAt int64 = -1
	if durationMs > 0 {
		expiresAt = time.Now().UnixMilli() + durationMs
	}
	return &Obj{
		Value:     value,
		ExpiresAt: expiresAt,
	}
}

func Put(k string, obj *Obj) {
	RWmutex.Lock()
	defer RWmutex.Unlock()
	if len(store) >= config.KeyLimit {
		evict()
	}
	store[k] = obj
}

func Get(k string) *Obj {
	RWmutex.Lock()
	defer RWmutex.Unlock()
	v := store[k]
	if v != nil && v.ExpiresAt != -1 && time.Now().UnixMilli() >= v.ExpiresAt {
		delete(store, k)
		return nil
	}
	return v
}

func Del(k string) bool {
	RWmutex.Lock()
	defer RWmutex.Unlock()
	if _, ok := store[k]; ok {
		delete(store, k)
		return true
	}
	return false
}
