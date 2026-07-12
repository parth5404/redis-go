package core

import (
	"github/redis.go/config"
	"sync"
	"time"
)

var store map[string]*Obj
var RWmutex sync.RWMutex

func init() {
	store = make(map[string]*Obj)
}

func NewObj(value interface{}, durationMs int64, oType uint8, oEnc uint8) *Obj {
	var expiresAt int64 = -1
	if durationMs > 0 {
		expiresAt = time.Now().UnixMilli() + durationMs
	}
	return &Obj{
		Value:        value,
		ExpiresAt:    expiresAt,
		TypeEncoding: oType | oEnc,
	}
}

func Put(k string, obj *Obj) {
	RWmutex.Lock()
	defer RWmutex.Unlock()
	if len(store) >= config.KeyLimit {
		evict()
	}
	store[k] = obj
	if KeyspaceStat[0] == nil {
		KeyspaceStat[0] = make(map[string]int)
	}
	KeyspaceStat[0]["keys"]++
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
		KeyspaceStat[0]["keys"]--
		return true
	}
	return false
}
