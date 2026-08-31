package core

import (
	"errors"
	"github/redis.go/config"
	"strconv"
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

// isExpired reports whether obj's TTL has elapsed. -1 means "no expiry".
func isExpired(obj *Obj) bool {
	return obj.ExpiresAt != -1 && time.Now().UnixMilli() >= obj.ExpiresAt
}

func Put(k string, obj *Obj) {
	RWmutex.Lock()
	defer RWmutex.Unlock()
	putLocked(k, obj)
}

// putLocked inserts obj at k, evicting first if the keyspace is at its cap.
// The caller must already hold the write lock.
func putLocked(k string, obj *Obj) {
	if len(store) >= config.KeyLimit {
		evict()
	}
	store[k] = obj
	trackKeyAdded()
}

func Get(k string) *Obj {
	RWmutex.Lock()
	defer RWmutex.Unlock()
	v := store[k]
	if v != nil && isExpired(v) {
		delete(store, k)
		trackKeyRemoved()
		return nil
	}
	return v
}

func Del(k string) bool {
	RWmutex.Lock()
	defer RWmutex.Unlock()
	if _, ok := store[k]; ok {
		delete(store, k)
		trackKeyRemoved()
		return true
	}
	return false
}

// Expire attaches a new TTL to an existing key and reports whether the key was
// there to update.
func Expire(k string, durationMs int64) bool {
	RWmutex.Lock()
	defer RWmutex.Unlock()

	obj, ok := store[k]
	if !ok {
		return false
	}
	if isExpired(obj) {
		delete(store, k)
		trackKeyRemoved()
		return false
	}
	obj.ExpiresAt = time.Now().UnixMilli() + durationMs
	return true
}

// IncrBy applies delta to the integer held at k, creating the key at zero if it
// is absent, and returns the new value.
//
// The whole read-modify-write runs inside one write lock on purpose. Reading
// through Get and then writing the object back would release the lock in
// between, and under --mcp the RESP event loop and the MCP stdio server issue
// commands from two different goroutines against this same map, so that gap is
// long enough to lose an increment.
func IncrBy(k string, delta int64) (int64, error) {
	RWmutex.Lock()
	defer RWmutex.Unlock()

	obj, ok := store[k]
	if ok && isExpired(obj) {
		delete(store, k)
		trackKeyRemoved()
		ok = false
	}
	if !ok {
		obj = NewObj("0", -1, OBJ_TYPE_STRING, OBJ_ENCODING_INT)
		putLocked(k, obj)
	}

	if err := assertType(obj.TypeEncoding, OBJ_TYPE_STRING); err != nil {
		return 0, err
	}
	if err := assertEncoding(obj.TypeEncoding, OBJ_ENCODING_INT); err != nil {
		return 0, err
	}

	valStr, ok := obj.Value.(string)
	if !ok {
		return 0, errors.New("ERR value is not an integer or out of range")
	}
	val, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		return 0, errors.New("ERR value is not an integer or out of range")
	}

	next := val + delta
	// Signs matching each other but not the result means the addition wrapped.
	if (delta > 0 && next < val) || (delta < 0 && next > val) {
		return 0, errors.New("ERR increment or decrement would overflow")
	}
	obj.Value = strconv.FormatInt(next, 10)
	return next, nil
}

// Len returns the number of keys currently held, expired-but-not-yet-swept
// entries included.
func Len() int {
	RWmutex.RLock()
	defer RWmutex.RUnlock()
	return len(store)
}
