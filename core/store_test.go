package core

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github/redis.go/config"
)

func TestLazyExpiryOnRead(t *testing.T) {
	resetStore(t)

	Put("k", NewObj("v", 20, OBJ_TYPE_STRING, oBJ_ENCODING_EMBSTR))
	if Get("k") == nil {
		t.Fatal("key expired before its TTL elapsed")
	}

	time.Sleep(40 * time.Millisecond)

	if Get("k") != nil {
		t.Error("expired key was still returned by Get")
	}
	// The lazy path must actually reclaim the entry, not just hide it.
	if Len() != 0 {
		t.Errorf("store still holds %d keys after the expired read", Len())
	}
}

func TestActiveExpirySweep(t *testing.T) {
	resetStore(t)

	// A key nobody reads again is exactly the case lazy expiry cannot reclaim,
	// so the sweep has to do it.
	for i := 0; i < 100; i++ {
		Put("k"+strconv.Itoa(i), NewObj("v", 10, OBJ_TYPE_STRING, oBJ_ENCODING_EMBSTR))
	}
	if Len() != 100 {
		t.Fatalf("setup stored %d keys, want 100", Len())
	}

	time.Sleep(30 * time.Millisecond)
	DelExpireKeys()

	if Len() == 100 {
		t.Error("the sweep reclaimed nothing")
	}
}

func TestExpireSampleReportsTheExpiredFraction(t *testing.T) {
	resetStore(t)

	// Half the sample expired, so the ratio must clear the 0.25 threshold that
	// tells DelExpireKeys to keep going.
	for i := 0; i < sampleSize/2; i++ {
		Put("live"+strconv.Itoa(i), NewObj("v", -1, OBJ_TYPE_STRING, oBJ_ENCODING_EMBSTR))
		Put("dead"+strconv.Itoa(i), NewObj("v", 5, OBJ_TYPE_STRING, oBJ_ENCODING_EMBSTR))
	}
	time.Sleep(20 * time.Millisecond)

	if frac := expireSample(); frac <= 0.25 {
		t.Errorf("expireSample() = %v, want more than 0.25", frac)
	}
}

// TestEvictionDoesNotDeadlock is a regression test.
//
// Put takes the store's write lock and then calls evict, which used to reclaim
// keys through Del — and Del takes that same non-reentrant mutex. Crossing the
// key limit therefore wedged the goroutine holding the lock, which on the event
// loop means every connection hangs at once, permanently.
func TestEvictionDoesNotDeadlock(t *testing.T) {
	resetStore(t)

	originalLimit, originalStrategy := config.KeyLimit, config.EvictionStrategy
	config.KeyLimit, config.EvictionStrategy = 50, "allkeys-random"
	t.Cleanup(func() {
		config.KeyLimit, config.EvictionStrategy = originalLimit, originalStrategy
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			Put("k"+strconv.Itoa(i), NewObj("v", -1, OBJ_TYPE_STRING, oBJ_ENCODING_EMBSTR))
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("writing past the key limit deadlocked")
	}

	if Len() > config.KeyLimit {
		t.Errorf("store holds %d keys, over the limit of %d", Len(), config.KeyLimit)
	}
}

func TestEvictionKeepsMemoryBounded(t *testing.T) {
	resetStore(t)

	originalLimit := config.KeyLimit
	config.KeyLimit = 100
	t.Cleanup(func() { config.KeyLimit = originalLimit })

	for i := 0; i < 1000; i++ {
		Put("k"+strconv.Itoa(i), NewObj("v", -1, OBJ_TYPE_STRING, oBJ_ENCODING_EMBSTR))
	}

	if Len() > config.KeyLimit {
		t.Errorf("store grew to %d keys despite a limit of %d", Len(), config.KeyLimit)
	}
	if Len() == 0 {
		t.Error("eviction emptied the store entirely")
	}
}

// TestConcurrentIncrBy is the reason IncrBy holds one lock across the whole
// read-modify-write. Under --mcp the RESP event loop and the MCP stdio server
// drive commands from two different goroutines, so a read and a write split
// across two locked sections loses increments. Run with -race.
func TestConcurrentIncrBy(t *testing.T) {
	resetStore(t)

	const goroutines, perGoroutine = 8, 500

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				if _, err := IncrBy("counter", 1); err != nil {
					t.Errorf("IncrBy returned error: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	obj := Get("counter")
	if obj == nil {
		t.Fatal("counter disappeared")
	}
	got, err := strconv.ParseInt(obj.Value.(string), 10, 64)
	if err != nil {
		t.Fatalf("counter holds %q: %v", obj.Value, err)
	}
	if want := int64(goroutines * perGoroutine); got != want {
		t.Errorf("counter = %d, want %d (%d increments were lost)", got, want, want-got)
	}
}

func TestIncrByRejectsNonIntegers(t *testing.T) {
	resetStore(t)
	Put("k", NewObj("hello", -1, OBJ_TYPE_STRING, oBJ_ENCODING_EMBSTR))

	if _, err := IncrBy("k", 1); err == nil {
		t.Error("IncrBy accepted a non-integer value")
	}
}

func TestIncrByDetectsOverflow(t *testing.T) {
	resetStore(t)
	Put("k", NewObj(strconv.FormatInt(1<<63-1, 10), -1, OBJ_TYPE_STRING, OBJ_ENCODING_INT))

	if _, err := IncrBy("k", 1); err == nil {
		t.Error("IncrBy wrapped past the int64 maximum instead of erroring")
	}
}

func TestExpireOnMissingKey(t *testing.T) {
	resetStore(t)
	if Expire("nope", 1000) {
		t.Error("Expire reported success for a key that does not exist")
	}
}

// TestAOFRoundTrip covers the "survives a restart" path: dump the keyspace,
// wipe it the way a process exit would, and reload from disk.
func TestAOFRoundTrip(t *testing.T) {
	resetStore(t)

	originalAOF := config.AOFfile
	config.AOFfile = filepath.Join(t.TempDir(), "appendonly.aof")
	t.Cleanup(func() { config.AOFfile = originalAOF })

	run(t, "SET", "plain", "value")
	run(t, "SET", "spaced", "a value with spaces")
	run(t, "SET", "number", "42")
	run(t, "SET", "temporary", "v", "EX", "600")

	if err := DumpAlLAof(); err != nil {
		t.Fatalf("DumpAlLAof returned error: %v", err)
	}

	resetStore(t)
	if Len() != 0 {
		t.Fatal("store was not cleared before the reload")
	}
	LoadAof()

	tests := []struct {
		key  string
		want string
	}{
		{key: "plain", want: "value"},
		{key: "spaced", want: "a value with spaces"},
		{key: "number", want: "42"},
		{key: "temporary", want: "v"},
	}
	for _, tt := range tests {
		obj := Get(tt.key)
		if obj == nil {
			t.Errorf("key %q did not survive the reload", tt.key)
			continue
		}
		if obj.Value != tt.want {
			t.Errorf("key %q reloaded as %q, want %q", tt.key, obj.Value, tt.want)
		}
	}

	// The TTL has to come back too, or a restart would silently make expiring
	// keys permanent.
	if obj := Get("temporary"); obj != nil && obj.ExpiresAt == -1 {
		t.Error("the TTL was lost across the reload")
	}
}

func TestLoadAofIgnoresGarbage(t *testing.T) {
	resetStore(t)

	originalAOF := config.AOFfile
	config.AOFfile = filepath.Join(t.TempDir(), "appendonly.aof")
	t.Cleanup(func() { config.AOFfile = originalAOF })

	// A file truncated by a crash mid-write must not take the server down on
	// the next boot.
	if err := os.WriteFile(config.AOFfile, []byte("*3\r\n$3\r\nSET\r\n$1\r\nk"), 0644); err != nil {
		t.Fatal(err)
	}

	LoadAof()

	if Len() != 0 {
		t.Errorf("a truncated AOF restored %d keys", Len())
	}
}
