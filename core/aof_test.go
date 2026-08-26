package core

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github/redis.go/config"
)

// AOF tests.
//
// The original persistence layer had four defects that each silently lost data,
// and every one of them has a test here:
//
//  1. Nothing was written unless a client sent BGREWRITE, so ordinary writes did
//     not survive a restart at all (TestPropagatedWritesSurviveAReload).
//  2. Records were built with fmt.Sprintf("SET %s %s") and reloaded by splitting
//     on spaces, so any key or value containing a space came back as a different
//     command (TestValuesWithSpacesAndBinaryRoundTrip).
//  3. TTLs were never persisted (TestTTLSurvivesAReload), and a relative TTL in
//     the log restarts its clock on every replay
//     (TestRelativeExpiryIsStoredAsAnAbsoluteDeadline).
//  4. The rewrite opened the live file with O_TRUNC, so a crash mid-rewrite
//     destroyed the whole dataset (TestRewriteIsAtomic).

// newTestAOF opens an AOF in a temporary directory and installs it as
// DefaultAOF, restoring the previous value afterwards.
func newTestAOF(t *testing.T) *AOF {
	t.Helper()
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := OpenAOF(path)
	if err != nil {
		t.Fatalf("OpenAOF: %v", err)
	}
	prev := DefaultAOF
	DefaultAOF = a
	t.Cleanup(func() {
		a.Close()
		DefaultAOF = prev
	})
	return a
}

// waitForAOFWrites blocks until the writer goroutine has accepted at least n
// records. Needed before anything that depends on ordering with the writer,
// because Propagate is asynchronous by design.
func waitForAOFWrites(t *testing.T, a *AOF, n int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if a.Snapshot().Written >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("only %d of %d records reached the writer within 10s",
		a.Snapshot().Written, n)
}

// closeAndFlush stops the writer, which drains the queue and flushes the bufio
// buffer to the file. Nothing else forces the 256 KB buffer out on demand, so
// any assertion about the file's contents or size has to go through here.
func closeAndFlush(t *testing.T, a *AOF) {
	t.Helper()
	a.Close()
}

// reload closes the AOF and replays it into a fresh store, which is the sequence
// main.go performs on startup.
func reload(t *testing.T, a *AOF) (*Store, int) {
	t.Helper()
	closeAndFlush(t, a)

	fresh := NewStore(8, 100_000)
	applied, err := LoadAOF(a.path, fresh)
	if err != nil {
		t.Fatalf("LoadAOF: %v", err)
	}
	withStore(t, fresh)
	return fresh, applied
}

func TestPropagatedWritesSurviveAReload(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	exec(t, "SET", "a", "1")
	exec(t, "SET", "b", "2")
	exec(t, "INCR", "counter")
	exec(t, "APPEND", "s", "xy")
	exec(t, "MSET", "m1", "v1", "m2", "v2")
	exec(t, "DEL", "b")

	fresh, applied := reload(t, a)
	if applied == 0 {
		t.Fatal("no commands were replayed; nothing was persisted")
	}

	if got := fresh.Get("a"); got == nil || got.StringValue() != "1" {
		t.Errorf("a did not survive: %#v", got)
	}
	if fresh.Exists("b") {
		t.Error("b survived although it was deleted before the reload")
	}
	if got := fresh.Get("counter"); got == nil || got.StringValue() != "1" {
		t.Errorf("counter did not survive: %#v", got)
	}
	if got := fresh.Get("s"); got == nil || got.StringValue() != "xy" {
		t.Errorf("APPEND was not persisted: %#v", got)
	}
	if got := fresh.Get("m2"); got == nil || got.StringValue() != "v2" {
		t.Errorf("MSET was not persisted: %#v", got)
	}
}

// TestLoadAOFHonoursItsStoreArgument is the regression test for a signature that
// lied: LoadAOF accepted a *Store and never referenced it, applying every
// replayed command to the package-level KV instead. It worked in main only
// because main happened to pass core.KV.
func TestLoadAOFHonoursItsStoreArgument(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)
	exec(t, "SET", "replayed", "v")
	closeAndFlush(t, a)

	// A sentinel store that must remain untouched.
	untouched := NewStore(4, 1000)
	withStore(t, untouched)

	target := NewStore(4, 1000)
	if _, err := LoadAOF(a.path, target); err != nil {
		t.Fatalf("LoadAOF: %v", err)
	}
	if !target.Exists("replayed") {
		t.Error("the store passed to LoadAOF did not receive the replayed key")
	}
	if untouched.Exists("replayed") {
		t.Error("LoadAOF wrote into the package-level KV instead of the store it was given")
	}
	if KV != untouched {
		t.Error("LoadAOF did not restore the package-level KV")
	}
}

// TestReadsAreNotPropagated checks the Write flag on the dispatch table is
// actually consulted. A log full of GETs would be harmless but would grow the
// file for no reason and make replay slower on every restart.
func TestReadsAreNotPropagated(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	exec(t, "SET", "k", "v")
	for i := 0; i < 100; i++ {
		exec(t, "GET", "k")
		exec(t, "EXISTS", "k")
		exec(t, "TTL", "k")
		exec(t, "STRLEN", "k")
		exec(t, "DBSIZE")
	}
	closeAndFlush(t, a)

	if n := a.Snapshot().Written; n != 1 {
		t.Fatalf("%d records written, want 1 (only the SET)", n)
	}
}

// TestFailedWritesAreNotPropagated matters because a log entry for a command
// that errored would be applied on replay, so replay would diverge from the
// state the server actually had.
func TestFailedWritesAreNotPropagated(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	exec(t, "SET", "text", "abc")       // succeeds: 1 record
	exec(t, "INCR", "text")             // fails: not an integer
	exec(t, "SET", "k", "v", "EX", "0") // fails: invalid expire
	exec(t, "RENAME", "absent", "x")    // fails: no such key
	exec(t, "MSET", "a", "1", "b")      // fails: odd argument count
	closeAndFlush(t, a)

	if n := a.Snapshot().Written; n != 1 {
		t.Fatalf("%d records written, want 1: a command that returned an error "+
			"was still logged, so replay would not reproduce the real state", n)
	}
}

// TestValuesWithSpacesAndBinaryRoundTrip is the regression test for the quoting
// bug. `SET k "hello world"` used to be written as the line `SET k hello world`
// and reloaded as a four-token command, so the value became "hello".
func TestValuesWithSpacesAndBinaryRoundTrip(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	cases := map[string]string{
		"plain":          "value",
		"with space":     "hello world",
		"with crlf":      "line1\r\nline2",
		"with lf":        "line1\nline2",
		"with tab":       "a\tb",
		"with quotes":    `he said "hi" and 'bye'`,
		"binary":         string([]byte{0x00, 0x01, 0xfe, 0xff}),
		"empty":          "",
		"resp-lookalike": "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\ny\r\n",
		"large":          strings.Repeat("q", 200_000),
	}
	for k, v := range cases {
		exec(t, "SET", k, v)
	}

	fresh, _ := reload(t, a)
	for k, want := range cases {
		obj := fresh.Get(k)
		if obj == nil {
			t.Errorf("key %q was lost", k)
			continue
		}
		if got := obj.StringValue(); got != want {
			t.Errorf("key %q: value did not round-trip\n got %q\nwant %q",
				k, truncate(got), truncate(want))
		}
	}
	// The RESP-lookalike value is the sharpest case: a length-prefixed format
	// reads it back as one argument, while any delimiter-scanning parser would
	// interpret its contents as further commands and invent extra keys.
	if fresh.Len() != len(cases) {
		t.Errorf("keyspace has %d keys after reload, want %d: a value's contents "+
			"were parsed as commands", fresh.Len(), len(cases))
	}
}

// TestKeysWithSpacesRoundTrip is the same bug on the key side, which is worse:
// the value ends up under a different key entirely.
func TestKeysWithSpacesRoundTrip(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	key := "user profile:john smith"
	exec(t, "SET", key, "data")

	fresh, _ := reload(t, a)
	if obj := fresh.Get(key); obj == nil || obj.StringValue() != "data" {
		t.Fatalf("key %q was not restored: %#v; the space split it into two arguments",
			key, obj)
	}
}

func TestTTLSurvivesAReload(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	exec(t, "SET", "ttl-set", "v", "EX", "1000")
	exec(t, "SET", "no-ttl", "v")
	exec(t, "SET", "expire-cmd", "v")
	exec(t, "EXPIRE", "expire-cmd", "1000")

	fresh, _ := reload(t, a)

	for _, k := range []string{"ttl-set", "expire-cmd"} {
		obj := fresh.Get(k)
		if obj == nil {
			t.Fatalf("%s was lost", k)
		}
		if obj.ExpiresAt == NoExpiry {
			t.Errorf("%s lost its TTL across the reload", k)
		}
	}
	if obj := fresh.Get("no-ttl"); obj == nil || obj.ExpiresAt != NoExpiry {
		t.Errorf("a key with no TTL gained one across the reload: %#v", obj)
	}
}

// TestRelativeExpiryIsStoredAsAnAbsoluteDeadline is the subtle one.
//
// If the log contains `SET k v EX 10`, every replay restarts the ten-second
// clock, so a key with a short TTL is resurrected with a full TTL on every
// restart and can outlive its deadline indefinitely. The propagated form must be
// PXAT/PEXPIREAT with an absolute millisecond timestamp.
func TestRelativeExpiryIsStoredAsAnAbsoluteDeadline(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	exec(t, "SET", "k", "v", "EX", "100")
	exec(t, "SET", "k2", "v")
	exec(t, "PEXPIRE", "k2", "100000")
	closeAndFlush(t, a)

	data, err := os.ReadFile(a.path)
	if err != nil {
		t.Fatalf("reading the log: %v", err)
	}
	log := string(data)

	if strings.Contains(log, "$2\r\nEX\r\n") {
		t.Errorf("the log still contains a relative EX:\n%s", log)
	}
	if !strings.Contains(log, "$4\r\nPXAT\r\n") {
		t.Errorf("SET ... EX was not rewritten to PXAT:\n%s", log)
	}
	if !strings.Contains(log, "$9\r\nPEXPIREAT\r\n") {
		t.Errorf("PEXPIRE was not rewritten to PEXPIREAT:\n%s", log)
	}
	if strings.Contains(log, "$7\r\nPEXPIRE\r\n") {
		t.Errorf("the log contains a raw relative PEXPIRE:\n%s", log)
	}
}

// TestExpireWithNonPositiveTTLIsLoggedAsDEL pins the other half of the same
// translation: EXPIRE k 0 deletes the key, and recording a past timestamp would
// leave replay to interpret it.
func TestExpireWithNonPositiveTTLIsLoggedAsDEL(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	exec(t, "SET", "k", "v")
	exec(t, "EXPIRE", "k", "0")
	closeAndFlush(t, a)

	data, _ := os.ReadFile(a.path)
	if !strings.Contains(string(data), "$3\r\nDEL\r\n") {
		t.Fatalf("EXPIRE k 0 was not logged as a DEL:\n%s", data)
	}

	fresh := NewStore(8, 100_000)
	if _, err := LoadAOF(a.path, fresh); err != nil {
		t.Fatalf("LoadAOF: %v", err)
	}
	if fresh.Exists("k") {
		t.Fatal("the key came back after a replay although EXPIRE 0 deleted it")
	}
}

// TestAlreadyExpiredKeysDoNotComeBack is the end-to-end version: a key whose
// deadline passed while the server was down must not be resurrected by replay.
func TestAlreadyExpiredKeysDoNotComeBack(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	exec(t, "SET", "doomed", "v", "PX", "50")
	exec(t, "SET", "survivor", "v")
	closeAndFlush(t, a)

	time.Sleep(120 * time.Millisecond)

	fresh := NewStore(8, 100_000)
	if _, err := LoadAOF(a.path, fresh); err != nil {
		t.Fatalf("LoadAOF: %v", err)
	}
	if fresh.Get("doomed") != nil {
		t.Error("a key whose absolute deadline had passed was resurrected by replay")
	}
	if fresh.Get("survivor") == nil {
		t.Error("a key with no TTL was lost")
	}
}

// TestReplayDoesNotRePropagate guards against a log that doubles in size on
// every restart: replay must apply commands without feeding them back into the
// log it is reading from.
func TestReplayDoesNotRePropagate(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	for i := 0; i < 10; i++ {
		exec(t, "SET", "k"+formatInt64(int64(i)), "v")
	}
	closeAndFlush(t, a)

	before := fileSize(t, a.path)

	// Reopen the same file, exactly as a restart would, then replay it.
	reopened, err := OpenAOF(a.path)
	if err != nil {
		t.Fatalf("OpenAOF: %v", err)
	}
	prev := DefaultAOF
	DefaultAOF = reopened

	fresh := NewStore(8, 100_000)
	applied, loadErr := LoadAOF(a.path, fresh)
	reopened.Close()
	DefaultAOF = prev

	if loadErr != nil {
		t.Fatalf("LoadAOF: %v", loadErr)
	}
	if applied != 10 {
		t.Fatalf("applied %d commands, want 10", applied)
	}
	if n := reopened.Snapshot().Written; n != 0 {
		t.Fatalf("replay wrote %d records back into the log; the file would double "+
			"in size on every restart", n)
	}
	if after := fileSize(t, a.path); after != before {
		t.Fatalf("the log grew from %d to %d bytes during replay", before, after)
	}
}

// TestLoadToleratesATruncatedTail covers a process killed mid-write. Redis
// behaves the same way (aof-load-truncated yes): everything before the
// truncation is applied, and the incomplete record is discarded.
func TestLoadToleratesATruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.aof")

	whole := string(EncodeStringArray([]string{"SET", "a", "1"})) +
		string(EncodeStringArray([]string{"SET", "b", "2"}))
	partial := string(EncodeStringArray([]string{"SET", "c", "3"}))

	// Truncate the third record at every possible point.
	for cut := 1; cut < len(partial); cut++ {
		content := whole + partial[:cut]
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		fresh := NewStore(4, 1000)
		applied, err := LoadAOF(path, fresh)
		if err != nil {
			t.Fatalf("cut at %d: LoadAOF failed: %v", cut, err)
		}
		if applied != 2 {
			t.Errorf("cut at %d: applied %d commands, want 2", cut, applied)
		}
		if !fresh.Exists("a") || !fresh.Exists("b") {
			t.Errorf("cut at %d: a complete record before the truncation was lost", cut)
		}
	}
}

func TestLoadMissingFileIsNotAnError(t *testing.T) {
	fresh := NewStore(4, 1000)
	n, err := LoadAOF(filepath.Join(t.TempDir(), "does-not-exist.aof"), fresh)
	if err != nil {
		t.Fatalf("a first start with no AOF must not fail: %v", err)
	}
	if n != 0 {
		t.Fatalf("applied %d commands from a missing file", n)
	}
}

func TestLoadEmptyFileIsNotAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.aof")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	fresh := NewStore(4, 1000)
	if _, err := LoadAOF(path, fresh); err != nil {
		t.Fatalf("an empty AOF must not fail: %v", err)
	}
}

// TestLoadSkipsUnknownAndNonWriteCommands keeps an older log loadable: a command
// that has since been renamed or removed is skipped rather than aborting the
// whole startup.
func TestLoadSkipsUnknownAndNonWriteCommands(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mixed.aof")
	content := string(EncodeStringArray([]string{"SET", "a", "1"})) +
		string(EncodeStringArray([]string{"NOSUCHCOMMAND", "x"})) +
		string(EncodeStringArray([]string{"GET", "a"})) +
		string(EncodeStringArray([]string{"SET"})) + // bad arity
		string(EncodeStringArray([]string{"SET", "b", "2"}))
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	fresh := NewStore(4, 1000)
	applied, err := LoadAOF(path, fresh)
	if err != nil {
		t.Fatalf("LoadAOF: %v", err)
	}
	if applied != 2 {
		t.Fatalf("applied %d commands, want 2 (the two valid SETs)", applied)
	}
	if !fresh.Exists("a") || !fresh.Exists("b") {
		t.Fatal("a valid command after an invalid one was skipped")
	}
}

// TestRewriteCompactsTheLog is the reason a rewrite exists at all: without it,
// a key written a million times occupies a million records for one value.
func TestRewriteCompactsTheLog(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	for i := 0; i < 5_000; i++ {
		exec(t, "SET", "hot", formatInt64(int64(i)))
	}
	exec(t, "SET", "other", "v")

	waitForAOFWrites(t, a, 5_001)
	closeAndFlush(t, a)
	before := fileSize(t, a.path)

	if err := a.Rewrite(KV); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	after := fileSize(t, a.path)

	if after >= before {
		t.Fatalf("the log did not shrink: %d -> %d bytes", before, after)
	}
	if got := a.Snapshot().LastRewriteKeys; got != 2 {
		t.Errorf("rewrite reported %d keys, want 2", got)
	}

	// The compacted log must still reproduce the final state.
	fresh := NewStore(8, 100_000)
	if _, err := LoadAOF(a.path, fresh); err != nil {
		t.Fatalf("LoadAOF after rewrite: %v", err)
	}
	if obj := fresh.Get("hot"); obj == nil || obj.StringValue() != "4999" {
		t.Fatalf("the rewritten log lost the final value: %#v", obj)
	}
	if !fresh.Exists("other") {
		t.Fatal("the rewritten log lost a key")
	}
}

func TestRewritePreservesTTLsAsAbsolute(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	exec(t, "SET", "with-ttl", "v", "EX", "1000")
	exec(t, "SET", "without", "v")
	if err := a.Rewrite(KV); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}

	data, _ := os.ReadFile(a.path)
	if !strings.Contains(string(data), "$4\r\nPXAT\r\n") {
		t.Fatalf("the rewritten log has no PXAT, so the TTL is lost:\n%s", data)
	}

	fresh := NewStore(8, 100_000)
	if _, err := LoadAOF(a.path, fresh); err != nil {
		t.Fatalf("LoadAOF: %v", err)
	}
	if obj := fresh.Get("with-ttl"); obj == nil || obj.ExpiresAt == NoExpiry {
		t.Errorf("TTL was not restored from the rewritten log: %#v", obj)
	}
	if obj := fresh.Get("without"); obj == nil || obj.ExpiresAt != NoExpiry {
		t.Errorf("a key with no TTL gained one: %#v", obj)
	}
}

// TestRewriteSkipsExpiredKeys stops a rewrite from making dead keys durable
// again -- the snapshot may contain keys the sweep has not reached yet.
func TestRewriteSkipsExpiredKeys(t *testing.T) {
	store := NewStore(8, 100_000)
	withStore(t, store)
	a := newTestAOF(t)

	store.Put("dead", NewStringObj("v", time.Now().UnixMilli()-1))
	store.Put("live", NewStringObj("v", NoExpiry))

	if err := a.Rewrite(store); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if got := a.Snapshot().LastRewriteKeys; got != 1 {
		t.Fatalf("rewrite wrote %d keys, want 1 (the expired key must be skipped)", got)
	}
}

// TestRewriteIsAtomic is the regression test for the O_TRUNC rewrite. The
// original truncated the live file and then wrote into it, so a crash between
// those two steps left an empty file and destroyed the entire dataset.
//
// Atomicity is verified structurally: the rewrite must build a temp file and
// rename it, so no temp file is left behind on success and the live path is
// never observed empty.
func TestRewriteIsAtomic(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	for i := 0; i < 200; i++ {
		exec(t, "SET", "k"+formatInt64(int64(i)), strings.Repeat("v", 500))
	}
	waitForAOFWrites(t, a, 200)
	closeAndFlush(t, a)

	dir := filepath.Dir(a.path)
	if err := a.Rewrite(KV); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("a temp file was left behind after a successful rewrite: %s", e.Name())
		}
	}
	if fileSize(t, a.path) == 0 {
		t.Fatal("the live log is empty after a rewrite")
	}

	fresh := NewStore(8, 100_000)
	if _, err := LoadAOF(a.path, fresh); err != nil {
		t.Fatalf("LoadAOF: %v", err)
	}
	if fresh.Len() != 200 {
		t.Fatalf("the rewritten log restored %d keys, want 200", fresh.Len())
	}
}

// TestConcurrentRewritesDoNotCorrupt covers two BGREWRITEAOF calls racing. Both
// build the same temp path, so without rewriteMu they interleave and the renamed
// file contains a mixture of two partial dumps.
func TestConcurrentRewritesDoNotCorrupt(t *testing.T) {
	store := NewStore(8, 100_000)
	withStore(t, store)
	a := newTestAOF(t)

	for i := 0; i < 500; i++ {
		store.Put("k"+formatInt64(int64(i)), NewStringObj(strings.Repeat("v", 100), NoExpiry))
	}

	done := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() { done <- a.Rewrite(store) }()
	}
	for i := 0; i < 4; i++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent rewrite failed: %v", err)
		}
	}

	fresh := NewStore(8, 100_000)
	if _, err := LoadAOF(a.path, fresh); err != nil {
		t.Fatalf("the log is unparseable after concurrent rewrites: %v", err)
	}
	if fresh.Len() != 500 {
		t.Fatalf("restored %d keys, want 500", fresh.Len())
	}
}

// TestPropagateAfterCloseDoesNotPanic covers the shutdown race: a command still
// in flight when Close runs would otherwise send on a closed channel and panic
// the process during what should be a clean exit.
func TestPropagateAfterCloseDoesNotPanic(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)
	a.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Propagate after Close panicked: %v", r)
		}
	}()
	for i := 0; i < 100; i++ {
		a.Propagate(&RedisCmd{Cmd: "SET", Args: []string{"k", "v"}})
	}
	// A second Close must also be safe: main's defer and an explicit shutdown
	// path can both reach it.
	a.Close()
}

func TestPropagateOnNilAOFIsSafe(t *testing.T) {
	var a *AOF
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a nil AOF panicked: %v", r)
		}
	}()
	a.Propagate(&RedisCmd{Cmd: "SET", Args: []string{"k", "v"}})
	a.Close()
	if err := a.Rewrite(NewStore(1, 10)); err == nil {
		t.Error("Rewrite on a nil AOF returned no error")
	}
}

// TestPropagateDropsRatherThanBlocking documents the backpressure decision: when
// the queue is full the record is dropped and counted, because blocking here
// would stall the reactor -- and therefore every client on it -- behind one slow
// disk. INFO reports aof_dropped so the loss is visible rather than silent.
func TestPropagateDropsRatherThanBlocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "small.aof")
	// Deliberately no writer goroutine, so the queue can only fill.
	a := &AOF{path: path, ch: make(chan []byte, 4), done: make(chan struct{})}

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			a.Propagate(&RedisCmd{Cmd: "SET", Args: []string{"k", "v"}})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Propagate blocked on a full queue; one slow disk would stall every client")
	}

	s := a.Snapshot()
	if s.Dropped != 96 {
		t.Errorf("dropped %d of 100 with a 4-slot queue, want 96", s.Dropped)
	}
}

// TestFsyncAlwaysSyncsEveryRecord checks the policy is actually applied rather
// than being a config value nothing reads.
func TestFsyncAlwaysSyncsEveryRecord(t *testing.T) {
	prev := config.AOFFsync
	config.AOFFsync = config.FsyncAlways
	t.Cleanup(func() { config.AOFFsync = prev })

	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	for i := 0; i < 20; i++ {
		exec(t, "SET", "k", formatInt64(int64(i)))
		// Let the writer drain between records: if all twenty are still queued
		// when Close runs they are flushed as one batch with a single fsync,
		// which would make this test pass or fail on timing rather than policy.
		waitForAOFWrites(t, a, int64(i+1))
	}

	if n := a.Snapshot().Fsyncs; n < 20 {
		t.Fatalf("%d fsyncs for 20 writes under appendfsync=always, want at least 20", n)
	}
}

func TestAOFStatsAreReported(t *testing.T) {
	withStore(t, NewStore(8, 100_000))
	a := newTestAOF(t)

	exec(t, "SET", "k", "value")
	waitForAOFWrites(t, a, 1)

	s := a.Snapshot()
	if s.Written != 1 {
		t.Errorf("Written = %d, want 1", s.Written)
	}
	if s.Bytes == 0 {
		t.Error("Bytes = 0 although a record was written")
	}

	// INFO must surface them, since aof_dropped is the only way a silent
	// durability loss becomes visible.
	info := exec(t, "INFO")
	for _, field := range []string{
		"aof_commands_written", "aof_bytes_written", "aof_dropped", "aof_fsyncs",
	} {
		if !strings.Contains(info, field) {
			t.Errorf("INFO does not report %s", field)
		}
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fi.Size()
}
