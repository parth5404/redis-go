package core

import (
	"strings"
	"testing"
	"time"
)

// Command behaviour tests.
//
// These go through Execute, the same funnel the TCP server uses, so they cover
// dispatch, arity checking and the reply encoding as well as the handler -- not
// just the handler in isolation.

// exec runs a command and returns its raw RESP reply.
func exec(t *testing.T, cmd string, args ...string) string {
	t.Helper()
	return string(Execute(&RedisCmd{Cmd: cmd, Args: args}))
}

// setupCmdTest gives each test a private keyspace and no AOF, so nothing leaks
// between tests and no test writes to disk.
func setupCmdTest(t *testing.T) {
	t.Helper()
	withStore(t, NewStore(8, 100_000))
	prevAOF := DefaultAOF
	DefaultAOF = nil
	t.Cleanup(func() { DefaultAOF = prevAOF })
}

func TestPingAndEcho(t *testing.T) {
	setupCmdTest(t)

	if got := exec(t, "PING"); got != "+PONG\r\n" {
		t.Errorf("PING = %q, want +PONG", got)
	}
	if got := exec(t, "PING", "hello"); got != "$5\r\nhello\r\n" {
		t.Errorf("PING hello = %q", got)
	}
	if got := exec(t, "ECHO", "hi"); got != "$2\r\nhi\r\n" {
		t.Errorf("ECHO hi = %q", got)
	}
}

// TestCommandNamesAreCaseInsensitive is the regression test for a bug that made
// the server unusable from a telnet prompt and from several client libraries:
// the dispatch compared raw bytes against uppercase literals, so `set` returned
// "unknown command 'set'".
func TestCommandNamesAreCaseInsensitive(t *testing.T) {
	setupCmdTest(t)

	for _, name := range []string{"SET", "set", "Set", "sEt"} {
		got := exec(t, name, "k", "v")
		if got != "+OK\r\n" {
			t.Errorf("%q k v = %q, want +OK", name, got)
		}
	}
	for _, name := range []string{"GET", "get", "Get"} {
		if got := exec(t, name, "k"); got != "$1\r\nv\r\n" {
			t.Errorf("%q k = %q", name, got)
		}
	}
}

func TestUnknownCommand(t *testing.T) {
	setupCmdTest(t)
	got := exec(t, "NOSUCHCOMMAND", "a")
	if !strings.HasPrefix(got, "-ERR unknown command") {
		t.Fatalf("got %q, want an unknown-command error", got)
	}
}

// TestArityIsEnforced covers the centralised arity check. The original checked
// argument counts inside each handler and got several wrong -- evalPing rejected
// only `len(args) > 2`, so `PING a b` was accepted with two arguments.
func TestArityIsEnforced(t *testing.T) {
	setupCmdTest(t)

	cases := []struct {
		cmd  string
		args []string
	}{
		{"GET", nil},
		{"GET", []string{"a", "b"}},
		{"SET", []string{"k"}},
		{"ECHO", nil},
		{"ECHO", []string{"a", "b"}},
		{"INCR", nil},
		{"INCR", []string{"a", "b"}},
		{"EXPIRE", []string{"k"}},
		{"TTL", nil},
		{"RENAME", []string{"a"}},
		{"DEL", nil},
	}
	for _, tc := range cases {
		got := exec(t, tc.cmd, tc.args...)
		if !strings.HasPrefix(got, "-ERR wrong number of arguments") {
			t.Errorf("%s %v = %q, want an arity error", tc.cmd, tc.args, got)
		}
	}
}

func TestSetAndGet(t *testing.T) {
	setupCmdTest(t)

	exec(t, "SET", "k", "v")
	if got := exec(t, "GET", "k"); got != "$1\r\nv\r\n" {
		t.Errorf("GET k = %q", got)
	}
	if got := exec(t, "GET", "absent"); got != "$-1\r\n" {
		t.Errorf("GET absent = %q, want the null bulk string", got)
	}
}

// TestSetPreservesValuesWithSpacesAndNewlines is the behavioural counterpart to
// the AOF quoting fix: a value containing a space must come back whole.
func TestSetPreservesValuesWithSpacesAndNewlines(t *testing.T) {
	setupCmdTest(t)

	values := []string{
		"two words",
		"line1\r\nline2",
		"tab\there",
		"",
		strings.Repeat("x", 10_000),
	}
	for _, v := range values {
		exec(t, "SET", "k", v)
		want := string(EncodeBulkString(v))
		if got := exec(t, "GET", "k"); got != want {
			t.Errorf("value %q did not round-trip: got %q", truncate(v), truncate(got))
		}
	}
}

func TestSetOptions(t *testing.T) {
	setupCmdTest(t)

	t.Run("NX only sets when absent", func(t *testing.T) {
		if got := exec(t, "SET", "nx", "first", "NX"); got != "+OK\r\n" {
			t.Fatalf("first NX = %q", got)
		}
		if got := exec(t, "SET", "nx", "second", "NX"); got != "$-1\r\n" {
			t.Fatalf("second NX = %q, want the null reply", got)
		}
		if got := exec(t, "GET", "nx"); got != "$5\r\nfirst\r\n" {
			t.Fatalf("value changed: %q", got)
		}
	})

	t.Run("XX only sets when present", func(t *testing.T) {
		if got := exec(t, "SET", "xx", "v", "XX"); got != "$-1\r\n" {
			t.Fatalf("XX on an absent key = %q", got)
		}
		exec(t, "SET", "xx", "v")
		if got := exec(t, "SET", "xx", "v2", "XX"); got != "+OK\r\n" {
			t.Fatalf("XX on a live key = %q", got)
		}
	})

	t.Run("EX sets a TTL", func(t *testing.T) {
		exec(t, "SET", "ex", "v", "EX", "100")
		if got := exec(t, "TTL", "ex"); got != ":100\r\n" {
			t.Fatalf("TTL = %q, want :100", got)
		}
	})

	t.Run("PX sets a millisecond TTL", func(t *testing.T) {
		exec(t, "SET", "px", "v", "PX", "100000")
		reply := exec(t, "PTTL", "px")
		if !strings.HasPrefix(reply, ":9") && !strings.HasPrefix(reply, ":100000") {
			t.Fatalf("PTTL = %q, want roughly 100000", reply)
		}
	})

	t.Run("KEEPTTL retains the existing TTL", func(t *testing.T) {
		exec(t, "SET", "keep", "v", "EX", "100")
		exec(t, "SET", "keep", "v2", "KEEPTTL")
		if got := exec(t, "TTL", "keep"); got == ":-1\r\n" {
			t.Fatal("KEEPTTL dropped the TTL")
		}
	})

	t.Run("a plain SET clears the TTL", func(t *testing.T) {
		exec(t, "SET", "clear", "v", "EX", "100")
		exec(t, "SET", "clear", "v2")
		if got := exec(t, "TTL", "clear"); got != ":-1\r\n" {
			t.Fatalf("TTL = %q after a plain overwrite, want :-1", got)
		}
	})

	t.Run("GET returns the previous value", func(t *testing.T) {
		exec(t, "SET", "g", "old")
		if got := exec(t, "SET", "g", "new", "GET"); got != "$3\r\nold\r\n" {
			t.Fatalf("SET ... GET = %q, want the old value", got)
		}
		if got := exec(t, "GET", "g"); got != "$3\r\nnew\r\n" {
			t.Fatalf("the new value was not stored: %q", got)
		}
	})

	t.Run("a non-positive EX is rejected", func(t *testing.T) {
		// Redis rejects these rather than treating them as "delete now", and a
		// client relying on that distinction would otherwise lose data silently.
		for _, v := range []string{"0", "-1"} {
			got := exec(t, "SET", "bad", "v", "EX", v)
			if !strings.HasPrefix(got, "-ERR") {
				t.Errorf("SET bad v EX %s = %q, want an error", v, got)
			}
		}
	})

	t.Run("a non-numeric EX is rejected", func(t *testing.T) {
		if got := exec(t, "SET", "bad", "v", "EX", "abc"); !strings.HasPrefix(got, "-ERR") {
			t.Errorf("got %q, want an error", got)
		}
	})

	t.Run("an unknown option is rejected", func(t *testing.T) {
		if got := exec(t, "SET", "k", "v", "NONSENSE"); !strings.HasPrefix(got, "-ERR") {
			t.Errorf("got %q, want a syntax error", got)
		}
	})
}

func TestSetNX(t *testing.T) {
	setupCmdTest(t)
	if got := exec(t, "SETNX", "k", "a"); got != ":1\r\n" {
		t.Fatalf("first SETNX = %q", got)
	}
	if got := exec(t, "SETNX", "k", "b"); got != ":0\r\n" {
		t.Fatalf("second SETNX = %q", got)
	}
	if got := exec(t, "GET", "k"); got != "$1\r\na\r\n" {
		t.Fatalf("value changed: %q", got)
	}
}

func TestGetSetAndGetDel(t *testing.T) {
	setupCmdTest(t)

	if got := exec(t, "GETSET", "k", "v1"); got != "$-1\r\n" {
		t.Fatalf("GETSET on an absent key = %q", got)
	}
	if got := exec(t, "GETSET", "k", "v2"); got != "$2\r\nv1\r\n" {
		t.Fatalf("GETSET = %q, want the old value", got)
	}
	if got := exec(t, "GETDEL", "k"); got != "$2\r\nv2\r\n" {
		t.Fatalf("GETDEL = %q", got)
	}
	if got := exec(t, "EXISTS", "k"); got != ":0\r\n" {
		t.Fatalf("GETDEL did not delete the key: EXISTS = %q", got)
	}
}

func TestMSetAndMGet(t *testing.T) {
	setupCmdTest(t)

	if got := exec(t, "MSET", "a", "1", "b", "2"); got != "+OK\r\n" {
		t.Fatalf("MSET = %q", got)
	}
	got := exec(t, "MGET", "a", "b", "missing")
	want := "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n"
	if got != want {
		t.Fatalf("MGET = %q, want %q", got, want)
	}

	// An odd argument count is a syntax error, not a partial write.
	if got := exec(t, "MSET", "a", "1", "b"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("MSET with an odd argument count = %q, want an error", got)
	}
}

func TestIncrDecrCommands(t *testing.T) {
	setupCmdTest(t)

	if got := exec(t, "INCR", "c"); got != ":1\r\n" {
		t.Fatalf("INCR on an absent key = %q, want :1", got)
	}
	if got := exec(t, "INCRBY", "c", "9"); got != ":10\r\n" {
		t.Fatalf("INCRBY = %q", got)
	}
	if got := exec(t, "DECR", "c"); got != ":9\r\n" {
		t.Fatalf("DECR = %q", got)
	}
	if got := exec(t, "DECRBY", "c", "9"); got != ":0\r\n" {
		t.Fatalf("DECRBY = %q", got)
	}

	exec(t, "SET", "text", "abc")
	if got := exec(t, "INCR", "text"); got != "-"+ErrNotInteger.Error()+"\r\n" {
		t.Fatalf("INCR on a non-numeric value = %q", got)
	}

	// DECRBY with MinInt64 cannot be negated without overflowing.
	if got := exec(t, "DECRBY", "c", formatInt64(minInt64)); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("DECRBY MinInt64 = %q, want an error", got)
	}
}

func TestIncrPreservesTTL(t *testing.T) {
	setupCmdTest(t)
	exec(t, "SET", "c", "1", "EX", "100")
	exec(t, "INCR", "c")
	if got := exec(t, "TTL", "c"); got == ":-1\r\n" {
		t.Fatal("INCR cleared the key's TTL")
	}
}

func TestAppendAndStrlen(t *testing.T) {
	setupCmdTest(t)

	if got := exec(t, "APPEND", "k", "abc"); got != ":3\r\n" {
		t.Fatalf("APPEND on an absent key = %q", got)
	}
	if got := exec(t, "APPEND", "k", "de"); got != ":5\r\n" {
		t.Fatalf("APPEND = %q", got)
	}
	if got := exec(t, "STRLEN", "k"); got != ":5\r\n" {
		t.Fatalf("STRLEN = %q", got)
	}
	if got := exec(t, "STRLEN", "absent"); got != ":0\r\n" {
		t.Fatalf("STRLEN on an absent key = %q, want :0", got)
	}
}

func TestExpireAndTTL(t *testing.T) {
	setupCmdTest(t)

	// TTL semantics: -2 means the key does not exist, -1 means it has no TTL.
	if got := exec(t, "TTL", "absent"); got != ":-2\r\n" {
		t.Fatalf("TTL on an absent key = %q, want :-2", got)
	}
	exec(t, "SET", "k", "v")
	if got := exec(t, "TTL", "k"); got != ":-1\r\n" {
		t.Fatalf("TTL with no expiry = %q, want :-1", got)
	}

	if got := exec(t, "EXPIRE", "k", "100"); got != ":1\r\n" {
		t.Fatalf("EXPIRE = %q", got)
	}
	if got := exec(t, "TTL", "k"); got != ":100\r\n" {
		t.Fatalf("TTL = %q, want :100", got)
	}
	if got := exec(t, "PERSIST", "k"); got != ":1\r\n" {
		t.Fatalf("PERSIST = %q", got)
	}
	if got := exec(t, "TTL", "k"); got != ":-1\r\n" {
		t.Fatalf("TTL after PERSIST = %q, want :-1", got)
	}
	if got := exec(t, "EXPIRE", "absent", "10"); got != ":0\r\n" {
		t.Fatalf("EXPIRE on an absent key = %q, want :0", got)
	}
}

// TestExpireWithNonPositiveTTLDeletes matches Redis: `EXPIRE k 0` and a negative
// TTL delete the key immediately rather than being rejected.
func TestExpireWithNonPositiveTTLDeletes(t *testing.T) {
	setupCmdTest(t)
	for _, ttl := range []string{"0", "-1"} {
		exec(t, "SET", "k", "v")
		if got := exec(t, "EXPIRE", "k", ttl); got != ":1\r\n" {
			t.Fatalf("EXPIRE k %s = %q, want :1", ttl, got)
		}
		if got := exec(t, "EXISTS", "k"); got != ":0\r\n" {
			t.Fatalf("EXPIRE k %s did not delete the key", ttl)
		}
	}
}

func TestExpireFlags(t *testing.T) {
	setupCmdTest(t)

	exec(t, "SET", "k", "v")
	// NX only sets a TTL when there is none.
	if got := exec(t, "EXPIRE", "k", "100", "NX"); got != ":1\r\n" {
		t.Fatalf("EXPIRE NX on a key with no TTL = %q", got)
	}
	if got := exec(t, "EXPIRE", "k", "200", "NX"); got != ":0\r\n" {
		t.Fatalf("EXPIRE NX on a key with a TTL = %q, want :0", got)
	}
	// GT only raises.
	if got := exec(t, "EXPIRE", "k", "50", "GT"); got != ":0\r\n" {
		t.Fatalf("EXPIRE GT with a lower TTL = %q, want :0", got)
	}
	if got := exec(t, "EXPIRE", "k", "500", "GT"); got != ":1\r\n" {
		t.Fatalf("EXPIRE GT with a higher TTL = %q, want :1", got)
	}
	// LT only lowers.
	if got := exec(t, "EXPIRE", "k", "100", "LT"); got != ":1\r\n" {
		t.Fatalf("EXPIRE LT with a lower TTL = %q, want :1", got)
	}
	// Incompatible combinations are rejected.
	if got := exec(t, "EXPIRE", "k", "100", "NX", "XX"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("EXPIRE NX XX = %q, want an error", got)
	}
}

func TestExpireAtAndPExpireAt(t *testing.T) {
	setupCmdTest(t)

	exec(t, "SET", "k", "v")
	future := time.Now().Unix() + 100
	if got := exec(t, "EXPIREAT", "k", formatInt64(future)); got != ":1\r\n" {
		t.Fatalf("EXPIREAT = %q", got)
	}
	ttl := exec(t, "TTL", "k")
	if ttl == ":-1\r\n" || ttl == ":-2\r\n" {
		t.Fatalf("TTL after EXPIREAT = %q", ttl)
	}

	// A timestamp in the past deletes the key.
	exec(t, "SET", "past", "v")
	if got := exec(t, "EXPIREAT", "past", "1"); got != ":1\r\n" {
		t.Fatalf("EXPIREAT with a past timestamp = %q", got)
	}
	if got := exec(t, "EXISTS", "past"); got != ":0\r\n" {
		t.Fatal("EXPIREAT with a past timestamp did not delete the key")
	}
}

// TestTTLRoundsUp matches Redis: a key with 1500 ms left reports 2 seconds, not
// 1. Truncating instead would make a client's "expires in N seconds" wrong by
// almost a full second.
func TestTTLRoundsUp(t *testing.T) {
	setupCmdTest(t)
	exec(t, "SET", "k", "v", "PX", "1500")
	if got := exec(t, "TTL", "k"); got != ":2\r\n" {
		t.Fatalf("TTL with 1500ms left = %q, want :2", got)
	}
}

func TestTypeAndObject(t *testing.T) {
	setupCmdTest(t)

	if got := exec(t, "TYPE", "absent"); got != "+none\r\n" {
		t.Fatalf("TYPE on an absent key = %q", got)
	}
	exec(t, "SET", "k", "v")
	if got := exec(t, "TYPE", "k"); got != "+string\r\n" {
		t.Fatalf("TYPE = %q", got)
	}

	// OBJECT ENCODING reports the same names Redis does, which is what makes the
	// int/embstr/raw distinction observable.
	exec(t, "SET", "num", "12345")
	if got := exec(t, "OBJECT", "ENCODING", "num"); got != "$3\r\nint\r\n" {
		t.Fatalf("OBJECT ENCODING of an integer = %q, want int", got)
	}
	exec(t, "SET", "short", "hello")
	if got := exec(t, "OBJECT", "ENCODING", "short"); got != "$6\r\nembstr\r\n" {
		t.Fatalf("OBJECT ENCODING of a short string = %q, want embstr", got)
	}
	exec(t, "SET", "long", strings.Repeat("x", 100))
	if got := exec(t, "OBJECT", "ENCODING", "long"); got != "$3\r\nraw\r\n" {
		t.Fatalf("OBJECT ENCODING of a long string = %q, want raw", got)
	}
}

func TestKeysCommand(t *testing.T) {
	setupCmdTest(t)

	exec(t, "MSET", "user:1", "a", "user:2", "b", "session:1", "c")
	reply := exec(t, "KEYS", "user:*")
	if !strings.HasPrefix(reply, "*2\r\n") {
		t.Fatalf("KEYS user:* = %q, want two results", reply)
	}
	if got := exec(t, "KEYS", "nope*"); got != "*0\r\n" {
		t.Fatalf("KEYS nope* = %q, want an empty array", got)
	}
}

func TestRenameCommand(t *testing.T) {
	setupCmdTest(t)

	if got := exec(t, "RENAME", "absent", "x"); got != "-"+ErrNoSuchKey.Error()+"\r\n" {
		t.Fatalf("RENAME on an absent key = %q", got)
	}
	exec(t, "SET", "a", "v")
	if got := exec(t, "RENAME", "a", "b"); got != "+OK\r\n" {
		t.Fatalf("RENAME = %q", got)
	}
	if got := exec(t, "GET", "b"); got != "$1\r\nv\r\n" {
		t.Fatalf("value did not follow the rename: %q", got)
	}
}

func TestDelAndExistsMultiKey(t *testing.T) {
	setupCmdTest(t)

	exec(t, "MSET", "a", "1", "b", "2", "c", "3")
	if got := exec(t, "EXISTS", "a", "b", "missing"); got != ":2\r\n" {
		t.Fatalf("EXISTS a b missing = %q, want :2", got)
	}
	if got := exec(t, "DEL", "a", "b", "missing"); got != ":2\r\n" {
		t.Fatalf("DEL a b missing = %q, want :2", got)
	}
	// Redis counts duplicates once, because the second delete finds nothing.
	exec(t, "SET", "d", "1")
	if got := exec(t, "EXISTS", "d", "d"); got != ":2\r\n" {
		t.Fatalf("EXISTS d d = %q, want :2 (Redis counts each argument)", got)
	}
}

func TestDBSizeAndFlush(t *testing.T) {
	setupCmdTest(t)

	exec(t, "MSET", "a", "1", "b", "2")
	if got := exec(t, "DBSIZE"); got != ":2\r\n" {
		t.Fatalf("DBSIZE = %q", got)
	}
	if got := exec(t, "FLUSHDB"); got != "+OK\r\n" {
		t.Fatalf("FLUSHDB = %q", got)
	}
	if got := exec(t, "DBSIZE"); got != ":0\r\n" {
		t.Fatalf("DBSIZE after FLUSHDB = %q", got)
	}
}

// TestConfigGetAnswersBenchmarkProbes is why redis-benchmark stops printing
// "WARNING: Could not fetch server CONFIG": it probes save and appendonly before
// starting, and an error reply there makes the tool warn on every run.
func TestConfigGetAnswersBenchmarkProbes(t *testing.T) {
	setupCmdTest(t)

	for _, param := range []string{"save", "appendonly", "maxmemory", "maxmemory-policy"} {
		got := exec(t, "CONFIG", "GET", param)
		if strings.HasPrefix(got, "-") {
			t.Errorf("CONFIG GET %s = %q, want a value", param, got)
		}
		if !strings.HasPrefix(got, "*2\r\n") {
			t.Errorf("CONFIG GET %s = %q, want a two-element array", param, got)
		}
	}
}

// TestCommandDocsReturnsAnArray is the equivalent fix for redis-cli, which sends
// COMMAND DOCS on connect and warns if the reply is an error.
func TestCommandDocsReturnsAnArray(t *testing.T) {
	setupCmdTest(t)
	if got := exec(t, "COMMAND", "DOCS"); strings.HasPrefix(got, "-") {
		t.Fatalf("COMMAND DOCS = %q, want an array", got)
	}
	if got := exec(t, "COMMAND", "COUNT"); !strings.HasPrefix(got, ":") {
		t.Fatalf("COMMAND COUNT = %q, want an integer", got)
	}
}

func TestInfoReportsStatistics(t *testing.T) {
	setupCmdTest(t)

	exec(t, "SET", "k", "v")
	exec(t, "GET", "k")
	exec(t, "GET", "missing")

	reply := exec(t, "INFO")
	for _, section := range []string{
		"# Server", "# Clients", "# Memory", "# Stats", "# Persistence", "# Keyspace",
		"redis_version", "keyspace_hits", "keyspace_misses", "io_reactors", "keyspace_shards",
	} {
		if !strings.Contains(reply, section) {
			t.Errorf("INFO is missing %q", section)
		}
	}
}

func TestSelectRejectsNonZeroDatabase(t *testing.T) {
	setupCmdTest(t)

	if got := exec(t, "SELECT", "0"); got != "+OK\r\n" {
		t.Fatalf("SELECT 0 = %q", got)
	}
	// Only one database exists. Accepting SELECT 1 and then ignoring it would
	// silently share the keyspace between what the client thinks are two
	// databases -- a data-corruption bug from the client's point of view.
	if got := exec(t, "SELECT", "1"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("SELECT 1 = %q, want an error", got)
	}
}

// TestExecuteHandlesNilCommand covers the defensive path: the parser can produce
// a nil entry from `*-1\r\n`, and Execute must not dereference it.
func TestExecuteHandlesNilCommand(t *testing.T) {
	setupCmdTest(t)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Execute(nil) panicked: %v", r)
		}
	}()
	if got := string(Execute(nil)); got != "$-1\r\n" {
		t.Fatalf("Execute(nil) = %q", got)
	}
}

// TestParseCommandsToleratesOddInput covers the inputs that used to panic during
// command construction rather than during parsing.
func TestParseCommandsToleratesOddInput(t *testing.T) {
	inputs := []struct {
		name  string
		value []interface{}
	}{
		{"nil element", []interface{}{nil}},
		{"empty array", []interface{}{[]interface{}{}}},
		{"integer argument", []interface{}{[]interface{}{"GET", int64(5)}}},
		{"nil argument", []interface{}{[]interface{}{"GET", nil}}},
	}
	for _, tc := range inputs {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			if _, err := ParseCommands(tc.value); err != nil {
				t.Logf("returned an error (acceptable): %v", err)
			}
		})
	}
}

func TestEveryRegisteredCommandHasAHandler(t *testing.T) {
	for name, meta := range commands {
		if meta.Handler == nil {
			t.Errorf("%s has no handler", name)
		}
		if meta.Name != name {
			t.Errorf("table key %q does not match Name %q", name, meta.Name)
		}
		if meta.Arity == 0 {
			t.Errorf("%s has arity 0, which can never be satisfied "+
				"(Redis's convention counts the command name)", name)
		}
		if meta.Summary == "" {
			t.Errorf("%s has no summary, so COMMAND DOCS cannot describe it", name)
		}
	}
}

// TestNoCommandPanicsOnPlausibleInput is a coarse robustness sweep: every
// command is called with a handful of argument shapes and must return a reply
// rather than crash. It is deliberately not asserting semantics -- the point is
// that no input reaches an unchecked index or type assertion.
func TestNoCommandPanicsOnPlausibleInput(t *testing.T) {
	setupCmdTest(t)

	argSets := [][]string{
		{},
		{"k"},
		{"k", "v"},
		{"k", "v", "EX"},
		{"k", "0"},
		{"k", "-1"},
		{"k", "notanumber"},
		{"k", "99999999999999999999999"},
		{"", ""},
		{"k", "v", "NONSENSE", "MORE"},
	}

	for name := range commands {
		// FLUSHDB/FLUSHALL would empty the keyspace mid-sweep, which is not a
		// crash but makes the rest of the sweep meaningless.
		if name == "FLUSHDB" || name == "FLUSHALL" {
			continue
		}
		for _, args := range argSets {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("%s %v panicked: %v", name, args, r)
					}
				}()
				reply := Execute(&RedisCmd{Cmd: name, Args: args})
				if len(reply) == 0 {
					t.Errorf("%s %v returned an empty reply; the client would hang "+
						"waiting for one", name, args)
				}
			}()
		}
	}
}

func truncate(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}
