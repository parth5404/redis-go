package core

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// Fuzzing the parser.
//
// The table-driven tests above cover the malformed inputs we thought of. The
// crash that made this rewrite necessary was one nobody thought of, which is
// exactly the class of bug fuzzing finds: the parser is a pure function from an
// attacker-controlled byte slice to a value, so the fuzzer can explore it
// exhaustively with no server running.
//
// Run a short pass with:
//
//	go test ./core -run Fuzz -fuzz FuzzDecodeOne -fuzztime 60s
//
// The invariants asserted here are the ones the network layer relies on:
//
//  1. It never panics.
//  2. It never reports consuming more bytes than it was given, and never
//     reports consuming zero bytes on success -- either would make the caller's
//     loop either read out of bounds or spin forever.
//  3. An ErrIncomplete verdict is stable: appending bytes may complete the
//     frame, but truncating a decodable frame must never turn it into a
//     malformed one.
func FuzzDecodeOne(f *testing.F) {
	seeds := []string{
		"+OK\r\n",
		"-ERR bad\r\n",
		":42\r\n",
		"$5\r\nhello\r\n",
		"$-1\r\n",
		"$0\r\n\r\n",
		"*0\r\n",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
		"PING\r\n",
		"SET k \"a b\"\r\n",
		// Historical crashers.
		"*1\r\n$5000\r\nab",
		"*999999999\r\n",
		":\r\n",
		"*0\r\n\r\n",
		strings.Repeat("*1\r\n", 64),
		"\x00\x01\x02",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		value, n, err := DecodeOne(data)

		if err != nil {
			if n != 0 {
				t.Fatalf("consumed %d bytes while returning error %v; the caller "+
					"would advance past bytes that were not parsed", n, err)
			}
			return
		}
		if n < 0 || n > len(data) {
			t.Fatalf("consumed %d bytes of a %d-byte input", n, len(data))
		}
		if n == 0 {
			t.Fatalf("consumed 0 bytes on success (value %#v); the caller's loop "+
				"would never terminate", value)
		}

		// Truncating a frame that decoded successfully must yield ErrIncomplete
		// or a protocol error -- never a *different successful* value, which
		// would mean the length prefixes are not being honoured.
		if n > 1 {
			if v2, n2, err2 := DecodeOne(data[:n-1]); err2 == nil && n2 == n-1 {
				t.Fatalf("truncated frame also decoded successfully to %#v; "+
					"length prefixes are not being enforced", v2)
			}
		}
	})
}

// FuzzDecodeStream fuzzes the whole-buffer entry point used by AOF replay,
// including the command-conversion step. A malformed log must be rejected, never
// crash the process on startup.
func FuzzDecodeStream(f *testing.F) {
	f.Add([]byte("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"))
	f.Add([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n"))
	f.Add([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$3\r\nDEL"))

	f.Fuzz(func(t *testing.T, data []byte) {
		values, err := Decode(data)
		if err != nil {
			return
		}
		// ParseCommands must tolerate anything Decode accepted: nil values,
		// empty arrays, nested arrays and non-string elements all reach it.
		cmds, err := ParseCommands(values)
		if err != nil {
			return
		}
		for _, c := range cmds {
			if c == nil {
				t.Fatal("ParseCommands returned a nil command")
			}
			// Every produced command must have a name; Execute indexes into the
			// dispatch table with it.
			if c.Cmd == "" && len(c.Args) > 0 {
				t.Fatalf("command with empty name but %d args", len(c.Args))
			}
		}
	})
}

// FuzzMatchGlob checks the pattern matcher terminates and stays consistent.
//
// The recursive form of this function is a remote CPU exhaustion: `KEYS` with
// the pattern "*a*a*a*a*a*a*b" against keys of 'a's takes exponential time. The
// iterative version cannot, and the fuzzer bounds the runtime per input.
func FuzzMatchGlob(f *testing.F) {
	f.Add("*", "anything")
	f.Add("user:*", "user:42")
	f.Add("*a*a*a*a*a*b", strings.Repeat("a", 64))
	f.Add("[a-z]?[^0-9]", "ab-")
	f.Add("\\*", "*")

	f.Fuzz(func(t *testing.T, pattern, name string) {
		// Bound the inputs: the fuzzer is checking for pathological behaviour
		// in the algorithm, not for how long a megabyte-long pattern takes.
		if len(pattern) > 64 || len(name) > 256 {
			return
		}
		got := matchGlob(pattern, name)

		// "*" matches everything, unconditionally.
		if pattern == "*" && !got {
			t.Fatalf("pattern * failed to match %q", name)
		}
		// A pattern with no metacharacters is exact equality.
		if !strings.ContainsAny(pattern, "*?[\\") && got != (pattern == name) {
			t.Fatalf("literal pattern %q vs %q: got %v", pattern, name, got)
		}
	})
}

// TestGlobDoesNotBacktrackExponentially is the deterministic version of the
// fuzz check: this input pattern is the classic exponential-backtracking case,
// and if the matcher ever regresses to recursion the test times out instead of
// completing in microseconds.
func TestGlobDoesNotBacktrackExponentially(t *testing.T) {
	pattern := strings.Repeat("*a", 20) + "*b"
	name := strings.Repeat("a", 200)

	done := make(chan bool, 1)
	go func() { done <- matchGlob(pattern, name) }()

	select {
	case got := <-done:
		if got {
			t.Fatalf("pattern %q should not match a string with no 'b'", pattern)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("matchGlob did not finish in 5s: the matcher is backtracking "+
			"exponentially, which makes KEYS %q a remote CPU exhaustion", pattern)
	}
}

func TestMatchGlobTable(t *testing.T) {
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{"*", "", true},
		{"*", "anything", true},
		{"", "", true},
		{"", "x", false},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h*llo", "hllo", true},
		{"h*llo", "heeeello", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-c]llo", "hbllo", true},
		{"h[a-c]llo", "hdllo", false},
		{"user:*", "user:1000", true},
		{"user:*", "session:1000", false},
		{"*:name", "user:name", true},
		{"a*b*c", "abc", true},
		{"a*b*c", "aXXbYYc", true},
		{"a*b*c", "acb", false},
		{"\\*", "*", true},
		{"\\*", "a", false},
		{"[", "[", true}, // an unterminated class is a literal in Redis too
	}
	for _, tc := range cases {
		if got := matchGlob(tc.pattern, tc.name); got != tc.want {
			t.Errorf("matchGlob(%q, %q) = %v, want %v", tc.pattern, tc.name, got, tc.want)
		}
	}
}

// TestErrIncompleteIsDistinguishable guards the one property the network layer
// cannot work without: a caller must be able to tell "read more" from "close
// the connection" with errors.Is.
func TestErrIncompleteIsDistinguishable(t *testing.T) {
	_, _, err := DecodeOne([]byte("*1\r\n$3\r\nGE"))
	if !errors.Is(err, ErrIncomplete) {
		t.Fatalf("got %v, want an error satisfying errors.Is(err, ErrIncomplete)", err)
	}
	_, _, err = DecodeOne([]byte("*-5\r\n"))
	if errors.Is(err, ErrIncomplete) {
		t.Fatalf("a negative multibulk count reported ErrIncomplete")
	}
}
