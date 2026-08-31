package core

import (
	"bytes"
	"testing"
	"time"
)

// resetStore gives a test a clean keyspace. The store is a package-level global,
// so tests in this file must not run in parallel with each other.
func resetStore(t *testing.T) {
	t.Helper()
	RWmutex.Lock()
	store = make(map[string]*Obj)
	RWmutex.Unlock()
	KeyspaceStat[0] = make(map[string]int)
}

// run evaluates one command through the real dispatch table and returns the raw
// RESP reply, so assertions are made against actual wire bytes.
func run(t *testing.T, cmd string, args ...string) string {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	cmds := RedisCmds{{Cmd: cmd, Args: args}}
	if err := EvalAndRespond(&cmds, buf); err != nil {
		t.Fatalf("EvalAndRespond(%s %v) returned error: %v", cmd, args, err)
	}
	return buf.String()
}

type call struct {
	cmd  string
	args []string
}

func TestCommands(t *testing.T) {
	tests := []struct {
		name  string
		setup []call
		cmd   string
		args  []string
		want  string
	}{
		{name: "PING", cmd: "PING", want: "+PONG\r\n"},
		{name: "PING with message", cmd: "PING", args: []string{"hello"}, want: "$5\r\nhello\r\n"},
		{name: "PING with too many args", cmd: "PING", args: []string{"a", "b"},
			want: "-ERR wrong number of arguments for 'ping' command\r\n"},

		{name: "SET", cmd: "SET", args: []string{"k", "v"}, want: "+OK\r\n"},
		{name: "SET missing value", cmd: "SET", args: []string{"k"},
			want: "-ERR wrong number of arguments for 'set' command\r\n"},
		{name: "SET with bad option", cmd: "SET", args: []string{"k", "v", "NX"},
			want: "-ERR syntax error\r\n"},
		{name: "SET EX without argument", cmd: "SET", args: []string{"k", "v", "EX"},
			want: "-ERR syntax error\r\n"},
		{name: "SET EX with non-numeric ttl", cmd: "SET", args: []string{"k", "v", "EX", "abc"},
			want: "-ERR value is not an integer or out of range\r\n"},

		{name: "GET existing", setup: []call{{"SET", []string{"k", "v"}}},
			cmd: "GET", args: []string{"k"}, want: "$1\r\nv\r\n"},
		{name: "GET missing", cmd: "GET", args: []string{"nope"}, want: "$-1\r\n"},
		{name: "GET value with spaces", setup: []call{{"SET", []string{"k", "hello world"}}},
			cmd: "GET", args: []string{"k"}, want: "$11\r\nhello world\r\n"},
		{name: "GET no args", cmd: "GET", want: "-ERR wrong number of arguments for 'get' command\r\n"},

		{name: "DEL existing", setup: []call{{"SET", []string{"k", "v"}}},
			cmd: "DEL", args: []string{"k"}, want: ":1\r\n"},
		{name: "DEL missing", cmd: "DEL", args: []string{"nope"}, want: ":0\r\n"},
		{name: "DEL multiple", setup: []call{{"SET", []string{"a", "1"}}, {"SET", []string{"b", "2"}}},
			cmd: "DEL", args: []string{"a", "b", "c"}, want: ":2\r\n"},

		{name: "EXISTS present", setup: []call{{"SET", []string{"k", "v"}}},
			cmd: "EXISTS", args: []string{"k"}, want: ":1\r\n"},
		{name: "EXISTS absent", cmd: "EXISTS", args: []string{"nope"}, want: ":0\r\n"},
		{name: "EXISTS counts only the present keys",
			setup: []call{{"SET", []string{"a", "1"}}},
			cmd:   "EXISTS", args: []string{"a", "b"}, want: ":1\r\n"},

		{name: "EXPIRE on existing key", setup: []call{{"SET", []string{"k", "v"}}},
			cmd: "EXPIRE", args: []string{"k", "100"}, want: ":1\r\n"},
		{name: "EXPIRE on missing key", cmd: "EXPIRE", args: []string{"nope", "100"}, want: ":0\r\n"},
		{name: "EXPIRE with non-numeric ttl", setup: []call{{"SET", []string{"k", "v"}}},
			cmd: "EXPIRE", args: []string{"k", "soon"},
			want: "-ERR value is not an integer or out of range\r\n"},

		{name: "TTL without expiry", setup: []call{{"SET", []string{"k", "v"}}},
			cmd: "TTL", args: []string{"k"}, want: ":-1\r\n"},
		{name: "TTL on missing key", cmd: "TTL", args: []string{"nope"}, want: ":-2\r\n"},

		{name: "TYPE of a string", setup: []call{{"SET", []string{"k", "v"}}},
			cmd: "TYPE", args: []string{"k"}, want: "+string\r\n"},
		{name: "TYPE of a missing key", cmd: "TYPE", args: []string{"nope"}, want: "+none\r\n"},

		{name: "INCR creates at one", cmd: "INCR", args: []string{"n"}, want: ":1\r\n"},
		{name: "INCR existing", setup: []call{{"SET", []string{"n", "41"}}},
			cmd: "INCR", args: []string{"n"}, want: ":42\r\n"},
		{name: "INCR on a non-integer", setup: []call{{"SET", []string{"k", "hello"}}},
			cmd: "INCR", args: []string{"k"},
			want: "-ERR value is not an integer or out of range\r\n"},
		{name: "INCR wrong arity", cmd: "INCR", args: []string{"a", "b"},
			want: "-ERR wrong number of arguments for 'incr' command\r\n"},

		{name: "DECR creates at minus one", cmd: "DECR", args: []string{"n"}, want: ":-1\r\n"},
		{name: "DECR existing", setup: []call{{"SET", []string{"n", "10"}}},
			cmd: "DECR", args: []string{"n"}, want: ":9\r\n"},

		{name: "unknown command", cmd: "NOPE", want: "-ERR unknown command 'NOPE'\r\n"},
		{name: "command name is case insensitive", cmd: "ping", want: "+PONG\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetStore(t)
			for _, s := range tt.setup {
				run(t, s.cmd, s.args...)
			}
			if got := run(t, tt.cmd, tt.args...); got != tt.want {
				t.Errorf("%s %v = %q, want %q", tt.cmd, tt.args, got, tt.want)
			}
		})
	}
}

// TestCommandTableCoverage keeps the advertised command list and the dispatch
// table from drifting apart.
func TestCommandTableCoverage(t *testing.T) {
	want := []string{
		"BGREWRITE", "COMMAND", "DECR", "DEL", "EXISTS", "EXPIRE",
		"GET", "INCR", "PING", "SET", "TTL", "TYPE",
	}

	got := CommandNames()
	if len(got) != len(want) {
		t.Fatalf("CommandNames() has %d entries %v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("CommandNames()[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// Every advertised name must actually dispatch.
	for _, name := range got {
		if _, ok := commands[name]; !ok {
			t.Errorf("advertised command %q has no handler", name)
		}
	}
}

func TestCOMMANDListsEveryCommand(t *testing.T) {
	resetStore(t)
	reply := run(t, "COMMAND")

	values, _, err := DecodeOne([]byte(reply))
	if err != nil {
		t.Fatalf("COMMAND reply did not decode: %v", err)
	}
	arr, ok := values.([]interface{})
	if !ok {
		t.Fatalf("COMMAND replied %#v, want an array", values)
	}
	if len(arr) != len(CommandNames()) {
		t.Errorf("COMMAND listed %d commands, want %d", len(arr), len(CommandNames()))
	}
}

// TestPipelinedCommands runs several commands through one evaluator call, the
// way the server handles a pipelined read.
func TestPipelinedCommands(t *testing.T) {
	resetStore(t)

	buf := bytes.NewBuffer(nil)
	cmds := RedisCmds{
		{Cmd: "SET", Args: []string{"a", "1"}},
		{Cmd: "INCR", Args: []string{"a"}},
		{Cmd: "GET", Args: []string{"a"}},
	}
	if err := EvalAndRespond(&cmds, buf); err != nil {
		t.Fatalf("EvalAndRespond returned error: %v", err)
	}

	want := "+OK\r\n:2\r\n$1\r\n2\r\n"
	if buf.String() != want {
		t.Errorf("pipeline replied %q, want %q", buf.String(), want)
	}
}

// TestTypeEncodingIsPackedIntoOneByte checks the object header directly: the
// high nibble carries the type and the low nibble the encoding, both inside a
// single uint8.
func TestTypeEncodingIsPackedIntoOneByte(t *testing.T) {
	tests := []struct {
		name         string
		value        string
		wantType     string
		wantEncoding string
	}{
		{name: "integer value", value: "1234", wantType: "string", wantEncoding: "int"},
		{name: "short text", value: "hello", wantType: "string", wantEncoding: "embstr"},
		{name: "text over the embstr limit", value: string(make([]byte, 45)),
			wantType: "string", wantEncoding: "raw"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetStore(t)
			run(t, "SET", "k", tt.value)

			obj := Get("k")
			if obj == nil {
				t.Fatal("key vanished after SET")
			}
			if got := typeName(obj.TypeEncoding); got != tt.wantType {
				t.Errorf("type = %q, want %q", got, tt.wantType)
			}
			if got := encodingName(obj.TypeEncoding); got != tt.wantEncoding {
				t.Errorf("encoding = %q, want %q", got, tt.wantEncoding)
			}
			// The type occupies the high nibble and the encoding the low one,
			// so the two must not bleed into each other.
			if getType(obj.TypeEncoding)|getEncoding(obj.TypeEncoding) != obj.TypeEncoding {
				t.Errorf("type and encoding do not reconstruct the header byte %08b", obj.TypeEncoding)
			}
		})
	}
}

// TestTTLReporting checks the countdown rather than an exact reply, since the
// remaining milliseconds tick down between the SET and the TTL.
func TestTTLReporting(t *testing.T) {
	tests := []struct {
		name  string
		setup []call
		want  int64
	}{
		{
			name:  "after SET EX",
			setup: []call{{"SET", []string{"k", "v", "EX", "100"}}},
			want:  100,
		},
		{
			name:  "after EXPIRE",
			setup: []call{{"SET", []string{"k", "v"}}, {"EXPIRE", []string{"k", "50"}}},
			want:  50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetStore(t)
			for _, s := range tt.setup {
				run(t, s.cmd, s.args...)
			}

			value, _, err := DecodeOne([]byte(run(t, "TTL", "k")))
			if err != nil {
				t.Fatalf("TTL reply did not decode: %v", err)
			}
			left, ok := value.(int64)
			if !ok {
				t.Fatalf("TTL replied %#v, want an integer", value)
			}
			// One second of slack absorbs the truncation in the millisecond
			// to second conversion.
			if left > tt.want || left < tt.want-1 {
				t.Errorf("TTL = %d, want %d or %d", left, tt.want-1, tt.want)
			}
		})
	}
}

func TestSetWithExpirySetsTTL(t *testing.T) {
	resetStore(t)
	run(t, "SET", "k", "v", "EX", "1")

	obj := Get("k")
	if obj == nil {
		t.Fatal("key missing right after SET")
	}
	if obj.ExpiresAt == -1 {
		t.Fatal("SET EX did not record an expiry")
	}

	left := obj.ExpiresAt - time.Now().UnixMilli()
	if left <= 0 || left > 1000 {
		t.Errorf("expiry is %d ms away, want between 0 and 1000", left)
	}
}
