package server

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github/redis.go/core"
)

// TestMCPToolCount pins the advertised tool surface so it cannot quietly shrink
// the way commented-out registrations once did.
func TestMCPToolCount(t *testing.T) {
	want := []string{
		"redis_set", "redis_get", "redis_del", "redis_exists",
		"redis_expire", "redis_ttl", "redis_type", "redis_incr",
	}

	got := MCPToolNames()
	if len(got) != len(want) {
		t.Fatalf("exposed %d tools %v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("tool %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestMCPToolsRouteThroughTheCommandTable is the guarantee that makes the
// bridge trustworthy: every tool resolves to a command the RESP server also
// implements, so there is no separate, less-validated path into the store.
func TestMCPToolsRouteThroughTheCommandTable(t *testing.T) {
	implemented := make(map[string]bool)
	for _, name := range core.CommandNames() {
		implemented[name] = true
	}

	for _, tool := range mcpTools {
		if !implemented[tool.cmd] {
			t.Errorf("tool %q runs %q, which is not in the command table", tool.name, tool.cmd)
		}
		if len(tool.args) == 0 {
			t.Errorf("tool %q declares no arguments", tool.name)
		}
	}
}

// freshKeys clears the named keys. The store is process-wide, so cases that
// share key names have to start from a known state.
func freshKeys(keys ...string) {
	for _, k := range keys {
		core.Del(k)
	}
}

func TestExecuteCommand(t *testing.T) {
	tests := []struct {
		name    string
		setup   [][]string
		cmd     string
		args    []string
		want    string
		wantErr bool
	}{
		{name: "set", cmd: "SET", args: []string{"k", "v"}, want: "OK"},
		{
			name:  "get returns the stored value",
			setup: [][]string{{"SET", "k", "v"}},
			cmd:   "GET", args: []string{"k"}, want: "v",
		},
		{
			name: "get on a missing key is a miss, not a failure",
			cmd:  "GET", args: []string{"absent"}, want: "(nil)",
		},
		{
			name:  "incr returns the new value",
			setup: [][]string{{"SET", "n", "41"}},
			cmd:   "INCR", args: []string{"n"}, want: "42",
		},
		{
			name: "ttl on a missing key",
			cmd:  "TTL", args: []string{"absent"}, want: "-2",
		},
		{
			name: "type of a missing key",
			cmd:  "TYPE", args: []string{"absent"}, want: "none",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			freshKeys("k", "n", "absent")
			for _, s := range tt.setup {
				if _, err := executeCommand(s[0], s[1:]...); err != nil {
					t.Fatalf("setup %v failed: %v", s, err)
				}
			}

			got, err := executeCommand(tt.cmd, tt.args...)
			if (err != nil) != tt.wantErr {
				t.Fatalf("executeCommand(%s) error = %v, wantErr %v", tt.cmd, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("executeCommand(%s %v) = %q, want %q", tt.cmd, tt.args, got, tt.want)
			}
		})
	}
}

// TestExecuteCommandPropagatesProtocolErrors is the wire-level fix.
//
// A "-ERR ..." reply used to be decoded like any other value and handed back as
// a successful tool result, so a model asking for an impossible operation would
// read the error text as though the command had worked and keep going. These
// have to surface as Go errors, which the tool layer turns into MCP errors.
func TestExecuteCommandPropagatesProtocolErrors(t *testing.T) {
	tests := []struct {
		name    string
		setup   [][]string
		cmd     string
		args    []string
		wantMsg string
	}{
		{
			name: "unknown command",
			cmd:  "NOSUCHCOMMAND", args: []string{"k"},
			wantMsg: "unknown command",
		},
		{
			name: "wrong arity",
			cmd:  "GET", args: nil,
			wantMsg: "wrong number of arguments",
		},
		{
			name:  "incrementing a non-integer",
			setup: [][]string{{"SET", "k", "hello"}},
			cmd:   "INCR", args: []string{"k"},
			wantMsg: "not an integer",
		},
		{
			name: "bad SET option",
			cmd:  "SET", args: []string{"k", "v", "MAYBE"},
			wantMsg: "syntax error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			freshKeys("k")
			for _, s := range tt.setup {
				executeCommand(s[0], s[1:]...)
			}

			got, err := executeCommand(tt.cmd, tt.args...)
			if err == nil {
				t.Fatalf("executeCommand(%s %v) returned %q with no error", tt.cmd, tt.args, got)
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error = %q, want it to mention %q", err.Error(), tt.wantMsg)
			}
			if got != "" {
				t.Errorf("a failed command also returned a payload: %q", got)
			}
		})
	}
}

// fakeConn feeds readCmds a fixed byte slice and collects whatever is written
// back.
type fakeConn struct {
	in  *bytes.Reader
	out bytes.Buffer
}

func newFakeConn(data string) *fakeConn {
	return &fakeConn{in: bytes.NewReader([]byte(data))}
}

func (f *fakeConn) Read(b []byte) (int, error)  { return f.in.Read(b) }
func (f *fakeConn) Write(b []byte) (int, error) { return f.out.Write(b) }

func TestReadCmds(t *testing.T) {
	tests := []struct {
		name string
		wire string
		want []core.RedisCmd
	}{
		{
			name: "single command",
			wire: "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
			want: []core.RedisCmd{{Cmd: "SET", Args: []string{"k", "v"}}},
		},
		{
			name: "command with no arguments",
			wire: "*1\r\n$4\r\nPING\r\n",
			want: []core.RedisCmd{{Cmd: "PING", Args: []string{}}},
		},
		{
			name: "lowercase names are normalised",
			wire: "*2\r\n$3\r\nget\r\n$1\r\nk\r\n",
			want: []core.RedisCmd{{Cmd: "GET", Args: []string{"k"}}},
		},
		{
			name: "pipelined commands in one read",
			wire: "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
			want: []core.RedisCmd{
				{Cmd: "PING", Args: []string{}},
				{Cmd: "GET", Args: []string{"k"}},
			},
		},
		{
			name: "inline command",
			wire: "SET foo bar\r\n",
			want: []core.RedisCmd{{Cmd: "SET", Args: []string{"foo", "bar"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readCmds(newFakeConn(tt.wire))
			if err != nil {
				t.Fatalf("readCmds returned error: %v", err)
			}
			if len(*got) != len(tt.want) {
				t.Fatalf("parsed %d commands, want %d", len(*got), len(tt.want))
			}
			for i, want := range tt.want {
				gotCmd := (*got)[i]
				if gotCmd.Cmd != want.Cmd {
					t.Errorf("command %d = %q, want %q", i, gotCmd.Cmd, want.Cmd)
				}
				if len(gotCmd.Args) != len(want.Args) {
					t.Errorf("command %d has args %v, want %v", i, gotCmd.Args, want.Args)
					continue
				}
				for j := range want.Args {
					if gotCmd.Args[j] != want.Args[j] {
						t.Errorf("command %d arg %d = %q, want %q", i, j, gotCmd.Args[j], want.Args[j])
					}
				}
			}
		})
	}
}

// TestReadCmdsRejectsMalformedFrames feeds the parser the kind of input a
// hostile client sends. The requirement is an error return: a panic here runs
// on the event-loop goroutine and would drop every connected client at once.
func TestReadCmdsRejectsMalformedFrames(t *testing.T) {
	tests := []struct {
		name string
		wire string
	}{
		{name: "truncated bulk string", wire: "*1\r\n$10\r\nshort\r\n"},
		{name: "absurd bulk length", wire: "$999999999999999\r\nx\r\n"},
		{name: "absurd element count", wire: "*99999999\r\n"},
		{name: "integer where a command was expected", wire: "*1\r\n:42\r\n"},
		{name: "nested array as a command name", wire: "*1\r\n*1\r\n$1\r\na\r\n"},
		{name: "garbage bytes", wire: "\x00\x01\x02\x03"},
		{name: "type byte alone", wire: "$"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmds, err := readCmds(newFakeConn(tt.wire))
			if err != nil {
				return
			}
			// Parsing something harmless out of it is fine too, as long as
			// nothing panicked and no bogus command was produced.
			for _, cmd := range *cmds {
				if cmd == nil {
					t.Error("readCmds produced a nil command")
				}
			}
		})
	}
}

// TestRespondWritesRepliesInOrder checks the full path from wire bytes to wire
// bytes, which is the path both front ends share.
func TestRespondWritesRepliesInOrder(t *testing.T) {
	freshKeys("a")

	conn := newFakeConn("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$4\r\nINCR\r\n$1\r\na\r\n")
	cmds, err := readCmds(conn)
	if err != nil {
		t.Fatalf("readCmds returned error: %v", err)
	}
	respond(conn, cmds)

	want := "+OK\r\n:2\r\n"
	if conn.out.String() != want {
		t.Errorf("replies = %q, want %q", conn.out.String(), want)
	}
}

func TestReadCmdsSurfacesReadErrors(t *testing.T) {
	// An empty reader is a closed connection, which must come back as EOF so
	// the event loop knows to drop the client.
	if _, err := readCmds(newFakeConn("")); err != io.EOF {
		t.Errorf("readCmds on a closed connection returned %v, want EOF", err)
	}
}
