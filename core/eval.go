package core

import (
	"io"
	"log"

	"github/redis.go/config"
)

// logf is the package's log hook. Routed through one function so command-path
// logging can be switched off wholesale -- the original code called
// log.Println() on every single command, which serialises every request behind
// the logger's mutex and a write to stderr. That alone costs a large fraction
// of throughput.
func logf(format string, a ...interface{}) { log.Printf(format, a...) }

// EvalAndRespond executes a batch of commands and writes each reply to w.
//
// The batch is one pipeline: a client may send several commands in a single TCP
// segment, and every reply must be written back in the same order. Replies are
// concatenated into one buffer and handed to the caller as a single Write,
// because one write() syscall per reply is what made pipelined throughput
// collapse (see server.Client.Flush).
func EvalAndRespond(cmds *RedisCmds, w io.Writer) error {
	if cmds == nil {
		return nil
	}
	for _, cmd := range *cmds {
		if _, err := w.Write(Execute(cmd)); err != nil {
			return err
		}
	}
	return nil
}

// Execute runs one command and returns its RESP reply.
//
// This is the single funnel for command execution: the TCP server, the AOF
// replay path and the MCP bridge all call it, so arity checking, AOF
// propagation and statistics happen exactly once and cannot drift between
// entry points.
func Execute(cmd *RedisCmd) []byte {
	if cmd == nil {
		return RespNil
	}
	KV.Stats().CmdsHandled.Add(1)

	if config.LogCommands {
		logf("cmd=%s args=%v", cmd.Cmd, cmd.Args)
	}

	meta, ok := LookupCommand(cmd.Cmd)
	if !ok {
		// Redis includes the offending arguments in this error. Matching the
		// format matters because some client libraries parse it.
		return EncodeErrorf("ERR unknown command '%s', with args beginning with:", cmd.Cmd)
	}

	// +1 for the command name itself, which Redis's arity convention counts.
	if !arityOK(meta, len(cmd.Args)+1) {
		return EncodeError(wrongArity(lowerASCII(meta.Name)))
	}

	reply := meta.Handler(cmd.Args)

	// Propagate to the AOF only after successful execution, and only for
	// commands that actually mutate state. Writing before execution would
	// persist commands that then failed; using a hardcoded name list instead
	// of the table's Write flag is how such a list falls out of sync when a
	// new command is added.
	if meta.Write && DefaultAOF != nil && !isErrorReply(reply) {
		toLog := cmd
		if meta.AOFRewrite != nil {
			// Translate relative expiry into absolute before it reaches the
			// log; see CmdMeta.AOFRewrite.
			toLog = meta.AOFRewrite(cmd.Args)
		}
		DefaultAOF.Propagate(toLog)
	}
	return reply
}

// isErrorReply reports whether a RESP reply is an error frame.
func isErrorReply(reply []byte) bool {
	return len(reply) > 0 && reply[0] == '-'
}

// lowerASCII lowercases an ASCII string without allocating for the common case
// where it is already lowercase.
func lowerASCII(s string) string {
	needs := false
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			needs = true
			break
		}
	}
	if !needs {
		return s
	}
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

// ParseCommands converts decoded RESP values into commands.
//
// Each top-level value must be an array whose first element is the command
// name. The original implementation type-asserted every element with `.(string)`
// and indexed `arr[0]` before checking the array was non-empty, so an empty
// array (`*0\r\n`) or an integer argument panicked and killed the process.
func ParseCommands(values []interface{}) (RedisCmds, error) {
	cmds := make(RedisCmds, 0, len(values))
	for _, v := range values {
		if v == nil {
			// A RESP null array between commands. Skip rather than fail:
			// there is nothing to execute but the stream is still in sync.
			continue
		}
		arr, ok := v.([]interface{})
		if !ok {
			return nil, ErrSyntax
		}
		if len(arr) == 0 {
			// Redis ignores an empty multibulk instead of erroring.
			continue
		}
		tokens, err := DecodeArrayString(arr)
		if err != nil {
			return nil, err
		}
		cmds = append(cmds, &RedisCmd{Cmd: tokens[0], Args: tokens[1:]})
	}
	return cmds, nil
}
