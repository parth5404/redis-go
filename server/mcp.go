package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github/redis.go/core"
)

// The MCP (Model Context Protocol) bridge exposes the keyspace as tools an LLM
// can call over stdio.
//
// It deliberately reuses core.Execute rather than reaching into the store: the
// same arity checks, the same AOF propagation and the same statistics apply, so
// a key written by an LLM is indistinguishable from one written over RESP and
// survives a restart the same way.

// executeCommand runs one command and renders its RESP reply as text.
//
// The original wrote into a bytes.Buffer through EvalAndRespond and then decoded
// it back, which is two extra copies and loses the error/ok distinction --
// fmt.Sprintf("%v") on a decoded error value produces the message with no
// indication that it *was* an error, so a WRONGTYPE looked like a successful
// result to the model. Here the reply is inspected directly and errors are
// returned as errors.
func executeCommand(cmd string, args ...string) (string, error) {
	reply := core.Execute(&core.RedisCmd{Cmd: cmd, Args: args})

	// Detect the error frame from the wire format rather than from the decoded
	// value: RESP simple strings and errors both decode to a Go string, so only
	// the leading byte distinguishes them. Without this check a WRONGTYPE was
	// handed to the model as though it were a successful result.
	if len(reply) > 0 && reply[0] == '-' {
		msg, _, err := core.DecodeOne(reply)
		if err != nil {
			return "", fmt.Errorf("%s failed", cmd)
		}
		return "", fmt.Errorf("%v", msg)
	}

	value, _, err := core.DecodeOne(reply)
	if err != nil {
		return "", fmt.Errorf("could not decode reply for %s: %w", cmd, err)
	}

	switch v := value.(type) {
	case nil:
		// RESP null. Distinct from an empty string: the key does not exist.
		return "(nil)", nil
	case string:
		return v, nil
	case int64:
		return fmt.Sprint(v), nil
	case []interface{}:
		parts := make([]string, 0, len(v))
		for _, e := range v {
			if e == nil {
				parts = append(parts, "(nil)")
				continue
			}
			parts = append(parts, fmt.Sprint(e))
		}
		return strings.Join(parts, "\n"), nil
	default:
		return fmt.Sprint(v), nil
	}
}

// stringArg pulls a required string argument out of a tool call.
func stringArg(req mcp.CallToolRequest, name string) (string, bool) {
	args, ok := req.Params.Arguments.(map[string]interface{})
	if !ok {
		return "", false
	}
	s, ok := args[name].(string)
	if !ok || s == "" {
		return "", false
	}
	return s, true
}

// optionalStringArg pulls an optional string argument, returning "" if absent.
func optionalStringArg(req mcp.CallToolRequest, name string) string {
	args, ok := req.Params.Arguments.(map[string]interface{})
	if !ok {
		return ""
	}
	switch v := args[name].(type) {
	case string:
		return v
	case float64:
		// JSON numbers decode to float64. Render integral values without the
		// ".000000" that %v would produce, since these become command arguments.
		if v == float64(int64(v)) {
			return fmt.Sprint(int64(v))
		}
		return fmt.Sprint(v)
	default:
		return ""
	}
}

// mcpTool describes one exposed command, so the registration loop below stays
// free of the copy-pasted argument-extraction block the original repeated for
// every tool -- which is why two of its four tools ended up commented out
// rather than fixed.
type mcpTool struct {
	name        string
	description string
	// keyDesc documents the required "key" argument.
	keyDesc string
	// extra names optional additional string arguments, in command order.
	extra []mcpArg
	// build turns the extracted arguments into a command.
	build func(key string, extra []string) (string, []string)
}

type mcpArg struct {
	name     string
	desc     string
	required bool
}

// StartMCPServer serves the MCP tool surface on stdio and blocks.
func StartMCPServer() error {
	s := mcpserver.NewMCPServer(
		"redis-go",
		"1.0.0",
		mcpserver.WithToolCapabilities(true),
	)

	tools := []mcpTool{
		{
			name:        "redis_set",
			description: "Store a string value at a key, optionally with a TTL in seconds.",
			keyDesc:     "The key to set",
			extra: []mcpArg{
				{name: "value", desc: "The value to store", required: true},
				{name: "ttl_seconds", desc: "Optional time-to-live in seconds", required: false},
			},
			build: func(key string, extra []string) (string, []string) {
				args := []string{key, extra[0]}
				if extra[1] != "" {
					args = append(args, "EX", extra[1])
				}
				return "SET", args
			},
		},
		{
			name:        "redis_get",
			description: "Read the string value stored at a key. Returns (nil) if absent.",
			keyDesc:     "The key to retrieve",
			build:       func(key string, _ []string) (string, []string) { return "GET", []string{key} },
		},
		{
			name:        "redis_del",
			description: "Delete a key. Returns the number of keys removed (0 or 1).",
			keyDesc:     "The key to delete",
			build:       func(key string, _ []string) (string, []string) { return "DEL", []string{key} },
		},
		{
			name:        "redis_incr",
			description: "Atomically increment the integer stored at a key by one, creating it at 0 first if absent.",
			keyDesc:     "The key to increment",
			build:       func(key string, _ []string) (string, []string) { return "INCR", []string{key} },
		},
		{
			name:        "redis_exists",
			description: "Check whether a key exists. Returns 1 or 0.",
			keyDesc:     "The key to test",
			build:       func(key string, _ []string) (string, []string) { return "EXISTS", []string{key} },
		},
		{
			name:        "redis_ttl",
			description: "Remaining time-to-live of a key in seconds. -1 means no expiry, -2 means the key is gone.",
			keyDesc:     "The key to inspect",
			build:       func(key string, _ []string) (string, []string) { return "TTL", []string{key} },
		},
		{
			name:        "redis_keys",
			description: "List keys matching a glob pattern such as user:*. Use * for all keys.",
			keyDesc:     "The glob pattern to match",
			build:       func(key string, _ []string) (string, []string) { return "KEYS", []string{key} },
		},
	}

	for _, t := range tools {
		t := t

		opts := []mcp.ToolOption{
			mcp.WithDescription(t.description),
			mcp.WithString("key", mcp.Required(), mcp.Description(t.keyDesc)),
		}
		for _, a := range t.extra {
			if a.required {
				opts = append(opts, mcp.WithString(a.name, mcp.Required(), mcp.Description(a.desc)))
			} else {
				opts = append(opts, mcp.WithString(a.name, mcp.Description(a.desc)))
			}
		}

		s.AddTool(mcp.NewTool(t.name, opts...),
			func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				key, ok := stringArg(req, "key")
				if !ok {
					return mcp.NewToolResultError("missing required argument 'key'"), nil
				}
				extra := make([]string, len(t.extra))
				for i, a := range t.extra {
					extra[i] = optionalStringArg(req, a.name)
					if a.required && extra[i] == "" {
						return mcp.NewToolResultError("missing required argument '" + a.name + "'"), nil
					}
				}

				cmd, args := t.build(key, extra)
				res, err := executeCommand(cmd, args...)
				if err != nil {
					return mcp.NewToolResultError(err.Error()), nil
				}
				return mcp.NewToolResultText(res), nil
			})
	}

	// Tool: redis_info exposes server statistics with no key argument, so it is
	// registered outside the table.
	s.AddTool(mcp.NewTool("redis_info",
		mcp.WithDescription("Server statistics: key count, hit rate, memory, persistence and connection counters."),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		res, err := executeCommand("INFO")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(res), nil
	})

	return mcpserver.ServeStdio(s)
}
