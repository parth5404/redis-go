package server

import (
	"bytes"
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github/redis.go/core"
)

// executeCommand runs one command through the very same evaluator the RESP TCP
// server uses and then decodes the reply off the wire.
//
// Sharing the evaluator is the whole point of the bridge: a key written by a
// model goes through identical argument validation, TTL handling, eviction,
// keyspace accounting and AOF persistence as one written by redis-cli. There
// is no second code path to keep in sync.
func executeCommand(cmd string, args ...string) (string, error) {
	redisCmd := &core.RedisCmd{Cmd: cmd, Args: args}
	cmds := core.RedisCmds{redisCmd}

	buf := bytes.NewBuffer(nil)
	if err := core.EvalAndRespond(&cmds, buf); err != nil {
		return "", err
	}
	if buf.Len() == 0 {
		return "OK", nil
	}

	res, _, err := core.DecodeOne(buf.Bytes())
	if err != nil {
		return "", err
	}

	// A "-ERR ..." frame decodes to a Go error value. Without this check the
	// message would be formatted into the success payload and the model would
	// read a rejected command as though it had worked — silently acting on a
	// key that was never written.
	if respErr, ok := res.(error); ok {
		return "", respErr
	}
	// A null bulk string decodes to nil, which is a miss rather than a failure.
	if res == nil {
		return "(nil)", nil
	}
	return fmt.Sprintf("%v", res), nil
}

// mcpArg is one tool parameter, forwarded positionally to the RESP command.
type mcpArg struct {
	name     string
	desc     string
	required bool
}

// mcpTool declares a tool purely in terms of the command it runs, so the tool
// surface stays auditable against the dispatch table in core.
type mcpTool struct {
	name        string
	description string
	cmd         string
	args        []mcpArg
}

var mcpTools = []mcpTool{
	{
		name:        "redis_set",
		description: "Store a string value at a key. Overwrites any existing value.",
		cmd:         "SET",
		args: []mcpArg{
			{name: "key", desc: "The key to set", required: true},
			{name: "value", desc: "The value to store", required: true},
		},
	},
	{
		name:        "redis_get",
		description: "Read the string value stored at a key. Returns (nil) if the key does not exist.",
		cmd:         "GET",
		args:        []mcpArg{{name: "key", desc: "The key to retrieve", required: true}},
	},
	{
		name:        "redis_del",
		description: "Delete a key. Returns the number of keys removed, so 0 means it was not there.",
		cmd:         "DEL",
		args:        []mcpArg{{name: "key", desc: "The key to delete", required: true}},
	},
	{
		name:        "redis_exists",
		description: "Check whether a key exists. Returns 1 or 0.",
		cmd:         "EXISTS",
		args:        []mcpArg{{name: "key", desc: "The key to test", required: true}},
	},
	{
		name:        "redis_expire",
		description: "Attach a time-to-live to an existing key. Returns 1 if the TTL was set, 0 if the key does not exist.",
		cmd:         "EXPIRE",
		args: []mcpArg{
			{name: "key", desc: "The key to expire", required: true},
			{name: "seconds", desc: "Lifetime in seconds", required: true},
		},
	},
	{
		name:        "redis_ttl",
		description: "Remaining time-to-live of a key in seconds. -1 means the key has no expiry, -2 means it does not exist.",
		cmd:         "TTL",
		args:        []mcpArg{{name: "key", desc: "The key to inspect", required: true}},
	},
	{
		name:        "redis_type",
		description: "Report the stored type of a key, or none if it does not exist.",
		cmd:         "TYPE",
		args:        []mcpArg{{name: "key", desc: "The key to inspect", required: true}},
	},
	{
		name:        "redis_incr",
		description: "Atomically increment the integer stored at a key by one, creating it at 0 first if absent.",
		cmd:         "INCR",
		args:        []mcpArg{{name: "key", desc: "The key to increment", required: true}},
	},
}

// MCPToolNames lists the tools exposed over stdio.
func MCPToolNames() []string {
	names := make([]string, 0, len(mcpTools))
	for _, t := range mcpTools {
		names = append(names, t.name)
	}
	return names
}

func stringArg(req mcp.CallToolRequest, name string) (string, bool) {
	args, ok := req.Params.Arguments.(map[string]interface{})
	if !ok {
		return "", false
	}
	v, ok := args[name].(string)
	return v, ok
}

func StartMCPServer() error {
	s := server.NewMCPServer(
		"redis-go",
		"1.0.0",
		server.WithToolCapabilities(true),
	)

	for _, t := range mcpTools {
		opts := []mcp.ToolOption{mcp.WithDescription(t.description)}
		for _, a := range t.args {
			if a.required {
				opts = append(opts, mcp.WithString(a.name, mcp.Required(), mcp.Description(a.desc)))
			} else {
				opts = append(opts, mcp.WithString(a.name, mcp.Description(a.desc)))
			}
		}

		s.AddTool(mcp.NewTool(t.name, opts...), toolHandler(t))
	}

	return server.ServeStdio(s)
}

// toolHandler builds the closure for one tool. It is a named function so each
// registration captures its own copy of t.
func toolHandler(t mcpTool) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		cmdArgs := make([]string, 0, len(t.args))
		for _, a := range t.args {
			v, ok := stringArg(req, a.name)
			if !ok {
				if a.required {
					return mcp.NewToolResultError("missing required argument '" + a.name + "'"), nil
				}
				continue
			}
			cmdArgs = append(cmdArgs, v)
		}

		res, err := executeCommand(t.cmd, cmdArgs...)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(res), nil
	}
}
