package server

import (
	"bytes"
	"context"
	"fmt"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github/redis.go/core"
)

// executeCommand runs a Redis command by going through the existing core evaluator
func executeCommand(cmd string, args ...string) (string, error) {
	redisCmd := &core.RedisCmd{
		Cmd:  cmd,
		Args: args,
	}
	cmds := core.RedisCmds{redisCmd}
	buf := bytes.NewBuffer(nil)

	err := core.EvalAndRespond(&cmds, buf)
	if err != nil {
		return "", err
	}

	// Decode the RESP output back to a Go interface
	if buf.Len() == 0 {
		return "OK", nil
	}
	res, _, err := core.DecodeOne(buf.Bytes())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", res), nil
}

func StartMCPServer() error {
	s := server.NewMCPServer(
		"redis-clone",
		"1.0.0",
		server.WithToolCapabilities(true),
	)

	// Tool: redis_set
	toolSet := mcp.NewTool("redis_set",
		mcp.WithDescription("Set a key-value pair in Redis"),
		mcp.WithString("key", mcp.Required(), mcp.Description("The key to set")),
		mcp.WithString("value", mcp.Required(), mcp.Description("The value to store")),
	)
	s.AddTool(toolSet, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args, ok := request.Params.Arguments.(map[string]interface{})
		if !ok {
			return mcp.NewToolResultError("Invalid arguments format"), nil
		}
		key, ok1 := args["key"].(string)
		value, ok2 := args["value"].(string)
		if !ok1 || !ok2 {
			return mcp.NewToolResultError("Missing key or value"), nil
		}
		res, err := executeCommand("SET", key, value)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(res), nil
	})

	// Tool: redis_get
	toolGet := mcp.NewTool("redis_get",
		mcp.WithDescription("Get a value by key from Redis"),
		mcp.WithString("key", mcp.Required(), mcp.Description("The key to retrieve")),
	)
	s.AddTool(toolGet, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args, ok := request.Params.Arguments.(map[string]interface{})
		if !ok {
			return mcp.NewToolResultError("Invalid arguments format"), nil
		}
		key, ok1 := args["key"].(string)
		if !ok1 {
			return mcp.NewToolResultError("Missing key"), nil
		}
		res, err := executeCommand("GET", key)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(res), nil
	})

	// Tool: redis_del
	toolDel := mcp.NewTool("redis_del",
		mcp.WithDescription("Delete a key from Redis"),
		mcp.WithString("key", mcp.Required(), mcp.Description("The key to delete")),
	)
	s.AddTool(toolDel, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args, ok := request.Params.Arguments.(map[string]interface{})
		if !ok {
			return mcp.NewToolResultError("Invalid arguments format"), nil
		}
		key, ok1 := args["key"].(string)
		if !ok1 {
			return mcp.NewToolResultError("Missing key"), nil
		}
		res, err := executeCommand("DEL", key)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(res), nil
	})

	// Tool: redis_incr
	toolIncr := mcp.NewTool("redis_incr",
		mcp.WithDescription("Increment an integer value by 1"),
		mcp.WithString("key", mcp.Required(), mcp.Description("The key to increment")),
	)
	s.AddTool(toolIncr, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args, ok := request.Params.Arguments.(map[string]interface{})
		if !ok {
			return mcp.NewToolResultError("Invalid arguments format"), nil
		}
		key, ok1 := args["key"].(string)
		if !ok1 {
			return mcp.NewToolResultError("Missing key"), nil
		}
		res, err := executeCommand("INCR", key)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(res), nil
	})

	// Start the stdio server
	return server.ServeStdio(s)
}
