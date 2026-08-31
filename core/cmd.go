package core

import "sort"

type RedisCmd struct {
	Cmd  string
	Args []string
}

type RedisCmds []*RedisCmd

// commandHandler evaluates one command's arguments and returns an encoded RESP
// reply.
type commandHandler func(args []string) []byte

// commands is the single dispatch table behind both front ends. The RESP TCP
// server and the MCP stdio server resolve names through this same map, so a
// key written by a model gets byte-for-byte the same validation, expiry,
// eviction and persistence handling as one written by redis-cli.
//
// It is filled in init rather than at declaration because evalCOMMAND reports
// the table's own contents, which the compiler would otherwise flag as an
// initialization cycle.
var commands map[string]commandHandler

func init() {
	commands = map[string]commandHandler{
		"PING":      evalPING,
		"SET":       evalSET,
		"GET":       evalGET,
		"DEL":       evalDEL,
		"EXISTS":    evalEXISTS,
		"EXPIRE":    evalEXPIRE,
		"TTL":       evalTTL,
		"TYPE":      evalTYPE,
		"INCR":      evalINCR,
		"DECR":      evalDECR,
		"COMMAND":   evalCOMMAND,
		"BGREWRITE": evalBGREWRITEAOF,
	}
}

// CommandNames lists the supported commands in sorted order.
func CommandNames() []string {
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
