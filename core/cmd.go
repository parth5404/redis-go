package core

import (
	"strings"
)

// RedisCmd is one parsed command: name plus arguments, excluding the name.
type RedisCmd struct {
	Cmd  string
	Args []string
}

// RedisCmds is a batch of commands parsed from one read, i.e. a pipeline.
type RedisCmds []*RedisCmd

// CmdMeta describes one command in the dispatch table.
type CmdMeta struct {
	// Name is the canonical uppercase name.
	Name string

	// Arity follows Redis's convention: a positive value means "exactly this
	// many tokens including the command name", a negative value means "at
	// least this many". Centralising it here means every handler can assume
	// its argument count is already valid instead of re-checking, which is
	// where the original code drifted (evalPing rejected `len(args) > 2`,
	// allowing `PING a b` through with two arguments).
	Arity int

	// Handler executes the command. args excludes the command name.
	Handler func(args []string) []byte

	// Write marks commands that mutate the keyspace. Only these are
	// propagated to the AOF.
	Write bool

	// AOFRewrite converts the command into the form written to the AOF, and
	// exists to turn *relative* expiry into *absolute*.
	//
	// A log containing `SET k v EX 10` restarts the ten-second clock every
	// time it is replayed, so a key with a TTL would be resurrected with a
	// full TTL on every restart and could live forever. Rewriting it to
	// `SET k v PXAT <deadline>` makes replay reproduce the original deadline.
	// Redis does the same thing, propagating EXPIRE as PEXPIREAT.
	//
	// It returns a whole command, not just arguments, because the translation
	// sometimes changes the name too: EXPIRE becomes PEXPIREAT, and an EXPIRE
	// with a non-positive TTL becomes DEL.
	//
	// nil means the command is deterministic on replay and is propagated
	// verbatim.
	AOFRewrite func(args []string) *RedisCmd

	// Summary is reported by COMMAND DOCS.
	Summary string
}

// commands is the dispatch table, keyed by uppercase name.
//
// A map lookup replaces the original switch statement for three reasons: arity
// can be validated uniformly before dispatch, AOF propagation can key off the
// Write flag instead of a second hand-maintained list of command names, and
// COMMAND COUNT / COMMAND DOCS can be answered from real data.
var commands map[string]*CmdMeta

func init() {
	table := []*CmdMeta{
		// --- connection / server ---
		{Name: "PING", Arity: -1, Handler: evalPING, Summary: "Ping the server"},
		{Name: "ECHO", Arity: 2, Handler: evalECHO, Summary: "Echo the given string"},
		{Name: "COMMAND", Arity: -1, Handler: evalCOMMAND, Summary: "Get command metadata"},
		{Name: "CONFIG", Arity: -2, Handler: evalCONFIG, Summary: "Read configuration parameters"},
		{Name: "INFO", Arity: -1, Handler: evalINFO, Summary: "Server information and statistics"},
		{Name: "DBSIZE", Arity: 1, Handler: evalDBSIZE, Summary: "Number of keys in the database"},
		{Name: "SELECT", Arity: 2, Handler: evalSELECT, Summary: "Change the selected database"},
		{Name: "CLIENT", Arity: -2, Handler: evalCLIENT, Summary: "Client connection management"},
		{Name: "FLUSHDB", Arity: -1, Handler: evalFLUSHDB, Write: true, Summary: "Remove all keys"},
		{Name: "FLUSHALL", Arity: -1, Handler: evalFLUSHDB, Write: true, Summary: "Remove all keys"},
		{Name: "BGREWRITEAOF", Arity: 1, Handler: evalBGREWRITEAOF, Summary: "Rewrite the append-only file"},
		// BGREWRITE is not a real Redis command; kept as an alias because
		// the project's own demo script and docs already used that name.
		{Name: "BGREWRITE", Arity: 1, Handler: evalBGREWRITEAOF, Summary: "Alias of BGREWRITEAOF"},

		// --- keyspace ---
		{Name: "DEL", Arity: -2, Handler: evalDEL, Write: true, Summary: "Delete keys"},
		{Name: "EXISTS", Arity: -2, Handler: evalEXISTS, Summary: "Count how many keys exist"},
		{Name: "EXPIRE", Arity: -3, Handler: evalEXPIRE, Write: true, AOFRewrite: rewriteExpire(1000), Summary: "Set a key's TTL in seconds"},
		{Name: "PEXPIRE", Arity: -3, Handler: evalPEXPIRE, Write: true, AOFRewrite: rewriteExpire(1), Summary: "Set a key's TTL in milliseconds"},
		{Name: "EXPIREAT", Arity: -3, Handler: evalEXPIREAT, Write: true, Summary: "Set a key's expiry to a Unix second timestamp"},
		{Name: "PEXPIREAT", Arity: -3, Handler: evalPEXPIREAT, Write: true, Summary: "Set a key's expiry to a Unix millisecond timestamp"},
		{Name: "TTL", Arity: 2, Handler: evalTTL, Summary: "Remaining TTL in seconds"},
		{Name: "PTTL", Arity: 2, Handler: evalPTTL, Summary: "Remaining TTL in milliseconds"},
		{Name: "PERSIST", Arity: 2, Handler: evalPERSIST, Write: true, Summary: "Remove a key's TTL"},
		{Name: "TYPE", Arity: 2, Handler: evalTYPE, Summary: "The type stored at a key"},
		{Name: "KEYS", Arity: 2, Handler: evalKEYS, Summary: "Find keys matching a pattern"},
		{Name: "RENAME", Arity: 3, Handler: evalRENAME, Write: true, Summary: "Rename a key"},
		{Name: "OBJECT", Arity: -2, Handler: evalOBJECT, Summary: "Inspect internal object encoding"},

		// --- strings ---
		{Name: "SET", Arity: -3, Handler: evalSET, Write: true, AOFRewrite: rewriteSet, Summary: "Set a string value"},
		{Name: "SETNX", Arity: 3, Handler: evalSETNX, Write: true, Summary: "Set if absent"},
		{Name: "GET", Arity: 2, Handler: evalGET, Summary: "Get a string value"},
		{Name: "GETSET", Arity: 3, Handler: evalGETSET, Write: true, Summary: "Set and return the old value"},
		{Name: "GETDEL", Arity: 2, Handler: evalGETDEL, Write: true, Summary: "Get and delete"},
		{Name: "MSET", Arity: -3, Handler: evalMSET, Write: true, Summary: "Set multiple keys"},
		{Name: "MGET", Arity: -2, Handler: evalMGET, Summary: "Get multiple keys"},
		{Name: "INCR", Arity: 2, Handler: evalINCR, Write: true, Summary: "Increment by one"},
		{Name: "DECR", Arity: 2, Handler: evalDECR, Write: true, Summary: "Decrement by one"},
		{Name: "INCRBY", Arity: 3, Handler: evalINCRBY, Write: true, Summary: "Increment by an amount"},
		{Name: "DECRBY", Arity: 3, Handler: evalDECRBY, Write: true, Summary: "Decrement by an amount"},
		{Name: "APPEND", Arity: 3, Handler: evalAPPEND, Write: true, Summary: "Append to a string"},
		{Name: "STRLEN", Arity: 2, Handler: evalSTRLEN, Summary: "Length of a string value"},
	}

	commands = make(map[string]*CmdMeta, len(table))
	for _, c := range table {
		commands[c.Name] = c
	}
}

// LookupCommand resolves a command name case-insensitively.
//
// RESP command names are case-insensitive in real Redis: `set`, `Set` and `SET`
// are the same command. The original code compared the raw bytes against
// uppercase literals, so every lowercase command -- which is what a human types
// at a telnet prompt and what several client libraries send -- was rejected
// with "unknown command 'set'".
func LookupCommand(name string) (*CmdMeta, bool) {
	c, ok := commands[strings.ToUpper(name)]
	return c, ok
}

// CommandCount reports the number of registered commands.
func CommandCount() int { return len(commands) }

// CommandNames returns every registered command name, unordered.
func CommandNames() []string {
	out := make([]string, 0, len(commands))
	for name := range commands {
		out = append(out, name)
	}
	return out
}

// arityOK validates a token count (command name included) against meta.
func arityOK(meta *CmdMeta, tokens int) bool {
	if meta.Arity >= 0 {
		return tokens == meta.Arity
	}
	return tokens >= -meta.Arity
}
