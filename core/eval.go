package core

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

var RESP_NIL []byte = []byte("$-1\r\n")

var RESP_OK []byte = []byte("+OK\r\n")

func wrongArgs(cmd string) []byte {
	return Encode(fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd), false)
}

func evalPING(args []string) []byte {
	if len(args) > 1 {
		return wrongArgs("ping")
	}
	if len(args) == 1 {
		return Encode(args[0], false)
	}
	return Encode("PONG", true)
}

func evalSET(args []string) []byte {
	if len(args) < 2 {
		return wrongArgs("set")
	}

	key, value := args[0], args[1]
	var expMs int64 = -1
	oType, eType := deduceTypeEncoding(value)

	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "EX":
			i++
			if i == len(args) {
				return Encode(errors.New("ERR syntax error"), false)
			}
			exDurSec, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil {
				return Encode(errors.New("ERR value is not an integer or out of range"), false)
			}
			if exDurSec <= 0 {
				return Encode(errors.New("ERR invalid expire time in 'set' command"), false)
			}
			expMs = exDurSec * 1000
		default:
			return Encode(errors.New("ERR syntax error"), false)
		}
	}

	Put(key, NewObj(value, expMs, oType, eType))
	return RESP_OK
}

func evalGET(args []string) []byte {
	if len(args) != 1 {
		return wrongArgs("get")
	}
	obj := Get(args[0])
	if obj == nil {
		return RESP_NIL
	}
	return Encode(obj.Value, false)
}

func evalDEL(args []string) []byte {
	if len(args) < 1 {
		return wrongArgs("del")
	}
	var cnt int64
	for _, key := range args {
		if Del(key) {
			cnt++
		}
	}
	return Encode(cnt, false)
}

func evalEXISTS(args []string) []byte {
	if len(args) < 1 {
		return wrongArgs("exists")
	}
	var cnt int64
	for _, key := range args {
		if Get(key) != nil {
			cnt++
		}
	}
	return Encode(cnt, false)
}

func evalEXPIRE(args []string) []byte {
	if len(args) != 2 {
		return wrongArgs("expire")
	}
	seconds, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return Encode(errors.New("ERR value is not an integer or out of range"), false)
	}
	if !Expire(args[0], seconds*1000) {
		return Encode(int64(0), false)
	}
	return Encode(int64(1), false)
}

func evalTTL(args []string) []byte {
	if len(args) != 1 {
		return wrongArgs("ttl")
	}
	obj := Get(args[0])
	if obj == nil {
		return Encode(int64(-2), false)
	}
	if obj.ExpiresAt == -1 {
		return Encode(int64(-1), false)
	}
	leftMs := obj.ExpiresAt - time.Now().UnixMilli()
	if leftMs < 0 {
		return Encode(int64(-2), false)
	}
	return Encode(leftMs/1000, false)
}

// evalTYPE reads the type straight back out of the packed type/encoding byte
// carried on every object.
func evalTYPE(args []string) []byte {
	if len(args) != 1 {
		return wrongArgs("type")
	}
	obj := Get(args[0])
	if obj == nil {
		return Encode("none", true)
	}
	return Encode(typeName(obj.TypeEncoding), true)
}

func evalINCR(args []string) []byte { return incrDecr(args, "incr", 1) }

func evalDECR(args []string) []byte { return incrDecr(args, "decr", -1) }

func incrDecr(args []string, name string, delta int64) []byte {
	if len(args) != 1 {
		return wrongArgs(name)
	}
	val, err := IncrBy(args[0], delta)
	if err != nil {
		return Encode(err, false)
	}
	return Encode(val, false)
}

func evalCOMMAND(args []string) []byte {
	if len(args) == 0 {
		return Encode(CommandNames(), false)
	}
	// redis-cli probes "COMMAND DOCS" on connect. Acknowledge subcommands
	// rather than pretending to implement the full introspection payload.
	return RESP_OK
}

// evalBGREWRITEAOF hands the snapshot to a background goroutine so a large
// keyspace does not block the event loop mid-dump.
func evalBGREWRITEAOF(args []string) []byte {
	go DumpAlLAof()
	return []byte("+Background append only file rewriting started\r\n")
}

func EvalAndRespond(cmds *RedisCmds, conn io.ReadWriter) error {
	if cmds == nil {
		return nil
	}
	for _, cmd := range *cmds {
		if cmd == nil {
			continue
		}

		var res []byte
		if handler, ok := commands[strings.ToUpper(cmd.Cmd)]; ok {
			res = handler(cmd.Args)
		} else {
			res = Encode(fmt.Errorf("ERR unknown command '%s'", cmd.Cmd), false)
		}

		if res == nil {
			continue
		}
		if _, err := conn.Write(res); err != nil {
			return err
		}
	}
	return nil
}
