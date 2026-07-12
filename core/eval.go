package core

import (
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"
)

var RESP_NIL []byte = []byte("$-1\r\n")

func evalPing(args []string) []byte {
	var b []byte
	if len(args) > 2 {
		return Encode(errors.New("ERR wrong number of arguments for 'ping' command"), false)
	}
	if len(args) == 1 {
		b = Encode(args[0], false)
	} else {
		b = Encode("PONG", true)
	}
	fmt.Println(string(b))
	return b
}

func evalSET(args []string) []byte {
	if len(args) < 2 {
		return Encode(errors.New("(error) ERR wrong number of arguments for set command"), false)
	}
	var key, value string
	var expMs int64 = -1
	key, value = args[0], args[1]
	oType, eType := deduceTypeEncoding(value)
	for i := 2; i < len(args); i++ {
		switch args[i] {
		case "EX", "ex":
			i++
			if i == len(args) {
				return Encode(errors.New("(error) ERR invalid syntax"), false)
			}
			exDurSec, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil {
				return Encode(errors.New("(error) ERR value is not an integer or out of range"), false)
			}
			expMs = exDurSec * 1000
		default:
			return Encode(errors.New("(error) ERR synatx error"), false)
		}
	}
	Put(key, NewObj(value, expMs, oType, eType))
	return []byte("+OK\r\n")
}

func evalGET(args []string) []byte {
	if len(args) < 1 {
		return Encode(errors.New("(error) ERR wrong number of arguments for set command"), false)
	}
	var key string
	key = args[0]
	value := Get(key)
	if value == nil {
		return RESP_NIL
	}
	if value.ExpiresAt != -1 && value.ExpiresAt <= time.Now().UnixMilli() {
		return RESP_NIL
	}

	return Encode(value.Value, false)
}

func evalTTL(args []string) []byte {
	if len(args) < 1 {
		return Encode(errors.New("(error) ERR wrong number of arguments for TTL command"), false)
	}
	var key string
	key = args[0]
	value := Get(key)
	if value == nil {
		return []byte(":-2\r\n")
	}

	if value.ExpiresAt == -1 {
		return []byte(":-1\r\n")
	}
	leftMs := value.ExpiresAt - time.Now().UnixMilli()
	if leftMs < 0 {
		return []byte(":-2\r\n")
	}
	return Encode(int64(leftMs/1000), false)
}

func evalDEL(args []string) []byte {
	if len(args) < 1 {
		return Encode(errors.New("(error) ERR wrong number of arguments for DEL command"), false)
	}
	cnt := 0
	for _, key := range args {
		if ok := Del(key); ok {
			cnt++
		}
	}
	return Encode(int64(cnt), false)
}

func evalCommand() []byte {
	return []byte("+OK\r\n")
}
func evalBGREAOF() []byte {
	go func() {
		DumpAlLAof()
	}()
	// if err != nil {
	// 	return Encode(errors.New("(error) ERR AOF file error"), false)
	// }
	return []byte("+OK\r\n")
}

func evalINCR(args []string) []byte {
	log.Println(args)
	if len(args) != 1 {
		return Encode(errors.New("(error) ERR wrong number of arguments for INCR command"), false)
	}
	key := args[0]
	obj := Get(key)
	if obj == nil {
		obj = NewObj("0", -1, OBJ_TYPE_STRING, OBJ_ENCODING_INT)
		Put(key, obj)
	}
	if err := assertType(obj.TypeEncoding, OBJ_TYPE_STRING); err != nil {
		return Encode(err, false)
	}
	if err := assertEncoding(obj.TypeEncoding, OBJ_ENCODING_INT); err != nil {
		return Encode(err, false)
	}
	valStr := obj.Value.(string)
	val, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		return Encode(errors.New("ERR value is not an integer or out of range"), false)
	}
	val++
	obj.Value = strconv.FormatInt(val, 10)
	return Encode(val, false)
}

func EvalAndRespond(cmds *RedisCmds, conn io.ReadWriter) error {
	//log.Println("command", cmd.Cmd)
	for _, cmd := range *cmds {
		var res []byte
		switch cmd.Cmd {
		case "PING":
			res = evalPing(cmd.Args)
		case "SET":
			res = evalSET(cmd.Args)
		case "GET":
			res = evalGET(cmd.Args)
		case "TTL":
			res = evalTTL(cmd.Args)
		case "DEL":
			res = evalDEL(cmd.Args)
		case "INCR":
			res = evalINCR(cmd.Args)
		case "COMMAND":
			res = evalCommand()
		case "BGREWRITE":
			res = evalBGREAOF()
		default:
			res = Encode(fmt.Errorf("ERR unknown command '%s'", cmd.Cmd), false)
		}

		if res != nil {
			conn.Write(res)
		}
	}

	return nil
}
