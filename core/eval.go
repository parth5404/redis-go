package core

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
)

var RESP_NIL []byte = []byte("$-1\r\n")

func evalPing(cmd *RedisCmd, conn io.ReadWriter) error {
	var b []byte
	if len(cmd.Args) > 2 {
		return errors.New("ERR wrong number of arguments for 'ping' command")
	}
	if len(cmd.Args) == 1 {
		b = Encode(cmd.Args[0], false)
	} else {
		b = Encode("PONG", true)
	}
	fmt.Println(string(b))
	_, err := conn.Write(b)
	return err
}

func evalSET(cmd *RedisCmd, conn io.ReadWriter) error {
	if len(cmd.Args) < 2 {
		return errors.New("(error) ERR wrong number of arguments for set command")
	}
	var key, value string
	var expMs int64 = -1
	key, value = cmd.Args[0], cmd.Args[1]
	for i := 2; i < len(cmd.Args); i++ {
		switch cmd.Args[i] {
		case "EX", "ex":
			i++
			if i == len(cmd.Args) {
				return errors.New("(error) ERR invalid syntax")
			}
			exDurSec, err := strconv.ParseInt(cmd.Args[i], 10, 64)
			if err != nil {
				return errors.New("(error) ERR value is not an integer or out of range")
			}
			expMs = exDurSec * 1000
		default:
			return errors.New("(error) ERR synatx error")
		}
	}
	Put(key, NewObj(value, expMs))
	if err := evalCommand(conn); err != nil {
		return errors.New("(error) ERR OK conn write error")
	}
	return nil
}

func evalGET(cmd *RedisCmd, conn io.ReadWriter) error {
	if len(cmd.Args) < 1 {
		return errors.New("(error) ERR wrong number of arguments for set command")
	}
	var key string
	key = cmd.Args[0]
	value := Get(key)
	if value == nil {
		conn.Write(RESP_NIL)
		return nil
	}
	if value.ExpiresAt != -1 && value.ExpiresAt <= time.Now().UnixMilli() {
		conn.Write(RESP_NIL)
		return nil
	}

	conn.Write(Encode(value.Value, false))

	return nil
}

func evalTTL(cmd *RedisCmd, conn io.ReadWriter) error {
	if len(cmd.Args) < 1 {
		return errors.New("(error) ERR wrong number of arguments for TTL command")
	}
	var key string
	key = cmd.Args[0]
	value := Get(key)
	if value == nil {
		conn.Write([]byte(":-2\r\n"))
		return nil
	}

	if value.ExpiresAt == -1 {
		conn.Write([]byte(":-1\r\n"))
		return nil
	}
	leftMs := value.ExpiresAt - time.Now().UnixMilli()
	if leftMs < 0 {
		conn.Write([]byte(":-2\r\n"))
		return nil
	}
	conn.Write(Encode(int64(leftMs/1000), false))

	return nil
}

func evalDEL(cmd *RedisCmd, conn io.ReadWriter) error {
	if len(cmd.Args) < 1 {
		return errors.New("(error) ERR wrong number of arguments for DEL command")
	}
	cnt := 0
	for _, key := range cmd.Args {
		if ok := Del(key); ok {
			cnt++
		}
	}
	conn.Write(Encode(int64(cnt), false))
	return nil
}

func evalCommand(conn io.ReadWriter) error {
	_, err := conn.Write([]byte("+OK\r\n"))
	return err
}
func evalBGREAOF(conn io.ReadWriter) error {
	go func() {
		DumpAlLAof()
	}()
	// if err != nil {
	// 	return errors.New("(error) ERR AOF file error")
	// }
	conn.Write([]byte("+OK\r\n"))
	return nil
}

func EvalAndRespond(cmds *RedisCmds, conn io.ReadWriter) error {
	//log.Println("command", cmd.Cmd)
	for _, cmd := range *cmds {
		var err error
		switch cmd.Cmd {
		case "PING":
			err = evalPing(cmd, conn)
		case "SET":
			err = evalSET(cmd, conn)
		case "GET":
			err = evalGET(cmd, conn)
		case "TTL":
			err = evalTTL(cmd, conn)
		case "DEL":
			err = evalDEL(cmd, conn)
		case "COMMAND":
			err = evalCommand(conn)
		case "BGREWRITE":
			err = evalBGREAOF(conn)
		default:
			errMsg := fmt.Sprintf("-ERR unknown command '%s'\r\n", cmd.Cmd)
			conn.Write([]byte(errMsg))
		}

		// If the eval function returned an error, send it to the client
		if err != nil {
			// Some of your errors already start with "(error) " or "ERR ".
			// Standard RESP errors start with a minus sign.
			conn.Write([]byte(fmt.Sprintf("-%s\r\n", err.Error())))
		}
	}

	return nil
}
