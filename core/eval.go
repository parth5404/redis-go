package core

import (
	"errors"
	"fmt"
	"log"
	"net"
)

func evalPing(cmd *RedisCmd, conn net.Conn) error {
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
func evalCommand(cmd *RedisCmd, conn net.Conn) error {
	_, err := conn.Write([]byte("*0\r\n"))
	return err
}

func EvalAndRespond(cmd *RedisCmd, conn net.Conn) error {
	log.Println("command", cmd.Cmd)
	switch cmd.Cmd {
	case "PING":
		return evalPing(cmd, conn)
	case "COMMAND":
		return evalCommand(cmd, conn)
	}
	return nil
}
