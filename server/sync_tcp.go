package server

import (
	"fmt"
	"github/redis.go/config"
	"github/redis.go/core"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

var con_clients int = 0

func RunSyncTCP() {
	log.Println("starting a synchronous TCP server on", config.Host, config.Port)
	addr := net.JoinHostPort(config.Host, strconv.Itoa(config.Port))

	lsnr, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	for {
		c, err := lsnr.Accept()
		if err != nil {
			log.Println("err: ", err)
			continue
		}
		con_clients++
		log.Println("client connected with address:", c.RemoteAddr(), "concurrent clients", con_clients)
		for {
			cmd, err := readCmd(c)
			if err != nil {
				c.Close()
				con_clients -= 1
				log.Println("client disconnected", c.RemoteAddr(), "concurrent clients", con_clients)
				if err == io.EOF {
					break
				}
				log.Println("Error", err)
			}
			respond(c, cmd)
		}
		// go handle(c)
	}
}

func readCmd(c io.ReadWriter) (*core.RedisCmd, error) {
	buf := make([]byte, 512)
	n, err := c.Read(buf[:])
	if err != nil {
		return nil, err
	}
	rediscmd, err := core.DecodeArrayString(buf[:n])
	if err != nil {
		return nil, err
	}
	return &core.RedisCmd{
		Cmd:  strings.ToUpper(rediscmd[0]),
		Args: rediscmd[1:],
	}, nil
}

func respond(c io.ReadWriter, cmd *core.RedisCmd) {
	//for RESP compliance
	err := core.EvalAndRespond(cmd, c)
	if err != nil {
		respondError(err, c)
	}

}
func respondError(err error, c io.ReadWriter) {
	c.Write([]byte(fmt.Sprintf("-%s\r\n", err)))
}

// func handle(c net.Conn) {
// 	for {
// 		cmd, err := readCmd(c)
// 		if err != nil {
// 			c.Close()
// 			con_clients -= 1
// 			log.Println("client disconnected", c.RemoteAddr(), "concurrent clients", con_clients)
// 			if err == io.EOF {
// 				break
// 			}
// 			log.Println("Error", err)
// 		}
// 		log.Println("command", cmd)
// 		if err = respond(c, cmd); err != nil {
// 			log.Print("err write:", err)
// 		}
// 	}
// }
