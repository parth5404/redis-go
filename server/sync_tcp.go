package server

import (
	"github/redis.go/config"
	"io"
	"log"
	"net"
	"strconv"
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
			log.Println("command", cmd)
			if err = respond(c, cmd); err != nil {
				log.Print("err write:", err)
			}
		}
		// go handle(c)
	}
}

func readCmd(c net.Conn) (string, error) {
	buf := make([]byte, 1024)
	n, err := c.Read(buf[:])
	if err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

func respond(c net.Conn, cmd string) error {
	//for RESP compliance
	_, err := c.Write([]byte("-ERR unknown command\r\n"))
	if err != nil {
		return err
	}
	return nil
}

func handle(c net.Conn) {
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
		log.Println("command", cmd)
		if err = respond(c, cmd); err != nil {
			log.Print("err write:", err)
		}
	}
}
