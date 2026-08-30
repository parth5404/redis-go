package main

import (
	"flag"
	"github/redis.go/config"
	"github/redis.go/core"
	"github/redis.go/server"
	"log"
	"net/http"
	_ "net/http/pprof"
)

var mcpMode bool

func setupFlags() {
	flag.StringVar(&config.Host, "host", config.Host, "host")
	flag.IntVar(&config.Port, "port", config.Port, "port")
	flag.BoolVar(&mcpMode, "mcp", false, "Run as an MCP stdio server")
	flag.Parse()
}

func main() {
	setupFlags()
	log.Println("Cache Hit")

	core.LoadAof()

	go func() {
		log.Println("Starting pprof on :6060")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.Printf("pprof failed: %v", err)
		}
	}()

	if mcpMode {
		log.Println("Starting MCP server over stdio...")
		go server.RunAsyncTCP()
		if err := server.StartMCPServer(); err != nil {
			log.Fatalf("MCP Server error: %v", err)
		}
	} else {
		server.RunAsyncTCP()
	}
}
