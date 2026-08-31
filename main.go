package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"

	"github/redis.go/config"
	"github/redis.go/core"
	"github/redis.go/server"
)

var (
	mcpMode   bool
	pprofAddr string
)

func setupFlags() {
	flag.StringVar(&config.Host, "host", config.Host, "host to bind")
	flag.IntVar(&config.Port, "port", config.Port, "port to listen on")
	flag.IntVar(&config.KeyLimit, "keylimit", config.KeyLimit, "maximum number of keys before eviction kicks in")
	flag.StringVar(&config.AOFfile, "aof", config.AOFfile, "append-only file used for persistence")
	flag.BoolVar(&mcpMode, "mcp", false, "also serve the MCP tool interface over stdio")
	flag.StringVar(&pprofAddr, "pprof", "", "serve net/http/pprof on this address, e.g. localhost:6060 (off by default)")
	flag.Parse()
}

func main() {
	setupFlags()

	core.LoadAof()

	if pprofAddr != "" {
		go func() {
			log.Printf("pprof listening on %s", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Printf("pprof failed: %v", err)
			}
		}()
	}

	if !mcpMode {
		if err := server.RunAsyncTCP(); err != nil {
			log.Fatalf("server error: %v", err)
		}
		return
	}

	// In MCP mode the RESP listener keeps running alongside the stdio server,
	// so redis-cli and a model can work against the same live keyspace.
	// Logging stays on stderr because stdout carries the MCP protocol.
	log.Println("serving MCP over stdio")
	go func() {
		if err := server.RunAsyncTCP(); err != nil {
			log.Printf("TCP server stopped: %v", err)
		}
	}()
	if err := server.StartMCPServer(); err != nil {
		log.Fatalf("MCP server error: %v", err)
	}
}
