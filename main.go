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

func setupFlags() {
	flag.StringVar(&config.Host, "host", config.Host, "host")
	flag.IntVar(&config.Port, "port", config.Port, "port")
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

	//server.RunSyncTCP()
	server.RunAsyncTCP()
}
