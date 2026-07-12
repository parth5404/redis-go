package main

import (
	"flag"
	"github/redis.go/config"
	"github/redis.go/server"
	"log"
)

func setupFlags() {
	flag.StringVar(&config.Host, "host", config.Host, "host")
	flag.IntVar(&config.Port, "port", config.Port, "port")
	flag.Parse()
}

func main() {
	setupFlags()
	log.Println("Cache Hit")
	//server.RunSyncTCP()
	server.RunAsyncTCP()
}
