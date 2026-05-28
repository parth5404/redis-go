package main

import (
	"flag"
	"github/redis.go/config"
	"github/redis.go/server"
	"log"
)

func setupFlags() {
	flag.StringVar(&config.Host, "host", "0.0.0.0", "host")
	flag.IntVar(&config.Port, "port", 7379, "port")
	flag.Parse()
}

func main() {
	setupFlags()
	log.Println("Cache Hit")
	//server.RunSyncTCP()
	server.RunAsyncTCP()
}
