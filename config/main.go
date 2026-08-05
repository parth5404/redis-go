package config

var Host string = "0.0.0.0"
var Port int = 7378
var KeyLimit int = 200
var AOFfile string = "appendonly.aof"
var EvictionRatio float32 = 0.4
var EvictionStrategy string = "allkeys-random"
