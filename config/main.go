package config

var Host string = "0.0.0.0"
var Port int = 7378

// KeyLimit is the point at which eviction starts reclaiming keys. It exists to
// keep memory bounded rather than to model a byte budget: the store counts keys,
// not bytes.
var KeyLimit int = 100_000

var AOFfile string = "appendonly.aof"

// EvictionRatio is the share of KeyLimit reclaimed in one eviction pass.
// Reclaiming a batch rather than a single key keeps eviction from running on
// every subsequent write once the store is full.
var EvictionRatio float32 = 0.4

var EvictionStrategy string = "allkeys-random"
