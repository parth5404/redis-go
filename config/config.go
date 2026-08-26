// Package config holds every tunable knob for the server.
//
// Values here are plain package-level variables rather than a struct so that
// `flag` can bind directly to them in main.go. Anything that derives from a
// config value (a socket address, a shard count) must be computed *after*
// flag.Parse() has run -- deriving it in a package-level initialiser is how
// the original code silently ignored --host and --port.
package config

// Fsync policies for the append-only file, mirroring Redis's appendfsync.
const (
	// FsyncAlways calls fsync(2) after every write batch. Slowest, safest.
	FsyncAlways = "always"
	// FsyncEverySec calls fsync(2) at most once per second from the AOF
	// writer goroutine. Redis's default; bounded loss window of ~1s.
	FsyncEverySec = "everysec"
	// FsyncNo never calls fsync explicitly and lets the kernel decide.
	FsyncNo = "no"
)

// Eviction policies applied when a shard reaches its share of MaxKeys.
const (
	// EvictAllKeysRandom drops a random subset of keys.
	EvictAllKeysRandom = "allkeys-random"
	// EvictAllKeysLRU samples keys and drops the least recently accessed,
	// the same approximated-LRU trick Redis uses (Redis samples 5).
	EvictAllKeysLRU = "allkeys-lru"
	// EvictNoEviction rejects writes with an OOM error once full.
	EvictNoEviction = "noeviction"
)

var (
	// Host is the interface to bind to.
	Host = "0.0.0.0"
	// Port is the TCP port for the RESP server.
	Port = 7378

	// MaxKeys is the total key budget across all shards. Each shard gets
	// MaxKeys/NumShards; see core.Store for why the limit is per-shard.
	MaxKeys = 1_000_000
	// EvictionRatio is the fraction of a full shard freed in one eviction
	// pass. Evicting a batch amortises the cost over many inserts instead
	// of evicting on every single write once the limit is reached.
	EvictionRatio = 0.10
	// EvictionStrategy is one of the Evict* constants.
	EvictionStrategy = EvictAllKeysLRU
	// EvictionSamples is how many keys approximated-LRU inspects per victim.
	EvictionSamples = 5

	// NumShards is the number of independently locked map partitions. Must
	// be a power of two so the shard index can be a mask instead of a
	// modulo. See core.Store.
	NumShards = 16

	// NumReactors is the number of independent epoll event loops. Each gets
	// its own listening socket via SO_REUSEPORT and the kernel load-balances
	// new connections across them. 1 reproduces classic single-threaded
	// Redis semantics.
	NumReactors = 1

	// Backlog is listen(2)'s queue depth: connections the kernel has finished
	// the handshake for but the server has not yet accepted. Too small and a
	// connection burst is met with refusals.
	Backlog = 4096

	// MaxEventsPerLoop is the size of the array epoll_wait fills per call.
	// It bounds how many ready sockets one loop iteration handles, not how
	// many connections are allowed.
	MaxEventsPerLoop = 1024

	// AOFEnabled turns command propagation to the append-only file on.
	AOFEnabled = true
	// AOFFile is the path to the append-only file.
	AOFFile = "appendonly.aof"
	// AOFFsync is one of the Fsync* constants.
	AOFFsync = FsyncEverySec
	// AOFBufferSize is the depth of the channel between command execution
	// and the AOF writer goroutine. A full buffer applies backpressure to
	// the reactor rather than growing without bound.
	AOFBufferSize = 8192

	// ExpiryCronFrequency (ms) is how often the active-expiry sweep runs.
	ExpiryCronFrequency = 100
	// ExpirySampleSize is how many keys per shard the sweep inspects.
	ExpirySampleSize = 20
	// ExpiryRetryThreshold is the expired-fraction above which the sweep
	// immediately samples again, per Redis's adaptive algorithm.
	ExpiryRetryThreshold = 0.25

	// PprofEnabled controls the net/http/pprof listener.
	PprofEnabled = false
	// PprofAddr is where pprof listens when enabled.
	PprofAddr = "localhost:6060"

	// ReadBufferSize is the per-connection read chunk size. The connection
	// keeps a growable accumulator on top of this, so a command larger than
	// this is assembled across multiple reads rather than truncated.
	ReadBufferSize = 16 * 1024
	// MaxRequestSize caps how much a single client may buffer before we
	// declare the request abusive and close the connection.
	MaxRequestSize = 64 * 1024 * 1024

	// LogCommands echoes every executed command to the log. Useful for
	// debugging, catastrophic for throughput -- off by default.
	LogCommands = false
)

// Version is reported by INFO.
const Version = "0.2.0"
