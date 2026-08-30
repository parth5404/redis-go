package core

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github/redis.go/config"
)

var startTime = time.Now()

func evalPING(args []string) []byte {
	// PING takes zero or one argument. With one, Redis echoes it as a bulk
	// string; with none it replies +PONG. Arity is validated centrally, so
	// this only has to distinguish the two shapes.
	if len(args) == 0 {
		return EncodeSimpleString("PONG")
	}
	if len(args) > 1 {
		return EncodeError(wrongArity("ping"))
	}
	return EncodeBulkString(args[0])
}

func evalECHO(args []string) []byte { return EncodeBulkString(args[0]) }

func evalSELECT(args []string) []byte {
	// Only database 0 exists. Reporting an honest error beats accepting the
	// command and silently ignoring it, which would make a client think it
	// had switched databases.
	if args[0] != "0" {
		return EncodeErrorf("ERR DB index is out of range")
	}
	return RespOK
}

func evalDBSIZE(args []string) []byte { return EncodeInt(int64(KV.Len())) }

func evalFLUSHDB(args []string) []byte {
	// ASYNC/SYNC are accepted and ignored: the flush is a map replacement,
	// which is already fast enough that the distinction is meaningless here.
	for _, a := range args {
		switch strings.ToUpper(a) {
		case "ASYNC", "SYNC":
		default:
			return EncodeError(ErrSyntax)
		}
	}
	KV.Flush()
	return RespOK
}

func evalCLIENT(args []string) []byte {
	// A minimal stub. redis-cli and several client libraries send CLIENT
	// subcommands during handshake and treat an error as fatal, so replying
	// sensibly to the common ones is what keeps those clients working.
	switch strings.ToUpper(args[0]) {
	case "SETNAME", "SETINFO":
		return RespOK
	case "GETNAME":
		return RespNil
	case "ID":
		return EncodeInt(1)
	default:
		return RespOK
	}
}

func evalCOMMAND(args []string) []byte {
	if len(args) == 0 {
		// The full COMMAND reply is an array of per-command metadata. We
		// report names only, which is enough for clients that use it to
		// feature-detect.
		return EncodeStringArray(CommandNames())
	}
	switch strings.ToUpper(args[0]) {
	case "COUNT":
		return EncodeInt(int64(CommandCount()))
	case "DOCS":
		// redis-cli calls COMMAND DOCS on connect purely for tab
		// completion. An empty map is a valid answer and stops it from
		// printing a warning; returning an error here made every
		// interactive session start with a complaint.
		return RespEmptyArray
	case "INFO":
		return RespEmptyArray
	default:
		return RespEmptyArray
	}
}

func evalCONFIG(args []string) []byte {
	switch strings.ToUpper(args[0]) {
	case "GET":
		if len(args) < 2 {
			return EncodeError(wrongArity("config|get"))
		}
		// redis-benchmark issues CONFIG GET save at startup and prints
		// "WARNING: Could not fetch server CONFIG" when it fails. Answering
		// it is why the benchmark output is clean now.
		params := map[string]string{
			"save":             "",
			"appendonly":       boolToYesNo(config.AOFEnabled),
			"appendfsync":      config.AOFFsync,
			"maxmemory":        "0",
			"maxmemory-policy": config.EvictionStrategy,
			"maxkeys":          formatInt64(int64(config.MaxKeys)),
			"databases":        "1",
			"port":             formatInt64(int64(config.Port)),
		}
		out := make([]string, 0, 4)
		for _, pattern := range args[1:] {
			for k, v := range params {
				if matchGlob(strings.ToLower(pattern), k) {
					out = append(out, k, v)
				}
			}
		}
		return EncodeStringArray(out)
	case "SET":
		// Accepted but not applied. Silently succeeding is the lesser evil:
		// benchmark tools set irrelevant parameters and abort on an error.
		return RespOK
	case "RESETSTAT":
		return RespOK
	default:
		return EncodeErrorf("ERR Unknown CONFIG subcommand '%s'", args[0])
	}
}

func boolToYesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func evalINFO(args []string) []byte {
	st := KV.Stats()
	hits, misses := st.Hits.Load(), st.Misses.Load()
	var hitRate float64
	if total := hits + misses; total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	var b strings.Builder
	fmt.Fprintf(&b, "# Server\r\n")
	fmt.Fprintf(&b, "redis_version:%s\r\n", config.Version)
	fmt.Fprintf(&b, "redis_go:1\r\n")
	fmt.Fprintf(&b, "process_id:%d\r\n", os.Getpid())
	fmt.Fprintf(&b, "go_version:%s\r\n", runtime.Version())
	fmt.Fprintf(&b, "uptime_in_seconds:%d\r\n", int64(time.Since(startTime).Seconds()))
	fmt.Fprintf(&b, "tcp_port:%d\r\n", config.Port)
	fmt.Fprintf(&b, "io_reactors:%d\r\n", config.NumReactors)
	fmt.Fprintf(&b, "keyspace_shards:%d\r\n", config.NumShards)

	fmt.Fprintf(&b, "\r\n# Clients\r\n")
	fmt.Fprintf(&b, "connected_clients:%d\r\n", st.Connections.Load())
	fmt.Fprintf(&b, "total_connections_received:%d\r\n", st.TotalConnections.Load())

	fmt.Fprintf(&b, "\r\n# Memory\r\n")
	fmt.Fprintf(&b, "used_memory:%d\r\n", mem.HeapAlloc)
	fmt.Fprintf(&b, "used_memory_rss:%d\r\n", mem.Sys)
	fmt.Fprintf(&b, "maxmemory_policy:%s\r\n", config.EvictionStrategy)
	fmt.Fprintf(&b, "gc_cycles:%d\r\n", mem.NumGC)

	fmt.Fprintf(&b, "\r\n# Stats\r\n")
	fmt.Fprintf(&b, "total_commands_processed:%d\r\n", st.CmdsHandled.Load())
	fmt.Fprintf(&b, "keyspace_hits:%d\r\n", hits)
	fmt.Fprintf(&b, "keyspace_misses:%d\r\n", misses)
	fmt.Fprintf(&b, "keyspace_hit_rate:%.4f\r\n", hitRate)
	fmt.Fprintf(&b, "expired_keys:%d\r\n", st.Expired.Load())
	fmt.Fprintf(&b, "evicted_keys:%d\r\n", st.Evicted.Load())

	fmt.Fprintf(&b, "\r\n# Persistence\r\n")
	fmt.Fprintf(&b, "aof_enabled:%d\r\n", boolToInt(config.AOFEnabled))
	fmt.Fprintf(&b, "aof_fsync_policy:%s\r\n", config.AOFFsync)
	if DefaultAOF != nil {
		s := DefaultAOF.Snapshot()
		fmt.Fprintf(&b, "aof_commands_written:%d\r\n", s.Written)
		fmt.Fprintf(&b, "aof_bytes_written:%d\r\n", s.Bytes)
		fmt.Fprintf(&b, "aof_fsyncs:%d\r\n", s.Fsyncs)
		fmt.Fprintf(&b, "aof_dropped:%d\r\n", s.Dropped)
		fmt.Fprintf(&b, "aof_last_rewrite_keys:%d\r\n", s.LastRewriteKeys)
	}

	fmt.Fprintf(&b, "\r\n# Keyspace\r\n")
	if n := KV.Len(); n > 0 {
		fmt.Fprintf(&b, "db0:keys=%d,expires=%d\r\n", n, 0)
	}

	// Filtering by section is cosmetic here; the whole block is cheap to
	// build, so an unrecognised section returns everything rather than
	// nothing.
	return EncodeBulkString(b.String())
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func evalBGREWRITEAOF(args []string) []byte {
	if DefaultAOF == nil {
		return EncodeErrorf("ERR AOF is disabled")
	}
	// Rewriting walks the whole keyspace and writes it to disk, so it runs
	// on its own goroutine and the client gets an immediate acknowledgement,
	// exactly as Redis's BGREWRITEAOF does. The original code did the same,
	// but discarded the error -- so a failed rewrite reported success.
	go func() {
		if err := DefaultAOF.Rewrite(KV); err != nil {
			logf("AOF rewrite failed: %v", err)
		}
	}()
	return EncodeSimpleString("Background append only file rewriting started")
}
