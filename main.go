// Command redis-go is a single-binary Redis-compatible in-memory data store.
//
// It speaks RESP over TCP, so redis-cli and any standard Redis client library
// can talk to it unmodified. It can also expose the keyspace to an LLM over the
// Model Context Protocol on stdio.
//
// Run `redis-go --help` for the full flag list.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github/redis.go/config"
	"github/redis.go/core"
	"github/redis.go/server"
)

var (
	mcpMode     bool
	syncMode    bool
	showVersion bool
)

// setupFlags binds every tunable to a command-line flag.
//
// The original bound only --host and --port, and even those had no effect: the
// listening socket's address was computed in a package-level variable in the
// server package, which Go initialises *before* main runs and therefore before
// flag.Parse. Every other knob -- shard count, eviction policy, fsync policy --
// could only be changed by editing the source and rebuilding.
func setupFlags() {
	flag.StringVar(&config.Host, "host", config.Host, "interface to bind (IPv4)")
	flag.IntVar(&config.Port, "port", config.Port, "TCP port for the RESP server")

	flag.IntVar(&config.NumReactors, "reactors", config.NumReactors,
		"number of epoll event loops; 0 means one per CPU core")
	flag.IntVar(&config.NumShards, "shards", config.NumShards,
		"keyspace shards (rounded up to a power of two)")

	flag.IntVar(&config.MaxKeys, "maxkeys", config.MaxKeys,
		"key budget before eviction begins")
	flag.StringVar(&config.EvictionStrategy, "eviction", config.EvictionStrategy,
		"eviction policy: allkeys-lru, allkeys-random or noeviction")
	flag.IntVar(&config.EvictionSamples, "eviction-samples", config.EvictionSamples,
		"keys sampled per victim under allkeys-lru")

	flag.BoolVar(&config.AOFEnabled, "aof", config.AOFEnabled,
		"enable the append-only file")
	flag.StringVar(&config.AOFFile, "aof-file", config.AOFFile,
		"path to the append-only file")
	flag.StringVar(&config.AOFFsync, "aof-fsync", config.AOFFsync,
		"fsync policy: always, everysec or no")

	flag.IntVar(&config.ExpiryCronFrequency, "expiry-hz", config.ExpiryCronFrequency,
		"milliseconds between active-expiry sweeps")

	flag.BoolVar(&config.PprofEnabled, "pprof", config.PprofEnabled,
		"serve net/http/pprof")
	flag.StringVar(&config.PprofAddr, "pprof-addr", config.PprofAddr,
		"address for the pprof listener")

	flag.BoolVar(&config.LogCommands, "log-commands", config.LogCommands,
		"log every command; heavy, for debugging only")

	flag.BoolVar(&mcpMode, "mcp", false,
		"also serve the Model Context Protocol on stdio")
	flag.BoolVar(&syncMode, "sync", false,
		"use the goroutine-per-connection server instead of epoll (for comparison)")
	flag.BoolVar(&showVersion, "version", false, "print the version and exit")

	flag.Parse()
}

// validateConfig normalises derived values after parsing.
//
// This has to run after flag.Parse and before core.KV is used, which is why the
// store is created lazily via core.ReinitStore rather than in an init function.
func validateConfig() error {
	if config.NumReactors == 0 {
		config.NumReactors = runtime.NumCPU()
	}
	if config.NumReactors < 1 {
		return fmt.Errorf("--reactors must be >= 0, got %d", config.NumReactors)
	}
	if config.NumShards < 1 {
		return fmt.Errorf("--shards must be >= 1, got %d", config.NumShards)
	}
	if config.MaxKeys < 1 {
		return fmt.Errorf("--maxkeys must be >= 1, got %d", config.MaxKeys)
	}
	switch config.EvictionStrategy {
	case config.EvictAllKeysLRU, config.EvictAllKeysRandom, config.EvictNoEviction:
	default:
		return fmt.Errorf("--eviction must be one of %s, %s, %s",
			config.EvictAllKeysLRU, config.EvictAllKeysRandom, config.EvictNoEviction)
	}
	switch config.AOFFsync {
	case config.FsyncAlways, config.FsyncEverySec, config.FsyncNo:
	default:
		return fmt.Errorf("--aof-fsync must be one of %s, %s, %s",
			config.FsyncAlways, config.FsyncEverySec, config.FsyncNo)
	}
	if config.ExpiryCronFrequency < 1 {
		return fmt.Errorf("--expiry-hz must be >= 1ms, got %d", config.ExpiryCronFrequency)
	}
	if config.EvictionSamples < 1 {
		config.EvictionSamples = 1
	}
	return nil
}

func main() {
	setupFlags()

	if showVersion {
		fmt.Printf("redis-go %s (%s/%s, %s)\n",
			config.Version, runtime.GOOS, runtime.GOARCH, runtime.Version())
		return
	}

	// In MCP mode stdout is the protocol channel, so anything written there
	// corrupts the JSON-RPC stream. Redirect logging to stderr, which the host
	// treats as diagnostics.
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := validateConfig(); err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	// Rebuild the keyspace with the parsed shard count and key budget.
	core.ReinitStore()

	if config.PprofEnabled {
		go func() {
			log.Printf("pprof listening on http://%s/debug/pprof/", config.PprofAddr)
			// The original started this unconditionally, so every deployment
			// exposed a profiling endpoint that can dump heap contents and,
			// via /debug/pprof/profile, stall the process for 30 seconds.
			if err := http.ListenAndServe(config.PprofAddr, nil); err != nil {
				log.Printf("pprof listener stopped: %v", err)
			}
		}()
	}

	// Open the AOF before replaying it, so writes that arrive during startup
	// are appended to the same file the replay came from.
	if config.AOFEnabled {
		applied, err := core.LoadAOF(config.AOFFile, core.KV)
		if err != nil {
			// A corrupt log is a data-loss event; refusing to start is the
			// correct response, since silently continuing would then overwrite
			// the file with a partial dataset on the next rewrite.
			log.Fatalf("could not replay %s: %v", config.AOFFile, err)
		}
		if applied > 0 {
			log.Printf("replayed %d commands from %s, %d keys live",
				applied, config.AOFFile, core.KV.Len())
		}

		aof, err := core.OpenAOF(config.AOFFile)
		if err != nil {
			log.Fatalf("could not open %s: %v", config.AOFFile, err)
		}
		core.DefaultAOF = aof
		defer aof.Close()
	}

	if syncMode {
		runSync()
		return
	}
	runAsync()
}

// runAsync starts the epoll server and blocks until it stops.
func runAsync() {
	srv, err := server.NewServer()
	if err != nil {
		log.Fatalf("could not start server: %v", err)
	}

	// Signal handling exists so that a Ctrl-C flushes the AOF instead of
	// abandoning up to a second of acknowledged writes in the writer's buffer.
	stop := installSignalHandler(func() {
		log.Println("shutting down")
		srv.Shutdown()
	})
	defer stop()

	if mcpMode {
		// The MCP server owns stdio and blocks, so the TCP server runs
		// alongside it and both share one keyspace.
		go func() {
			if err := srv.Run(); err != nil {
				log.Printf("tcp server stopped: %v", err)
			}
		}()
		if err := server.StartMCPServer(); err != nil {
			log.Printf("mcp server stopped: %v", err)
		}
		srv.Shutdown()
		return
	}

	if err := srv.Run(); err != nil {
		log.Printf("server stopped: %v", err)
	}
	shutdownSummary()
}

// runSync starts the goroutine-per-connection server.
func runSync() {
	stop := installSignalHandler(func() {
		log.Println("shutting down")
		// The synchronous server blocks in Accept with no wake mechanism, so
		// the honest thing is to flush persistence and exit rather than pretend
		// to drain connections. This is one of the reasons it is the comparison
		// path and not the default.
		core.DefaultAOF.Close()
		shutdownSummary()
		os.Exit(0)
	})
	defer stop()

	if err := server.RunSyncTCP(); err != nil {
		log.Printf("server stopped: %v", err)
	}
	shutdownSummary()
}

// installSignalHandler runs fn on SIGINT or SIGTERM and returns a cleanup func.
func installSignalHandler(fn func()) func() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		if _, ok := <-ch; ok {
			fn()
		}
	}()
	return func() { signal.Stop(ch); close(ch) }
}

// shutdownSummary logs what the process did, which is useful when the server
// ran as part of a benchmark or test script.
func shutdownSummary() {
	st := core.KV.Stats()
	log.Printf("served %d commands, %d connections, %d keys remaining",
		st.CmdsHandled.Load(), st.TotalConnections.Load(), core.KV.Len())
}
