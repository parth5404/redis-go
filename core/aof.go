package core

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github/redis.go/config"
)

// AOF is an append-only file: a durable log of every write command, replayed on
// startup to reconstruct the keyspace.
//
// # Why a goroutine and a channel
//
// The file is touched by exactly one goroutine. Nothing else ever holds the
// file handle, so no mutex protects it -- the data is not shared, it is *sent*.
// Command execution pushes onto a buffered channel and returns immediately, so
// a client's latency never includes a disk write.
//
// A bounded buffer is deliberate. If the writer cannot keep up, the channel
// fills and the producer has to decide between blocking (backpressure) and
// dropping. Blocking on the reactor goroutine would stall every client on one
// slow disk, so Propagate drops and counts instead, and INFO reports
// aof_dropped so the loss is visible rather than silent.
//
// # What the original did
//
// The previous implementation was not an append-only log at all: it wrote a
// full snapshot only when a client sent BGREWRITE. Nothing was persisted
// otherwise, so any key written without an explicit BGREWRITE was lost on
// restart. It also built each record with fmt.Sprintf("SET %s %s") and split on
// spaces, so a key or value containing a space was silently split into extra
// arguments and reloaded as a different command. TTLs were never written, so
// every expiry was lost across a restart.
type AOF struct {
	path string

	ch   chan []byte
	done chan struct{}
	wg   sync.WaitGroup

	// closed guards against a double Close, and makes Propagate a no-op once
	// shutdown has begun. Without it, a command still in flight during
	// shutdown panics by sending on a closed channel.
	closed atomic.Bool

	// rewriteMu serialises rewrites against each other so two concurrent
	// BGREWRITEAOF calls cannot interleave writes into the same temp file.
	rewriteMu sync.Mutex

	written         atomic.Int64
	bytes           atomic.Int64
	fsyncs          atomic.Int64
	dropped         atomic.Int64
	lastRewriteKeys atomic.Int64
}

// DefaultAOF is the process-wide AOF, nil when persistence is disabled.
var DefaultAOF *AOF

// AOFStats is a point-in-time copy of the counters, for INFO.
type AOFStats struct {
	Written         int64
	Bytes           int64
	Fsyncs          int64
	Dropped         int64
	LastRewriteKeys int64
}

// Snapshot copies the counters.
func (a *AOF) Snapshot() AOFStats {
	return AOFStats{
		Written:         a.written.Load(),
		Bytes:           a.bytes.Load(),
		Fsyncs:          a.fsyncs.Load(),
		Dropped:         a.dropped.Load(),
		LastRewriteKeys: a.lastRewriteKeys.Load(),
	}
}

// OpenAOF opens (or creates) the append-only file and starts its writer.
func OpenAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	a := &AOF{
		path: path,
		ch:   make(chan []byte, config.AOFBufferSize),
		done: make(chan struct{}),
	}
	a.wg.Add(1)
	go a.writeLoop(f)
	return a, nil
}

// Propagate queues cmd for durable storage.
//
// Never blocks: a non-blocking send with a default branch means a saturated
// disk degrades durability instead of throughput. The drop is counted.
func (a *AOF) Propagate(cmd *RedisCmd) {
	if a == nil || a.closed.Load() {
		return
	}
	rec := encodeCommand(cmd)
	select {
	case a.ch <- rec:
	default:
		a.dropped.Add(1)
	}
}

// encodeCommand serialises a command as a RESP array.
//
// RESP framing rather than a space-separated line is the whole fix for the
// original's quoting bug: every argument carries an explicit byte count, so a
// value containing spaces, newlines or binary data round-trips exactly.
func encodeCommand(cmd *RedisCmd) []byte {
	tokens := make([]string, 0, len(cmd.Args)+1)
	tokens = append(tokens, strings.ToUpper(cmd.Cmd))
	tokens = append(tokens, cmd.Args...)
	return EncodeStringArray(tokens)
}

// writeLoop owns the file handle for its entire lifetime.
func (a *AOF) writeLoop(f *os.File) {
	defer a.wg.Done()
	defer f.Close()

	// Buffer in user space so a burst of small commands becomes a few large
	// write() syscalls rather than one syscall each.
	bw := bufio.NewWriterSize(f, 256*1024)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	// dirty tracks whether anything has been written since the last fsync, so
	// the everysec timer does not issue a pointless fsync on an idle server.
	dirty := false

	flush := func(sync bool) {
		if err := bw.Flush(); err != nil {
			logf("AOF write error: %v", err)
			return
		}
		if sync && dirty {
			// fsync is the only thing that actually makes data survive a
			// power loss; a successful write(2) only reaches the page cache.
			if err := f.Sync(); err != nil {
				logf("AOF fsync error: %v", err)
			} else {
				a.fsyncs.Add(1)
			}
			dirty = false
		}
	}

	for {
		select {
		case rec := <-a.ch:
			n, err := bw.Write(rec)
			if err != nil {
				logf("AOF write error: %v", err)
				continue
			}
			a.written.Add(1)
			a.bytes.Add(int64(n))
			dirty = true

			if config.AOFFsync == config.FsyncAlways {
				flush(true)
			}

		case <-ticker.C:
			if config.AOFFsync == config.FsyncEverySec {
				flush(true)
			} else {
				flush(false)
			}

		case <-a.done:
			// Drain whatever is still queued before exiting, so a clean
			// shutdown loses nothing that was already acknowledged.
			for {
				select {
				case rec := <-a.ch:
					if n, err := bw.Write(rec); err == nil {
						a.written.Add(1)
						a.bytes.Add(int64(n))
						dirty = true
					}
					continue
				default:
				}
				break
			}
			flush(true)
			return
		}
	}
}

// Close stops the writer after draining the queue.
func (a *AOF) Close() {
	if a == nil || !a.closed.CompareAndSwap(false, true) {
		return
	}
	close(a.done)
	a.wg.Wait()
}

// Rewrite compacts the log to the minimum set of commands that reproduces the
// current keyspace.
//
// An append-only log grows without bound: a key written a million times has a
// million records but one value. Rewriting emits one SET per live key.
//
// The new file is built at a temporary path and moved into place with rename(2),
// which is atomic on POSIX. Writing the live file in place -- which the original
// did, opening it with O_TRUNC -- means a crash mid-rewrite leaves a truncated
// file and the entire dataset is gone.
func (a *AOF) Rewrite(store *Store) error {
	if a == nil {
		return errors.New("AOF disabled")
	}
	a.rewriteMu.Lock()
	defer a.rewriteMu.Unlock()

	// Snapshot first, so the store's locks are released before any disk I/O.
	snapshot := store.Snapshot()

	tmp := a.path + ".rewrite.tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 256*1024)

	now := time.Now().UnixMilli()
	count := 0
	for k, obj := range snapshot {
		if obj.IsExpired(now) {
			continue
		}
		tokens := []string{"SET", k, obj.StringValue()}
		if obj.ExpiresAt != NoExpiry {
			// PXAT persists the *absolute* deadline. Writing a relative TTL
			// would restart the clock on every rewrite and on every restart,
			// so a key with a 10s TTL could live forever across restarts.
			tokens = append(tokens, "PXAT", formatInt64(obj.ExpiresAt))
		}
		if _, err := bw.Write(EncodeStringArray(tokens)); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		count++
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	// fsync the data before the rename: rename only guarantees the directory
	// entry is atomic, not that the file's contents already reached disk.
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, a.path); err != nil {
		os.Remove(tmp)
		return err
	}
	// fsync the directory so the rename itself is durable.
	if dir, err := os.Open(filepath.Dir(a.path)); err == nil {
		_ = dir.Sync()
		dir.Close()
	}

	a.lastRewriteKeys.Store(int64(count))
	logf("AOF rewrite complete: %d keys", count)
	return nil
}

// LoadAOF replays path into store and returns how many commands were applied.
//
// A truncated tail is tolerated rather than fatal: the process may have been
// killed mid-write, and Redis behaves the same way (aof-load-truncated yes).
// Whatever prefix parses is applied; the rest is discarded.
//
// The store argument is honoured by pointing the package-level KV at it for the
// duration of the replay, because the command handlers read KV directly. That is
// safe only because replay happens during startup, before any reactor is
// accepting connections. It is worth the awkwardness: the previous version
// accepted a store and then silently ignored it, applying every command to
// whatever KV happened to be -- which worked by accident in main and would have
// written into the wrong keyspace anywhere else.
func LoadAOF(path string, store *Store) (int, error) {
	if store != nil && store != KV {
		prev := KV
		KV = store
		defer func() { KV = prev }()
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if len(data) == 0 {
		return 0, nil
	}

	values, err := Decode(data)
	if err != nil {
		return 0, err
	}
	cmds, err := ParseCommands(values)
	if err != nil {
		return 0, err
	}

	applied := 0
	for _, cmd := range cmds {
		meta, ok := LookupCommand(cmd.Cmd)
		// Replay only write commands. A log that somehow contains a read
		// would be harmless but pointless, and skipping unknown names keeps
		// an older file loadable after a command is renamed.
		if !ok || !meta.Write {
			continue
		}
		if !arityOK(meta, len(cmd.Args)+1) {
			continue
		}
		reply := meta.Handler(cmd.Args)
		if isErrorReply(reply) {
			continue
		}
		applied++
	}
	return applied, nil
}
