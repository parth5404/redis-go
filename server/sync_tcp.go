package server

import (
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github/redis.go/config"
	"github/redis.go/core"
)

// The synchronous server: one goroutine per connection, blocking reads.
//
// It exists as a control for the epoll server rather than as the production
// path. Both share the same parser, the same dispatch table and the same
// keyspace, so a benchmark of one against the other measures exactly one
// variable: the I/O model. The numbers in the README come from running both.
//
// # What was wrong with the original
//
// The accept loop ran the per-connection read/respond loop *inline*, so the
// server handled exactly one client at a time -- the second connection sat in
// the accept queue until the first disconnected. The `go handle(c)` that would
// have fixed it was commented out. On top of that, an error from readCmds closed
// the connection and decremented the counter but then fell through to respond()
// on a nil command list, and readCmds returned (nil, nil) for a non-array frame,
// so a client sending `+OK\r\n` caused a nil-pointer dereference.

// syncClients counts live connections on the synchronous server.
var syncClients atomic.Int64

// SyncServer is the synchronous server's handle.
//
// It mirrors Server: bind in the constructor so a bad address is returned to the
// caller rather than logged from inside a goroutine, and expose Shutdown so the
// listener and every live connection are closed on the way out. The original had
// neither -- RunSyncTCP bound, logged and looped forever with no way back -- which
// also made it impossible to test.
type SyncServer struct {
	lsnr net.Listener

	wg   sync.WaitGroup
	once sync.Once

	shuttingDown atomic.Bool

	// conns tracks live connections so Shutdown can close them. Without this,
	// closing the listener stops new connections but Run still blocks in
	// wg.Wait() until every existing client happens to disconnect.
	mu    sync.Mutex
	conns map[net.Conn]struct{}

	stopExpiry func()
}

// NewSyncServer binds the listening socket.
//
// A port of 0 asks the kernel to choose one; Addr reports what it picked.
func NewSyncServer() (*SyncServer, error) {
	addr := net.JoinHostPort(config.Host, strconv.Itoa(config.Port))
	lsnr, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &SyncServer{lsnr: lsnr, conns: make(map[net.Conn]struct{})}, nil
}

// Addr reports the address actually bound.
func (s *SyncServer) Addr() string { return s.lsnr.Addr().String() }

// Run accepts connections until Shutdown is called.
func (s *SyncServer) Run() error {
	defer s.lsnr.Close()

	log.Printf("redis-go %s (sync mode) listening on %s", config.Version, s.Addr())

	s.stopExpiry = core.KV.StartActiveExpiry()
	defer s.stopExpiry()

	for {
		c, err := s.lsnr.Accept()
		if err != nil {
			if s.shuttingDown.Load() {
				// Shutdown closed the listener; this error is expected.
				s.wg.Wait()
				return nil
			}
			// A temporary accept error (fd exhaustion, for instance) must not
			// kill the server; the original returned from the loop on any error.
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			s.wg.Wait()
			return err
		}
		if tcp, ok := c.(*net.TCPConn); ok {
			// Same reasoning as the epoll path: without this, every reply pays
			// the Nagle/delayed-ACK stall.
			tcp.SetNoDelay(true)
		}

		s.mu.Lock()
		if s.shuttingDown.Load() {
			// Raced with Shutdown: refuse rather than register a connection
			// nobody will ever close.
			s.mu.Unlock()
			c.Close()
			continue
		}
		s.conns[c] = struct{}{}
		s.mu.Unlock()

		// The go statement is the whole point. The original ran this inline, so
		// the server handled exactly one client at a time and the second
		// connection sat in the accept queue until the first disconnected.
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				s.mu.Lock()
				delete(s.conns, c)
				s.mu.Unlock()
			}()
			handleSyncConn(c)
		}()
	}
}

// Shutdown closes the listener and every live connection, then returns.
func (s *SyncServer) Shutdown() {
	s.once.Do(func() {
		s.shuttingDown.Store(true)
		s.lsnr.Close()

		s.mu.Lock()
		for c := range s.conns {
			// Closing the socket under the reading goroutine makes its blocking
			// Read return immediately, which is the only way to interrupt it.
			c.Close()
		}
		s.mu.Unlock()
	})
}

// RunSyncTCP serves RESP with one goroutine per connection and blocks.
func RunSyncTCP() error {
	s, err := NewSyncServer()
	if err != nil {
		return err
	}
	return s.Run()
}

// handleSyncConn runs one connection to completion.
func handleSyncConn(c net.Conn) {
	syncClients.Add(1)
	st := core.KV.Stats()
	st.Connections.Add(1)
	st.TotalConnections.Add(1)

	defer func() {
		c.Close()
		syncClients.Add(-1)
		st.Connections.Add(-1)
	}()

	buf := make([]byte, config.ReadBufferSize)
	// acc is the same idea as Client.in: TCP is a byte stream, so a command may
	// span reads and a read may span commands.
	acc := make([]byte, 0, config.ReadBufferSize)

	for {
		n, err := c.Read(buf)
		if n > 0 {
			if len(acc)+n > config.MaxRequestSize {
				c.Write(core.EncodeErrorf("ERR Protocol error: request larger than limit"))
				return
			}
			acc = append(acc, buf[:n]...)

			cmds, consumed, perr := drainCommands(acc)
			if perr != nil {
				c.Write(core.EncodeError(perr))
				return
			}
			acc = append(acc[:0], acc[consumed:]...)

			if len(cmds) > 0 {
				// One buffer, one Write: see Client.Flush.
				var out []byte
				for _, cmd := range cmds {
					out = append(out, core.Execute(cmd)...)
				}
				if _, werr := c.Write(out); werr != nil {
					return
				}
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("sync: read from %v: %v", c.RemoteAddr(), err)
			}
			return
		}
	}
}

// drainCommands parses as many whole commands as data contains and reports how
// many bytes were consumed. A trailing partial frame is left for the caller.
func drainCommands(data []byte) (core.RedisCmds, int, error) {
	var cmds core.RedisCmds
	consumed := 0

	for consumed < len(data) {
		value, used, err := core.DecodeOne(data[consumed:])
		if err != nil {
			if errors.Is(err, core.ErrIncomplete) {
				break
			}
			return nil, consumed, err
		}
		if used <= 0 {
			return nil, consumed, errors.New("ERR Protocol error: decoder made no progress")
		}
		consumed += used

		if value == nil {
			continue
		}
		arr, ok := value.([]interface{})
		if !ok {
			return nil, consumed, errors.New("ERR Protocol error: expected an array of arguments")
		}
		if len(arr) == 0 {
			continue
		}
		tokens, err := core.DecodeArrayString(arr)
		if err != nil {
			return nil, consumed, err
		}
		cmds = append(cmds, &core.RedisCmd{Cmd: tokens[0], Args: tokens[1:]})
	}
	return cmds, consumed, nil
}
