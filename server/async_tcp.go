package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"syscall"

	"github/redis.go/config"
	"github/redis.go/core"
)

// soReusePort is SO_REUSEPORT.
//
// Go's syscall package does not export it (only x/sys/unix does), and this
// project has no x/sys dependency, so the Linux value is declared here. It is
// stable ABI: 15 on every Linux architecture except a handful of oddballs
// (Alpha, MIPS, SPARC) that this server does not target.
const soReusePort = 15

// Server is the epoll-based TCP front end.
//
// # Architecture
//
// One or more reactors, each an OS thread running its own epoll loop, its own
// listening socket and its own set of clients. Nothing is shared between
// reactors except the keyspace, which is sharded and independently locked, so
// two reactors only contend when they touch the same shard.
//
// This is what the two TODOs in the original file were asking for -- "assign
// this new client to a separate IO thread" and "same I/O thread for read cmd".
// The design answer to both is that a client is owned by exactly one reactor
// for its whole lifetime: accept, read, execute and write all happen on the same
// goroutine, so no per-client state needs a lock and no reply can be interleaved
// with another.
//
// # Why SO_REUSEPORT rather than one shared listener
//
// With a single listening socket and N reactors, every reactor wakes on every
// incoming connection and N-1 of them lose the race -- the thundering herd. With
// SO_REUSEPORT each reactor binds its own socket to the same address and the
// kernel hashes each incoming connection to exactly one of them, so exactly one
// reactor wakes. It also removes the accept-queue lock from the fast path.
type Server struct {
	reactors []*reactor

	wg   sync.WaitGroup
	once sync.Once

	// shuttingDown makes reactors exit their loops. Setting it is not enough on
	// its own: a reactor blocked in EpollWait with an infinite timeout would
	// never notice, which is what wakeFd is for.
	shuttingDown atomic.Bool

	stopExpiry func()
}

type reactor struct {
	id       int
	listenFd int
	epfd     int

	// wakeFd is the read end of a self-pipe registered in this reactor's epoll
	// set. Writing one byte to wakeWrite makes EpollWait return immediately,
	// which is how a blocking event loop is shut down without polling on a
	// timeout.
	wakeFd    int
	wakeWrite int

	clients map[int]*Client
	events  []syscall.EpollEvent
	scratch *scratch

	srv *Server
}

// NewServer binds the listening sockets and creates the epoll instances.
//
// Everything that can fail is done here, before any goroutine starts, so a bad
// port or a permissions problem is reported to the caller as an error instead of
// being logged from inside a reactor.
func NewServer() (*Server, error) {
	addr, err := resolveBindAddr(config.Host, config.Port)
	if err != nil {
		return nil, err
	}

	n := config.NumReactors
	if n < 1 {
		n = 1
	}

	s := &Server{reactors: make([]*reactor, 0, n)}

	for i := 0; i < n; i++ {
		// SO_REUSEPORT is only needed when several sockets share the address.
		r, err := newReactor(i, addr, n > 1)
		if err != nil {
			s.closeAll()
			return nil, err
		}
		s.reactors = append(s.reactors, r)
		r.srv = s
	}
	return s, nil
}

// resolveBindAddr turns a host string and port into a sockaddr.
//
// # Two bugs fixed here
//
// First, net.ParseIP returns a 16-byte slice for an IPv4 address in its
// IPv4-in-IPv6 form, so the original code's `ipv4[0], ipv4[1], ipv4[2],
// ipv4[3]` read the leading zeroes of the v6 prefix rather than the address.
// For "0.0.0.0" the result happened to be correct, which is why the bug went
// unnoticed -- any other bind address silently bound to the wrong interface.
// .To4() normalises to the 4-byte form.
//
// Second, this used to be a package-level var. Package-level initialisation
// runs before main, and therefore before flag.Parse, so --host and --port were
// baked in at their defaults and the flags had no effect. Resolving at call
// time is what makes them work.
func resolveBindAddr(host string, port int) (*syscall.SockaddrInet4, error) {
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid port %d", port)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, fmt.Errorf("invalid bind address %q", host)
	}
	v4 := ip.To4()
	if v4 == nil {
		return nil, fmt.Errorf("bind address %q is not IPv4; this server is AF_INET only", host)
	}
	sa := &syscall.SockaddrInet4{Port: port}
	copy(sa.Addr[:], v4)
	return sa, nil
}

func newReactor(id int, addr *syscall.SockaddrInet4, reusePort bool) (*reactor, error) {
	// SOCK_NONBLOCK, not O_NONBLOCK. The original passed syscall.O_NONBLOCK to
	// socket(2), which worked only by coincidence: on Linux both constants are
	// 0x800. On any other platform, or if either value ever changed, the socket
	// would have been created blocking and the whole event loop would stall on
	// the first accept.
	//
	// SOCK_CLOEXEC prevents the listening socket leaking into child processes.
	fd, err := syscall.Socket(syscall.AF_INET,
		syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("socket: %w", err)
	}

	cleanup := func(e error) (*reactor, error) {
		syscall.Close(fd)
		return nil, e
	}

	// SO_REUSEADDR lets the server restart immediately instead of waiting out
	// the TIME_WAIT state of the previous instance's sockets. Without it, a
	// restart within ~60 seconds fails with "address already in use" -- which
	// cost real debugging time on this project, because a failed rebind looks
	// exactly like a server that started and then hung.
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return cleanup(fmt.Errorf("SO_REUSEADDR: %w", err))
	}
	if reusePort {
		if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, soReusePort, 1); err != nil {
			return cleanup(fmt.Errorf("SO_REUSEPORT: %w", err))
		}
	}
	if err := syscall.Bind(fd, addr); err != nil {
		return cleanup(fmt.Errorf("bind %s:%d: %w", config.Host, config.Port, err))
	}
	// The backlog is the queue of connections the kernel has completed the
	// handshake for but the application has not accepted yet. A small backlog
	// turns a connection burst into refused connections.
	if err := syscall.Listen(fd, config.Backlog); err != nil {
		return cleanup(fmt.Errorf("listen: %w", err))
	}

	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return cleanup(fmt.Errorf("epoll_create1: %w", err))
	}

	pipe := make([]int, 2)
	if err := syscall.Pipe2(pipe, syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		syscall.Close(epfd)
		return cleanup(fmt.Errorf("pipe2: %w", err))
	}

	r := &reactor{
		id:        id,
		listenFd:  fd,
		epfd:      epfd,
		wakeFd:    pipe[0],
		wakeWrite: pipe[1],
		clients:   make(map[int]*Client),
		events:    make([]syscall.EpollEvent, config.MaxEventsPerLoop),
		scratch:   newScratch(),
	}

	if err := r.add(fd, syscall.EPOLLIN); err != nil {
		r.close()
		return nil, fmt.Errorf("epoll_ctl listen fd: %w", err)
	}
	if err := r.add(r.wakeFd, syscall.EPOLLIN); err != nil {
		r.close()
		return nil, fmt.Errorf("epoll_ctl wake fd: %w", err)
	}
	return r, nil
}

func (r *reactor) add(fd int, events uint32) error {
	return syscall.EpollCtl(r.epfd, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	})
}

func (r *reactor) mod(fd int, events uint32) error {
	return syscall.EpollCtl(r.epfd, syscall.EPOLL_CTL_MOD, fd, &syscall.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	})
}

func (r *reactor) close() {
	for _, c := range r.clients {
		c.Close()
	}
	r.clients = nil
	syscall.Close(r.wakeWrite)
	syscall.Close(r.wakeFd)
	syscall.Close(r.epfd)
	syscall.Close(r.listenFd)
}

func (s *Server) closeAll() {
	for _, r := range s.reactors {
		r.close()
	}
}

// Addr reports the address the server is listening on.
func (s *Server) Addr() string {
	return net.JoinHostPort(config.Host, fmt.Sprint(config.Port))
}

// Run starts every reactor and blocks until the server is shut down.
func (s *Server) Run() error {
	s.stopExpiry = core.KV.StartActiveExpiry()

	log.Printf("redis-go %s listening on %s (%d reactor(s), %d keyspace shards)",
		config.Version, s.Addr(), len(s.reactors), config.NumShards)

	errs := make([]error, len(s.reactors))
	for i, r := range s.reactors {
		s.wg.Add(1)
		go func(i int, r *reactor) {
			defer s.wg.Done()
			errs[i] = r.loop()
		}(i, r)
	}
	s.wg.Wait()

	if s.stopExpiry != nil {
		s.stopExpiry()
	}
	s.closeAll()

	return errors.Join(errs...)
}

// Shutdown asks every reactor to stop and returns immediately.
//
// Safe to call from a signal handler goroutine: it only sets a flag and writes
// a byte to each reactor's wake pipe.
func (s *Server) Shutdown() {
	s.once.Do(func() {
		s.shuttingDown.Store(true)
		for _, r := range s.reactors {
			// A one-byte write to a pipe with a 64 KB buffer cannot block, and
			// the error is deliberately ignored: if the pipe is already full
			// the reactor is guaranteed to wake anyway.
			syscall.Write(r.wakeWrite, []byte{1})
		}
	})
}

// loop is one reactor's event loop.
func (r *reactor) loop() error {
	for {
		n, err := syscall.EpollWait(r.epfd, r.events, -1)
		if err != nil {
			// # The EINTR bug
			//
			// This single check is the difference between a server that runs
			// and a server that dies. The Go runtime preempts goroutines by
			// sending SIGURG to the thread, roughly every 10 ms, and a signal
			// delivered while a thread is blocked in a syscall makes that
			// syscall return EINTR. The original code treated any error from
			// EpollWait as fatal and returned, so the server died within
			// milliseconds of starting under any real load -- and the
			// workaround in the project's own README, GODEBUG=asyncpreemptoff=1,
			// was disabling the runtime's preemption to hide it.
			//
			// EINTR is not an error condition. It means "you were interrupted,
			// call me again".
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return fmt.Errorf("reactor %d: epoll_wait: %w", r.id, err)
		}

		for i := 0; i < n; i++ {
			fd := int(r.events[i].Fd)
			ev := r.events[i].Events

			switch {
			case fd == r.wakeFd:
				// Drain the pipe; the byte itself carries no information.
				var b [8]byte
				syscall.Read(r.wakeFd, b[:])
				if r.srv.shuttingDown.Load() {
					return nil
				}

			case fd == r.listenFd:
				r.acceptAll()

			default:
				r.handleClient(fd, ev)
			}
		}
	}
}

// acceptAll drains the accept queue.
//
// Looping until EAGAIN matters with level-triggered epoll too: several
// connections can arrive between two wakeups, and accepting only one per event
// would leave the rest queued until the next connection arrives.
func (r *reactor) acceptAll() {
	for {
		// SOCK_NONBLOCK on the accepted socket, set by accept4 itself. The
		// original called Accept4(fd, 0) -- flags zero -- so every *client*
		// socket was blocking even though the listener was not. A blocking
		// client socket means one slow or malicious client can wedge the
		// entire event loop inside read().
		clientFd, _, err := syscall.Accept4(r.listenFd,
			syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				return
			}
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			// ECONNABORTED means the peer went away between the handshake and
			// our accept. Common under load, and not a reason to stop.
			if errors.Is(err, syscall.ECONNABORTED) {
				continue
			}
			log.Printf("reactor %d: accept: %v", r.id, err)
			return
		}

		// TCP_NODELAY disables Nagle's algorithm.
		//
		// Nagle holds a small write until the previous segment is acknowledged,
		// and the peer's delayed-ACK timer waits ~40 ms before sending that
		// acknowledgement. The interaction produces a fixed ~40 ms stall per
		// request, which is exactly the flat 41 ms latency this server used to
		// report at every pipeline depth. Every real Redis client sets this,
		// and so must the server.
		if err := syscall.SetsockoptInt(clientFd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1); err != nil {
			log.Printf("reactor %d: TCP_NODELAY: %v", r.id, err)
		}

		c := NewClient(clientFd)
		if err := r.add(clientFd, syscall.EPOLLIN); err != nil {
			log.Printf("reactor %d: epoll_ctl add client: %v", r.id, err)
			c.Close()
			continue
		}
		r.clients[clientFd] = c

		st := core.KV.Stats()
		st.Connections.Add(1)
		st.TotalConnections.Add(1)
	}
}

// handleClient processes one readiness event for one client.
func (r *reactor) handleClient(fd int, events uint32) {
	c, ok := r.clients[fd]
	if !ok {
		// An event for a socket we no longer track. Closing the fd already
		// removed it from the epoll set, so this can only be a stale event from
		// the same batch; ignore it rather than closing an fd number that may
		// have been recycled by a new connection.
		return
	}

	// EPOLLHUP/EPOLLERR mean the connection is gone. Still try to read first:
	// a peer that sent a command and immediately called shutdown(SHUT_WR)
	// deserves its reply, and EPOLLIN is usually set alongside.
	hangup := events&(syscall.EPOLLHUP|syscall.EPOLLERR) != 0

	if events&syscall.EPOLLOUT != 0 {
		done, err := c.Flush()
		if err != nil {
			r.drop(c)
			return
		}
		if done {
			// Stop asking about writability, otherwise the loop spins on a
			// permanently writable socket.
			if err := r.mod(fd, syscall.EPOLLIN); err != nil {
				r.drop(c)
				return
			}
		}
	}

	if events&syscall.EPOLLIN != 0 || hangup {
		if err := c.Read(r.scratch); err != nil {
			if !errors.Is(err, ErrClientClosed) {
				// A protocol-level failure gets an error reply before the
				// socket goes away, so the client learns why.
				c.Write(core.EncodeError(err))
				c.Flush()
			}
			r.drop(c)
			return
		}

		cmds, err := c.ParseCommands()
		if err != nil {
			// A malformed frame desynchronises the stream: there is no way to
			// know where the next command starts, so Redis also closes the
			// connection after replying.
			c.Write(core.EncodeError(err))
			c.Flush()
			r.drop(c)
			return
		}

		if len(cmds) > 0 {
			// Execute the whole pipeline into the client's write buffer, then
			// flush once. This is the other half of the Nagle fix: N commands
			// cost one write() syscall, not N.
			for _, cmd := range cmds {
				c.Write(core.Execute(cmd))
			}
			done, err := c.Flush()
			if err != nil {
				r.drop(c)
				return
			}
			if !done {
				// The kernel send buffer filled. Register interest in
				// writability so the remainder goes out when there is room,
				// instead of busy-looping or dropping the reply.
				if err := r.mod(fd, syscall.EPOLLIN|syscall.EPOLLOUT); err != nil {
					r.drop(c)
					return
				}
			}
		}
	}

	if hangup && !c.HasPendingWrites() {
		r.drop(c)
	}
}

// drop closes a client and forgets it.
func (r *reactor) drop(c *Client) {
	if _, ok := r.clients[c.Fd]; ok {
		delete(r.clients, c.Fd)
		core.KV.Stats().Connections.Add(-1)
	}
	c.Close()
}

// RunAsyncTCP starts a server with the current configuration and blocks.
//
// Kept for compatibility with the original entry point; new code should use
// NewServer so it can hold on to the handle and call Shutdown.
func RunAsyncTCP() error {
	s, err := NewServer()
	if err != nil {
		return err
	}
	return s.Run()
}
