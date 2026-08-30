package server

import (
	"errors"
	"syscall"

	"github/redis.go/config"
	"github/redis.go/core"
)

// Client is the per-connection state for one socket.
//
// # Why per-connection buffers are not optional
//
// TCP is a byte stream. A single read() can return half a command, or three
// commands, or two and a half. The original code allocated a fresh 512-byte
// buffer per event and assumed one read delivered exactly one complete command,
// which breaks in both directions:
//
//   - Under-read: a command split across two segments was parsed as garbage.
//   - Over-read: a pipeline of ten commands arriving together was parsed, but
//     with no accumulator a trailing partial eleventh was lost.
//
// So each client keeps an inbound accumulator that retains an unfinished frame
// until the rest arrives, and an outbound accumulator so all replies for one
// event become a single write() syscall.
type Client struct {
	Fd int

	// in holds bytes received but not yet consumed by a complete command.
	in []byte
	// out holds replies not yet handed to the kernel.
	out []byte

	// closed marks a client whose socket has been closed, so a reactor does
	// not act on a second event for a dead fd.
	closed bool
}

// NewClient wraps an accepted socket.
func NewClient(fd int) *Client {
	return &Client{
		Fd: fd,
		in: make([]byte, 0, config.ReadBufferSize),
	}
}

// ErrClientClosed signals the peer hung up cleanly.
var ErrClientClosed = errors.New("client closed connection")

// scratch is a reusable read landing area.
//
// One buffer per reactor goroutine, not per read: allocating 16 KB on every
// event makes the garbage collector the bottleneck at high request rates. It is
// safe to share only because a single reactor goroutine owns it and its
// contents are copied into c.in before anything else can run.
type scratch struct{ b []byte }

func newScratch() *scratch { return &scratch{b: make([]byte, config.ReadBufferSize)} }

// Read pulls whatever the kernel has into the client's accumulator.
//
// Returns ErrClientClosed on a clean EOF. A short read is normal and not an
// error: the accumulator keeps the partial frame for the next event.
func (c *Client) Read(s *scratch) error {
	for {
		n, err := syscall.Read(c.Fd, s.b)
		if err != nil {
			// EAGAIN means the socket is drained, which is the expected way
			// out of this loop for a non-blocking fd.
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				return nil
			}
			// EINTR is not a failure. A signal interrupted the syscall and
			// the correct response is to retry, not to drop the connection.
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return err
		}
		if n == 0 {
			return ErrClientClosed
		}

		// Refuse to buffer without bound. Otherwise a client that opens a
		// connection and dribbles `$536870911\r\n` forces us to hold half a
		// gigabyte per connection.
		if len(c.in)+n > config.MaxRequestSize {
			return errors.New("ERR Protocol error: request larger than limit")
		}
		c.in = append(c.in, s.b[:n]...)

		// A full buffer means there is probably more waiting; anything less
		// means the socket is drained and another read would only return
		// EAGAIN. Skipping that syscall is worth roughly one syscall per
		// request.
		if n < len(s.b) {
			return nil
		}
	}
}

// ParseCommands drains complete commands from the inbound accumulator.
//
// Whatever remains is an incomplete frame and is retained. This is the loop
// that makes both pipelining and split commands work, and it is why the RESP
// decoder must distinguish "malformed" from "incomplete".
func (c *Client) ParseCommands() (core.RedisCmds, error) {
	cmds, consumed, err := drainCommands(c.in)
	if err != nil {
		return nil, err
	}
	c.consume(consumed)
	return cmds, nil
}

// consume drops n bytes from the front of the inbound accumulator.
func (c *Client) consume(n int) {
	if n == 0 {
		return
	}
	if n >= len(c.in) {
		// Reset length but keep the capacity, so a steady-state connection
		// never allocates again.
		c.in = c.in[:0]
		return
	}
	// copy-and-truncate rather than re-slicing: re-slicing would keep the
	// consumed prefix alive and let the backing array grow without bound on a
	// long-lived pipelined connection.
	c.in = append(c.in[:0], c.in[n:]...)
}

// Write appends a reply to the outbound buffer. It does not touch the socket.
func (c *Client) Write(b []byte) (int, error) {
	c.out = append(c.out, b...)
	return len(b), nil
}

// Flush pushes the outbound buffer to the kernel.
//
// # Why buffering the replies is the single biggest performance fix
//
// The original server issued one write() per reply. With Nagle's algorithm
// enabled (the default) the kernel holds a small write back until the previous
// segment is acknowledged, and the client's delayed-ACK timer does not fire for
// ~40 ms. So a pipeline of N commands paid one 40 ms stall, which is exactly
// the flat ~41 ms round-trip measured at every pipeline depth from 2 to 16.
//
// Two independent changes remove it: TCP_NODELAY on the accepted socket
// (disabling Nagle) and coalescing every reply for one event into a single
// write() here. Either alone would help; together they turn a pipeline of 64
// commands into one syscall and one segment.
//
// Returns true when the buffer was fully drained. A false return means the
// socket's send buffer is full and the caller must wait for writability.
func (c *Client) Flush() (bool, error) {
	for len(c.out) > 0 {
		n, err := syscall.Write(c.Fd, c.out)
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				// Kernel buffer full. Keep the remainder and report that we
				// still owe the peer data.
				return false, nil
			}
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return false, err
		}
		// A short write is normal on a non-blocking socket. Dropping the
		// unwritten tail here would silently corrupt the protocol stream,
		// desynchronising the client for the rest of the connection.
		if n < len(c.out) {
			c.out = append(c.out[:0], c.out[n:]...)
			continue
		}
		c.out = c.out[:0]
	}
	return true, nil
}

// HasPendingWrites reports whether the client still owes the peer bytes.
func (c *Client) HasPendingWrites() bool { return len(c.out) > 0 }

// Close shuts the socket down, idempotently.
func (c *Client) Close() {
	if c.closed {
		return
	}
	c.closed = true
	// Closing the fd removes it from every epoll set it belongs to, so no
	// explicit EPOLL_CTL_DEL is required.
	syscall.Close(c.Fd)
}
