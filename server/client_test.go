package server

import (
	"bytes"
	"errors"
	"strings"
	"syscall"
	"testing"

	"github/redis.go/config"
	"github/redis.go/core"
)

// Tests for the pieces of the connection layer that can be exercised without a
// listening socket: the stream parser, the bind-address resolver, and Client's
// read/write buffering over a socketpair.

func TestDrainCommandsSingle(t *testing.T) {
	cmds, consumed, err := drainCommands([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cmds) != 1 || cmds[0].Cmd != "PING" {
		t.Fatalf("got %#v", cmds)
	}
	if consumed != 14 {
		t.Fatalf("consumed %d, want 14", consumed)
	}
}

// TestDrainCommandsPipeline is the property pipelining depends on: several
// commands in one buffer must all come out, in order, with the byte count the
// caller needs to advance.
func TestDrainCommandsPipeline(t *testing.T) {
	buf := []byte("*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*1\r\n$4\r\nPING\r\n")
	cmds, consumed, err := drainCommands(buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cmds) != 3 {
		t.Fatalf("got %d commands, want 3", len(cmds))
	}
	if cmds[1].Cmd != "GET" || len(cmds[1].Args) != 1 || cmds[1].Args[0] != "k" {
		t.Fatalf("second command wrong: %#v", cmds[1])
	}
	if consumed != len(buf) {
		t.Fatalf("consumed %d of %d bytes", consumed, len(buf))
	}
}

// TestDrainCommandsKeepsPartialTail is what makes a command split across TCP
// segments work: the complete prefix is returned and the incomplete tail is left
// unconsumed for the next read.
func TestDrainCommandsKeepsPartialTail(t *testing.T) {
	complete := "*1\r\n$4\r\nPING\r\n"
	partial := "*2\r\n$3\r\nGE"
	cmds, consumed, err := drainCommands([]byte(complete + partial))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cmds) != 1 {
		t.Fatalf("got %d commands, want 1", len(cmds))
	}
	if consumed != len(complete) {
		t.Fatalf("consumed %d, want %d: the partial frame must be retained",
			consumed, len(complete))
	}
}

func TestDrainCommandsEmptyBuffer(t *testing.T) {
	cmds, consumed, err := drainCommands(nil)
	if err != nil || len(cmds) != 0 || consumed != 0 {
		t.Fatalf("got (%#v, %d, %v)", cmds, consumed, err)
	}
}

// TestDrainCommandsRejectsNonArrays covers the crash in the original sync
// server: a client sending `+OK\r\n` decoded to a Go string, the code
// type-asserted it to []interface{} without checking, and the nil result was
// indexed.
func TestDrainCommandsRejectsNonArrays(t *testing.T) {
	for _, in := range []string{"+OK\r\n", ":42\r\n", "$3\r\nfoo\r\n"} {
		_, _, err := drainCommands([]byte(in))
		if err == nil {
			t.Errorf("drainCommands(%q) succeeded, want a protocol error", in)
		}
	}
}

// TestDrainCommandsSkipsEmptyFramesWithoutSpinning is the liveness property: a
// frame that yields no command must still advance the offset, or the loop
// consumes zero bytes forever and the reactor never returns.
func TestDrainCommandsSkipsEmptyFramesWithoutSpinning(t *testing.T) {
	buf := []byte("*0\r\n*-1\r\n*1\r\n$4\r\nPING\r\n")
	cmds, consumed, err := drainCommands(buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cmds) != 1 {
		t.Fatalf("got %d commands, want 1", len(cmds))
	}
	if consumed != len(buf) {
		t.Fatalf("consumed %d of %d bytes; an empty frame did not advance the offset",
			consumed, len(buf))
	}
}

func TestDrainCommandsInline(t *testing.T) {
	cmds, consumed, err := drainCommands([]byte("PING\r\nSET k \"a b\"\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cmds) != 2 {
		t.Fatalf("got %d commands, want 2", len(cmds))
	}
	if cmds[1].Cmd != "SET" || cmds[1].Args[1] != "a b" {
		t.Fatalf("inline quoting wrong: %#v", cmds[1])
	}
	if consumed != 19 {
		t.Fatalf("consumed %d, want 19", consumed)
	}
}

func TestResolveBindAddr(t *testing.T) {
	t.Run("IPv4 octets are not truncated", func(t *testing.T) {
		// The original read the first four bytes of net.ParseIP's 16-byte
		// IPv4-in-IPv6 representation, which are the v6 prefix's leading zeroes.
		// It bound to 0.0.0.0 for every input, so the bug was invisible for the
		// default host and silently wrong for every other one.
		sa, err := resolveBindAddr("127.0.0.1", 6379)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sa.Addr != [4]byte{127, 0, 0, 1} {
			t.Fatalf("addr = %v, want [127 0 0 1]", sa.Addr)
		}
		if sa.Port != 6379 {
			t.Fatalf("port = %d, want 6379", sa.Port)
		}
	})

	t.Run("wildcard", func(t *testing.T) {
		sa, err := resolveBindAddr("0.0.0.0", 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sa.Addr != [4]byte{0, 0, 0, 0} {
			t.Fatalf("addr = %v", sa.Addr)
		}
	})

	t.Run("rejected inputs", func(t *testing.T) {
		cases := []struct {
			host string
			port int
		}{
			{"127.0.0.1", 0},
			{"127.0.0.1", -1},
			{"127.0.0.1", 65536},
			{"not-an-ip", 6379},
			{"", 6379},
			{"::1", 6379},       // IPv6: this server is AF_INET only
			{"localhost", 6379}, // not resolved: an IP is required
			{"999.1.1.1", 6379},
		}
		for _, tc := range cases {
			if _, err := resolveBindAddr(tc.host, tc.port); err == nil {
				t.Errorf("resolveBindAddr(%q, %d) succeeded, want an error", tc.host, tc.port)
			}
		}
	})
}

// socketPair returns two connected non-blocking sockets, so Client's syscall
// paths can be driven directly without a server.
func socketPair(t *testing.T) (local, peer int) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX,
		syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	t.Cleanup(func() {
		syscall.Close(fds[0])
		syscall.Close(fds[1])
	})
	return fds[0], fds[1]
}

// TestClientReadAccumulatesAcrossReads is the regression test for the 512-byte
// per-event buffer. The original allocated a fresh buffer per event and assumed
// one read delivered exactly one whole command, so a command split across two
// segments was parsed as garbage.
func TestClientReadAccumulatesAcrossReads(t *testing.T) {
	local, peer := socketPair(t)
	c := NewClient(local)
	s := newScratch()

	// First segment: a valid prefix of one command.
	if _, err := syscall.Write(peer, []byte("*2\r\n$3\r\nGE")); err != nil {
		t.Fatal(err)
	}
	if err := c.Read(s); err != nil {
		t.Fatalf("first Read: %v", err)
	}
	cmds, err := c.ParseCommands()
	if err != nil {
		t.Fatalf("a valid prefix was reported as malformed: %v", err)
	}
	if len(cmds) != 0 {
		t.Fatalf("got %d commands from a partial frame, want 0", len(cmds))
	}

	// Second segment completes it.
	if _, err := syscall.Write(peer, []byte("T\r\n$1\r\nk\r\n")); err != nil {
		t.Fatal(err)
	}
	if err := c.Read(s); err != nil {
		t.Fatalf("second Read: %v", err)
	}
	cmds, err = c.ParseCommands()
	if err != nil {
		t.Fatalf("ParseCommands: %v", err)
	}
	if len(cmds) != 1 || cmds[0].Cmd != "GET" || cmds[0].Args[0] != "k" {
		t.Fatalf("got %#v, want GET k", cmds)
	}
}

// TestClientReadHandlesByteAtATimeDelivery is the extreme version: a peer is
// entitled to send one byte per segment.
func TestClientReadHandlesByteAtATimeDelivery(t *testing.T) {
	local, peer := socketPair(t)
	c := NewClient(local)
	s := newScratch()

	frame := []byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	for i, b := range frame {
		if _, err := syscall.Write(peer, []byte{b}); err != nil {
			t.Fatal(err)
		}
		if err := c.Read(s); err != nil {
			t.Fatalf("byte %d: Read: %v", i, err)
		}
		cmds, err := c.ParseCommands()
		if err != nil {
			t.Fatalf("byte %d: ParseCommands: %v", i, err)
		}
		if i < len(frame)-1 {
			if len(cmds) != 0 {
				t.Fatalf("a command was produced after only %d of %d bytes",
					i+1, len(frame))
			}
			continue
		}
		if len(cmds) != 1 || cmds[0].Cmd != "SET" {
			t.Fatalf("final byte produced %#v", cmds)
		}
	}
}

// TestClientReadHandlesCommandLargerThanTheReadBuffer covers a request bigger
// than one read(): the accumulator has to grow across several reads.
func TestClientReadHandlesCommandLargerThanTheReadBuffer(t *testing.T) {
	local, peer := socketPair(t)
	c := NewClient(local)
	s := newScratch()

	value := strings.Repeat("v", 3*config.ReadBufferSize)
	frame := core.EncodeStringArray([]string{"SET", "k", value})

	// Feed it in chunks, reading after each, exactly as the reactor would.
	for off := 0; off < len(frame); {
		end := off + 4096
		if end > len(frame) {
			end = len(frame)
		}
		n, err := syscall.Write(peer, frame[off:end])
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) {
				// The peer's send buffer is full; drain and retry.
				if err := c.Read(s); err != nil {
					t.Fatalf("Read: %v", err)
				}
				continue
			}
			t.Fatal(err)
		}
		off += n
		if err := c.Read(s); err != nil {
			t.Fatalf("Read: %v", err)
		}
	}

	cmds, err := c.ParseCommands()
	if err != nil {
		t.Fatalf("ParseCommands: %v", err)
	}
	if len(cmds) != 1 {
		t.Fatalf("got %d commands, want 1", len(cmds))
	}
	if cmds[0].Args[1] != value {
		t.Fatalf("value length %d, want %d", len(cmds[0].Args[1]), len(value))
	}
}

func TestClientReadReportsCleanEOF(t *testing.T) {
	local, peer := socketPair(t)
	c := NewClient(local)
	s := newScratch()

	if err := syscall.Close(peer); err != nil {
		t.Fatal(err)
	}
	err := c.Read(s)
	if !errors.Is(err, ErrClientClosed) {
		t.Fatalf("got %v, want ErrClientClosed", err)
	}
}

// TestClientReadRejectsAnAbusiveRequest covers the memory-exhaustion guard: a
// client that opens a connection and dribbles bytes forever must not be able to
// make the server buffer without bound.
func TestClientReadRejectsAnAbusiveRequest(t *testing.T) {
	prev := config.MaxRequestSize
	config.MaxRequestSize = 8 * 1024
	t.Cleanup(func() { config.MaxRequestSize = prev })

	local, peer := socketPair(t)
	c := NewClient(local)
	s := newScratch()

	// Enlarge the peer's send buffer so a single write can exceed the limit.
	syscall.SetsockoptInt(peer, syscall.SOL_SOCKET, syscall.SO_SNDBUF, 1<<20)

	var readErr error
	for i := 0; i < 64 && readErr == nil; i++ {
		if _, err := syscall.Write(peer, bytes.Repeat([]byte("x"), 4096)); err != nil {
			break
		}
		readErr = c.Read(s)
	}
	if readErr == nil {
		t.Fatal("buffering past MaxRequestSize was allowed")
	}
	if !strings.Contains(readErr.Error(), "larger than limit") {
		t.Fatalf("got %v, want a request-too-large error", readErr)
	}
}

// TestClientFlushHandlesShortWrites is the correctness half of the write path.
// A non-blocking socket accepts only what fits in its send buffer; dropping the
// unwritten tail would desynchronise the client for the rest of the connection,
// and it would look like data corruption rather than a networking bug.
func TestClientFlushHandlesShortWrites(t *testing.T) {
	local, peer := socketPair(t)
	// A small send buffer forces the partial-write path deterministically.
	if err := syscall.SetsockoptInt(local, syscall.SOL_SOCKET, syscall.SO_SNDBUF, 4096); err != nil {
		t.Fatalf("SO_SNDBUF: %v", err)
	}

	c := NewClient(local)
	payload := bytes.Repeat([]byte("abcdefgh"), 128*1024) // 1 MiB
	c.Write(payload)

	done, err := c.Flush()
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if done {
		t.Skip("the kernel accepted 1 MiB in one write; SO_SNDBUF was not honoured here")
	}
	if !c.HasPendingWrites() {
		t.Fatal("Flush reported an incomplete write but kept no pending bytes")
	}

	// Drain from the peer and keep flushing, as the reactor does on EPOLLOUT.
	got := make([]byte, 0, len(payload))
	buf := make([]byte, 64*1024)
	for i := 0; len(got) < len(payload); i++ {
		if i > 100_000 {
			t.Fatalf("gave up after %d rounds with %d of %d bytes received",
				i, len(got), len(payload))
		}
		n, rerr := syscall.Read(peer, buf)
		if n > 0 {
			got = append(got, buf[:n]...)
		}
		if rerr != nil && !errors.Is(rerr, syscall.EAGAIN) {
			t.Fatalf("peer read: %v", rerr)
		}
		if done, err = c.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}

	if !bytes.Equal(got, payload) {
		t.Fatalf("received %d bytes, want %d; a short write lost its tail",
			len(got), len(payload))
	}
	if c.HasPendingWrites() {
		t.Fatal("bytes are still pending after the payload was fully received")
	}
}

// TestConsumeDoesNotGrowTheBufferWithoutBound covers a long-lived pipelined
// connection. Re-slicing the accumulator instead of copying would keep every
// consumed byte alive in the backing array, so a connection that served a
// million commands would hold a buffer proportional to all of them.
func TestConsumeDoesNotGrowTheBufferWithoutBound(t *testing.T) {
	c := NewClient(-1) // no socket needed: only the buffer logic is exercised
	frame := []byte("*1\r\n$4\r\nPING\r\n")

	for i := 0; i < 10_000; i++ {
		c.in = append(c.in, frame...)
		cmds, err := c.ParseCommands()
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if len(cmds) != 1 {
			t.Fatalf("iteration %d: got %d commands", i, len(cmds))
		}
	}
	if len(c.in) != 0 {
		t.Fatalf("%d bytes left unconsumed", len(c.in))
	}
	if cap(c.in) > 4*config.ReadBufferSize {
		t.Fatalf("the accumulator grew to %d bytes after 10000 commands; "+
			"consumed bytes are not being released", cap(c.in))
	}
}

func TestClientCloseIsIdempotent(t *testing.T) {
	local, _ := socketPair(t)
	c := NewClient(local)
	c.Close()
	// A second Close must not close the fd again: the number may already have
	// been recycled by another connection, and closing it would break that one.
	c.Close()
	if !c.closed {
		t.Fatal("closed flag not set")
	}
}
