package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github/redis.go/config"
	"github/redis.go/core"
)

// Integration tests. These drive a real epoll server over a real loopback
// socket, because most of what this package does only exists at the syscall
// boundary: partial reads, partial writes, EPOLLOUT re-arming, EINTR, and the
// accept loop. A mock connection would test none of it.

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

type testServer struct {
	srv  *Server
	addr string

	mu     sync.Mutex
	waited bool
	runErr error
	done   chan error
}

// wait blocks until Run returns. Safe to call more than once, so a test may
// assert on shutdown itself and the cleanup can still check the result.
func (ts *testServer) wait() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.waited {
		return ts.runErr
	}
	select {
	case ts.runErr = <-ts.done:
	case <-time.After(20 * time.Second):
		ts.runErr = errors.New("Run did not return within 20s after Shutdown")
	}
	ts.waited = true
	return ts.runErr
}

// freePort asks the kernel for an unused port and immediately gives it back.
// There is a window in which something else could take it, which is why
// newTestServer retries.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// newTestServer starts a server on loopback with its own keyspace and no AOF.
//
// config is a set of package-level variables read at construction time, so they
// have to be set before NewServer and restored afterwards. Tests in this package
// therefore must not call t.Parallel.
func newTestServer(t *testing.T, reactors int) *testServer {
	t.Helper()

	prevHost, prevPort, prevReactors := config.Host, config.Port, config.NumReactors
	prevKV, prevAOF := core.KV, core.DefaultAOF
	t.Cleanup(func() {
		config.Host, config.Port, config.NumReactors = prevHost, prevPort, prevReactors
		core.KV, core.DefaultAOF = prevKV, prevAOF
	})

	config.Host = "127.0.0.1"
	config.NumReactors = reactors
	core.KV = core.NewStore(16, 200_000)
	// Persistence has its own tests; a disk write per command would only make
	// these slower and flakier.
	core.DefaultAOF = nil

	var srv *Server
	var err error
	for attempt := 0; attempt < 25; attempt++ {
		config.Port = freePort(t)
		srv, err = NewServer()
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ts := &testServer{srv: srv, addr: srv.Addr(), done: make(chan error, 1)}
	go func() { ts.done <- srv.Run() }()

	t.Cleanup(func() {
		srv.Shutdown()
		if err := ts.wait(); err != nil {
			t.Errorf("Run returned %v", err)
		}
	})

	waitForListener(t, ts.addr)
	return ts
}

func waitForListener(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("server at %s never started accepting", addr)
}

// ---------------------------------------------------------------------------
// a minimal RESP client
// ---------------------------------------------------------------------------

type respConn struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func dial(t *testing.T, addr string) *respConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	conn.SetDeadline(time.Now().Add(60 * time.Second))
	t.Cleanup(func() { conn.Close() })
	return &respConn{t: t, conn: conn, r: bufio.NewReaderSize(conn, 64*1024)}
}

func encode(args ...string) []byte {
	return core.EncodeStringArray(args)
}

func (rc *respConn) sendRaw(b []byte) {
	rc.t.Helper()
	if _, err := rc.conn.Write(b); err != nil {
		rc.t.Fatalf("write: %v", err)
	}
}

func (rc *respConn) send(args ...string) {
	rc.t.Helper()
	rc.sendRaw(encode(args...))
}

// reply reads exactly one RESP reply and returns its raw wire bytes, so tests
// can assert on the encoding and not just the decoded value.
func (rc *respConn) reply() string {
	rc.t.Helper()
	var sb strings.Builder
	if err := readReply(rc.r, &sb); err != nil {
		rc.t.Fatalf("reading reply: %v (got %q so far)", err, sb.String())
	}
	return sb.String()
}

func (rc *respConn) do(args ...string) string {
	rc.t.Helper()
	rc.send(args...)
	return rc.reply()
}

func readReply(r *bufio.Reader, sb *strings.Builder) error {
	line, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	sb.WriteString(line)
	if len(line) < 3 {
		return fmt.Errorf("malformed reply line %q", line)
	}
	body := strings.TrimSuffix(strings.TrimSuffix(line[1:], "\n"), "\r")

	switch line[0] {
	case '+', '-', ':':
		return nil

	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return fmt.Errorf("bad bulk length %q", body)
		}
		if n < 0 { // null bulk string
			return nil
		}
		buf := make([]byte, n+2) // payload plus CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			return fmt.Errorf("reading %d-byte bulk: %w", n, err)
		}
		if !bytes.HasSuffix(buf, []byte("\r\n")) {
			return errors.New("bulk string not terminated by CRLF")
		}
		sb.Write(buf)
		return nil

	case '*':
		n, err := strconv.Atoi(body)
		if err != nil {
			return fmt.Errorf("bad array length %q", body)
		}
		for i := 0; i < n; i++ {
			if err := readReply(r, sb); err != nil {
				return fmt.Errorf("array element %d: %w", i, err)
			}
		}
		return nil

	default:
		return fmt.Errorf("unknown reply type %q", line[0])
	}
}

// bulk builds the wire form of a bulk-string reply, for exact comparisons.
func bulk(s string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s) }

// ---------------------------------------------------------------------------
// basics
// ---------------------------------------------------------------------------

func TestServerAnswersPing(t *testing.T) {
	ts := newTestServer(t, 1)
	c := dial(t, ts.addr)

	if got := c.do("PING"); got != "+PONG\r\n" {
		t.Fatalf("PING = %q", got)
	}
	if got := c.do("ECHO", "hello world"); got != bulk("hello world") {
		t.Fatalf("ECHO = %q", got)
	}
}

func TestValuesRoundTripOverTheWire(t *testing.T) {
	ts := newTestServer(t, 1)
	c := dial(t, ts.addr)

	cases := []struct{ name, value string }{
		{"plain", "value"},
		{"empty", ""},
		{"spaces", "a value with spaces"},
		{"newlines", "line1\r\nline2\nline3"},
		{"resp lookalike", "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\ny\r\n"},
		{"binary", string([]byte{0x00, 0x01, 0xff, 0xfe, 0x0d, 0x0a})},
		{"utf8", "héllo wörld — ünïcode"},
		{"64KiB", strings.Repeat("x", 64*1024)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.do("SET", tc.name, tc.value); got != "+OK\r\n" {
				t.Fatalf("SET = %q", got)
			}
			if got := c.do("GET", tc.name); got != bulk(tc.value) {
				t.Fatalf("GET returned %d bytes, want %d", len(got), len(bulk(tc.value)))
			}
		})
	}
}

func TestInlineCommandsOverTheWire(t *testing.T) {
	ts := newTestServer(t, 1)
	c := dial(t, ts.addr)

	// This is what a human gets by running `telnet host port`, and what
	// redis-cli falls back to for some diagnostics.
	c.sendRaw([]byte("PING\r\n"))
	if got := c.reply(); got != "+PONG\r\n" {
		t.Fatalf("inline PING = %q", got)
	}

	c.sendRaw([]byte("SET inline \"a b c\"\r\nGET inline\r\n"))
	if got := c.reply(); got != "+OK\r\n" {
		t.Fatalf("inline SET = %q", got)
	}
	if got := c.reply(); got != bulk("a b c") {
		t.Fatalf("inline GET = %q", got)
	}
}

// ---------------------------------------------------------------------------
// stream framing
// ---------------------------------------------------------------------------

// TestPipelineRepliesComeBackInOrder is the property a client library depends
// on: replies are positional, so one missing or reordered reply desynchronises
// every subsequent command on the connection.
func TestPipelineRepliesComeBackInOrder(t *testing.T) {
	ts := newTestServer(t, 1)
	c := dial(t, ts.addr)

	const n = 1000
	var out bytes.Buffer
	for i := 0; i < n; i++ {
		out.Write(encode("SET", fmt.Sprintf("pk%d", i), fmt.Sprintf("pv%d", i)))
	}
	for i := 0; i < n; i++ {
		out.Write(encode("GET", fmt.Sprintf("pk%d", i)))
	}
	c.sendRaw(out.Bytes())

	for i := 0; i < n; i++ {
		if got := c.reply(); got != "+OK\r\n" {
			t.Fatalf("reply %d (SET) = %q", i, got)
		}
	}
	for i := 0; i < n; i++ {
		want := bulk(fmt.Sprintf("pv%d", i))
		if got := c.reply(); got != want {
			t.Fatalf("reply %d (GET) = %q, want %q", i, got, want)
		}
	}
}

// TestCommandSplitAcrossSegmentsIsReassembled sends one byte per write with a
// pause between each, which forces the kernel to deliver separate segments. The
// original server, which allocated a fresh buffer per event and parsed it in
// isolation, could not answer this at all.
func TestCommandSplitAcrossSegmentsIsReassembled(t *testing.T) {
	ts := newTestServer(t, 1)
	c := dial(t, ts.addr)

	frame := encode("SET", "split", "reassembled")
	for _, b := range frame {
		c.sendRaw([]byte{b})
		time.Sleep(200 * time.Microsecond)
	}
	if got := c.reply(); got != "+OK\r\n" {
		t.Fatalf("split SET = %q", got)
	}
	if got := c.do("GET", "split"); got != bulk("reassembled") {
		t.Fatalf("GET = %q", got)
	}
}

// TestRequestLargerThanTheReadBufferIsReassembled covers the other direction:
// one request that cannot possibly arrive in a single read().
func TestRequestLargerThanTheReadBufferIsReassembled(t *testing.T) {
	ts := newTestServer(t, 1)
	c := dial(t, ts.addr)

	value := strings.Repeat("abcdefgh", 512*1024) // 4 MiB, 256x the read buffer
	c.send("SET", "big", value)
	if got := c.reply(); got != "+OK\r\n" {
		t.Fatalf("SET = %q", got)
	}
	if got := c.do("STRLEN", "big"); got != fmt.Sprintf(":%d\r\n", len(value)) {
		t.Fatalf("STRLEN = %q, want :%d", got, len(value))
	}
}

// TestReplyLargerThanTheSocketBufferIsFullyDelivered exercises the partial-write
// path end to end. A 4 MiB reply cannot fit in the socket send buffer, so Flush
// returns early, the reactor arms EPOLLOUT, and the remainder goes out on later
// events. Dropping that tail would look like data corruption to the client
// rather than a networking bug, so this is the test that pins it.
func TestReplyLargerThanTheSocketBufferIsFullyDelivered(t *testing.T) {
	ts := newTestServer(t, 1)
	c := dial(t, ts.addr)

	value := strings.Repeat("0123456789abcdef", 256*1024) // 4 MiB
	if got := c.do("SET", "huge", value); got != "+OK\r\n" {
		t.Fatalf("SET = %q", got)
	}

	got := c.do("GET", "huge")
	if got != bulk(value) {
		t.Fatalf("GET returned %d bytes, want %d", len(got), len(bulk(value)))
	}

	// The connection must still be usable afterwards: if the EPOLLOUT
	// bookkeeping were wrong the socket would be left registered for
	// writability and spin, or be dropped.
	if got := c.do("PING"); got != "+PONG\r\n" {
		t.Fatalf("PING after a large reply = %q", got)
	}
}

// TestPipelinedRoundTripIsNotStalledByNagle is the performance regression test.
//
// Before TCP_NODELAY and reply coalescing, every round trip took a flat ~41 ms
// regardless of pipeline depth, because the server issued one write() per reply
// and the second small segment waited on the peer's delayed ACK. The threshold
// here is deliberately far below that floor and far above any plausible loopback
// time, so it discriminates the bug without being sensitive to machine load.
func TestPipelinedRoundTripIsNotStalledByNagle(t *testing.T) {
	ts := newTestServer(t, 1)
	c := dial(t, ts.addr)

	const depth = 64
	var batch bytes.Buffer
	for i := 0; i < depth; i++ {
		batch.Write(encode("PING"))
	}

	const rounds = 50
	samples := make([]time.Duration, 0, rounds)
	for i := 0; i < rounds; i++ {
		start := time.Now()
		c.sendRaw(batch.Bytes())
		for j := 0; j < depth; j++ {
			if got := c.reply(); got != "+PONG\r\n" {
				t.Fatalf("round %d reply %d = %q", i, j, got)
			}
		}
		samples = append(samples, time.Since(start))
	}

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	median := samples[len(samples)/2]
	t.Logf("pipeline depth %d: median round trip %v, best %v, worst %v",
		depth, median, samples[0], samples[len(samples)-1])

	if median > 20*time.Millisecond {
		t.Fatalf("median round trip %v: a delayed-ACK stall is back "+
			"(TCP_NODELAY or reply coalescing regressed)", median)
	}
}

// ---------------------------------------------------------------------------
// error handling and isolation
// ---------------------------------------------------------------------------

// TestMalformedFrameClosesOnlyThatConnection pins two behaviours at once: the
// offending client is told why and disconnected (a desynchronised stream cannot
// be recovered), and no other client -- nor the process -- is affected. The
// original server panicked on this input and took every connection with it.
func TestMalformedFrameClosesOnlyThatConnection(t *testing.T) {
	ts := newTestServer(t, 1)

	bystander := dial(t, ts.addr)
	if got := bystander.do("SET", "survivor", "yes"); got != "+OK\r\n" {
		t.Fatalf("setup SET = %q", got)
	}

	garbage := []struct{ name, payload string }{
		{"not an array", "+OK\r\n"},
		{"integer at top level", ":42\r\n"},
		{"bare bulk string", "$3\r\nfoo\r\n"},
		{"non numeric multibulk", "*abc\r\n"},
		{"non numeric bulk length", "$xyz\r\n"},
		{"negative element count", "*2\r\n$-5\r\nab\r\n"},
		{"binary noise", "\x00\x01\x02\x03\xff\xfe\r\n"},
	}
	for _, tc := range garbage {
		t.Run(tc.name, func(t *testing.T) {
			victim := dial(t, ts.addr)
			victim.sendRaw([]byte(tc.payload))

			// Read whatever comes back: an error reply then EOF, or just EOF.
			// Either is acceptable; a hang or a dead server is not.
			victim.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			data, err := io.ReadAll(victim.conn)
			if err != nil {
				t.Fatalf("the connection neither replied nor closed: %v", err)
			}
			if len(data) > 0 && data[0] != '-' {
				t.Fatalf("got %q, want an error reply or a clean close", data)
			}
			t.Logf("server said: %q", data)
		})
	}

	// The bystander's connection and its data must be untouched.
	if got := bystander.do("GET", "survivor"); got != bulk("yes") {
		t.Fatalf("bystander GET = %q; a malformed frame on another connection "+
			"affected this one", got)
	}
	if got := bystander.do("PING"); got != "+PONG\r\n" {
		t.Fatalf("bystander PING = %q", got)
	}
}

// TestUnknownAndMisusedCommandsDoNotCloseTheConnection separates protocol errors
// (fatal to the stream) from command errors (not fatal). Conflating them means a
// single typo from a client library drops a pooled connection.
func TestUnknownAndMisusedCommandsDoNotCloseTheConnection(t *testing.T) {
	ts := newTestServer(t, 1)
	c := dial(t, ts.addr)

	if got := c.do("SET", "nonnumeric", "abc"); got != "+OK\r\n" {
		t.Fatalf("setup SET = %q", got)
	}

	for _, args := range [][]string{
		{"NOSUCHCOMMAND"},
		{"GET"},                       // too few arguments
		{"GET", "a", "b"},             // too many
		{"INCR", "nonnumeric"},        // value is not an integer
		{"EXPIRE", "k", "abc"},        // unparseable TTL
		{"SET", "k", "v", "BOGUSOPT"}, // unknown option
	} {
		reply := c.do(args...)
		if !strings.HasPrefix(reply, "-") {
			t.Errorf("%v = %q, want an error reply", args, reply)
		}
		// The connection must still work for the next command.
		if got := c.do("PING"); got != "+PONG\r\n" {
			t.Fatalf("connection died after %v: PING = %q", args, got)
		}
	}
}

func TestOversizedRequestIsRejectedNotBuffered(t *testing.T) {
	prev := config.MaxRequestSize
	config.MaxRequestSize = 32 * 1024
	t.Cleanup(func() { config.MaxRequestSize = prev })

	ts := newTestServer(t, 1)
	c := dial(t, ts.addr)

	// Dribble a well-formed but never-ending command in small chunks, reading
	// after each one. This is the shape of the attack the limit exists for: the
	// client never completes a frame, so the server has to keep the partial
	// request buffered, and without a cap it would hold as much as the client
	// cares to send.
	//
	// Chunking also keeps the test deterministic. Dumping the whole payload in
	// one write means the server closes the socket with hundreds of kilobytes
	// still unread, which makes the kernel send an RST -- and an RST discards the
	// error reply that was already sitting in the client's receive buffer.
	// The declared length stays under the decoder's own 512 MB bulk ceiling, so
	// the frame is "incomplete" rather than "malformed" -- otherwise the
	// connection would be closed by the protocol check and this test would pass
	// for the wrong reason.
	c.sendRaw([]byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$268435456\r\n"))

	chunk := bytes.Repeat([]byte("x"), 4096)
	var got []byte
	for i := 0; i < 40 && len(got) == 0; i++ {
		if _, err := c.conn.Write(chunk); err != nil {
			// The server may have closed already; that is a valid outcome too.
			break
		}
		c.conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		buf := make([]byte, 4096)
		n, err := c.conn.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			break
		}
	}

	if len(got) == 0 {
		t.Fatal("the server accepted more than MaxRequestSize without complaint")
	}
	if !strings.HasPrefix(string(got), "-") {
		t.Fatalf("got %q, want an error reply", got)
	}
	if !strings.Contains(string(got), "larger than limit") {
		t.Fatalf("got %q, want a request-too-large error", got)
	}

	// The offending connection is closed; the server itself is fine.
	other := dial(t, ts.addr)
	if got := other.do("PING"); got != "+PONG\r\n" {
		t.Fatalf("PING after an oversized request = %q", got)
	}
}

// ---------------------------------------------------------------------------
// concurrency
// ---------------------------------------------------------------------------

// TestConcurrentClientsDoNotCrossTalk is the strongest available check that
// per-connection buffers really are per-connection. Every client writes and
// reads only its own keys, so any buffer sharing, fd mix-up, or misrouted reply
// shows up as one client reading another's value.
func TestConcurrentClientsDoNotCrossTalk(t *testing.T) {
	ts := newTestServer(t, 1)

	const clients = 50
	const ops = 200

	var wg sync.WaitGroup
	errCh := make(chan string, clients)

	for id := 0; id < clients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", ts.addr, 5*time.Second)
			if err != nil {
				errCh <- fmt.Sprintf("client %d: dial: %v", id, err)
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(60 * time.Second))
			r := bufio.NewReaderSize(conn, 32*1024)

			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("c%d:k%d", id, i)
				val := fmt.Sprintf("c%d:v%d", id, i)

				// Pipeline the pair so both framing paths are under load.
				var out bytes.Buffer
				out.Write(encode("SET", key, val))
				out.Write(encode("GET", key))
				if _, err := conn.Write(out.Bytes()); err != nil {
					errCh <- fmt.Sprintf("client %d op %d: write: %v", id, i, err)
					return
				}

				var sb strings.Builder
				if err := readReply(r, &sb); err != nil {
					errCh <- fmt.Sprintf("client %d op %d: SET reply: %v", id, i, err)
					return
				}
				if sb.String() != "+OK\r\n" {
					errCh <- fmt.Sprintf("client %d op %d: SET = %q", id, i, sb.String())
					return
				}
				sb.Reset()
				if err := readReply(r, &sb); err != nil {
					errCh <- fmt.Sprintf("client %d op %d: GET reply: %v", id, i, err)
					return
				}
				if sb.String() != bulk(val) {
					errCh <- fmt.Sprintf("client %d op %d: GET = %q, want %q "+
						"(replies crossed between connections)",
						id, i, sb.String(), bulk(val))
					return
				}
			}
		}(id)
	}
	wg.Wait()
	close(errCh)

	failures := 0
	for msg := range errCh {
		t.Error(msg)
		failures++
	}
	if failures > 0 {
		return
	}

	// Every key must be present exactly once.
	c := dial(t, ts.addr)
	want := fmt.Sprintf(":%d\r\n", clients*ops)
	if got := c.do("DBSIZE"); got != want {
		t.Fatalf("DBSIZE = %q, want %q", got, want)
	}
}

// TestManyShortLivedConnections is the accept-and-teardown path under churn:
// connections that arrive faster than they are accepted, and peers that vanish
// without a graceful close. A leaked fd or a missed EPOLL_CTL_DEL shows up here.
func TestManyShortLivedConnections(t *testing.T) {
	ts := newTestServer(t, 1)

	const rounds = 300
	for i := 0; i < rounds; i++ {
		conn, err := net.DialTimeout("tcp", ts.addr, 5*time.Second)
		if err != nil {
			t.Fatalf("round %d: dial: %v", i, err)
		}
		switch i % 3 {
		case 0:
			// Close immediately, before sending anything.
		case 1:
			// Send a command and abandon the reply.
			conn.Write(encode("SET", fmt.Sprintf("churn%d", i), "v"))
		case 2:
			// Send half a command and disappear: the server is left holding an
			// incomplete frame it must discard on hangup.
			conn.Write([]byte("*3\r\n$3\r\nSET\r\n$5\r\nchur"))
		}
		conn.Close()
	}

	// The server must still be healthy and the connection gauge must return to
	// zero once every peer is reaped.
	c := dial(t, ts.addr)
	if got := c.do("PING"); got != "+PONG\r\n" {
		t.Fatalf("PING after %d short-lived connections = %q", rounds, got)
	}
	waitForConnections(t, 1)
}

func waitForConnections(t *testing.T, want int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var got int64
	for time.Now().Before(deadline) {
		got = core.KV.Stats().Connections.Load()
		if got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("connected_clients settled at %d, want %d", got, want)
}

// TestConnectionGaugeTracksClients checks the accounting in accept and drop.
// Getting this wrong is harmless to correctness and very visible in INFO, which
// is exactly the kind of bug that survives for a long time.
func TestConnectionGaugeTracksClients(t *testing.T) {
	ts := newTestServer(t, 1)

	const n = 20
	conns := make([]net.Conn, 0, n)
	for i := 0; i < n; i++ {
		conn, err := net.DialTimeout("tcp", ts.addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		// Send something so the server has definitely accepted and registered
		// the socket before we count it.
		conn.Write(encode("PING"))
		buf := make([]byte, 7)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("client %d: %v", i, err)
		}
		conns = append(conns, conn)
	}
	waitForConnections(t, n)

	for _, conn := range conns {
		conn.Close()
	}
	waitForConnections(t, 0)
}

// TestMultipleReactorsServeEveryClient exercises SO_REUSEPORT: several listening
// sockets on one address, with the kernel steering each connection to exactly one
// reactor. The invariant being tested is ownership -- a client is handled by one
// reactor for its whole life -- which is what the two TODOs in the original
// reactor were asking about.
func TestMultipleReactorsServeEveryClient(t *testing.T) {
	ts := newTestServer(t, 4)
	if len(ts.srv.reactors) != 4 {
		t.Fatalf("got %d reactors, want 4", len(ts.srv.reactors))
	}

	const clients = 40
	var wg sync.WaitGroup
	errCh := make(chan string, clients)

	for id := 0; id < clients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", ts.addr, 5*time.Second)
			if err != nil {
				errCh <- fmt.Sprintf("client %d: dial: %v", id, err)
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(60 * time.Second))
			r := bufio.NewReaderSize(conn, 16*1024)

			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("mr%d:%d", id, i)
				if _, err := conn.Write(encode("SET", key, key)); err != nil {
					errCh <- fmt.Sprintf("client %d: write: %v", id, err)
					return
				}
				var sb strings.Builder
				if err := readReply(r, &sb); err != nil {
					errCh <- fmt.Sprintf("client %d: read: %v", id, err)
					return
				}
				if sb.String() != "+OK\r\n" {
					errCh <- fmt.Sprintf("client %d: SET = %q", id, sb.String())
					return
				}
			}
		}(id)
	}
	wg.Wait()
	close(errCh)
	for msg := range errCh {
		t.Error(msg)
	}

	c := dial(t, ts.addr)
	want := fmt.Sprintf(":%d\r\n", clients*100)
	if got := c.do("DBSIZE"); got != want {
		t.Fatalf("DBSIZE = %q, want %q; a reactor lost writes", got, want)
	}
}

// TestServerSurvivesASignalStorm is the EINTR regression test.
//
// The Go runtime preempts goroutines by sending SIGURG, which makes any syscall
// blocked in a thread return EINTR. The original reactor treated every error
// from epoll_wait as fatal, so the server died within milliseconds under load --
// and the project's own README worked around it with GODEBUG=asyncpreemptoff=1.
// This test does the opposite: it deliberately floods the process with SIGURG
// while clients are active, and requires the server to keep serving.
func TestServerSurvivesASignalStorm(t *testing.T) {
	ts := newTestServer(t, 2)

	stop := make(chan struct{})
	var storm sync.WaitGroup
	storm.Add(1)
	go func() {
		defer storm.Done()
		pid := os.Getpid()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// SIGURG is what the runtime itself uses for preemption, so a
			// spurious one is handled and discarded -- but it still interrupts
			// whatever syscall a thread happens to be blocked in, which is
			// precisely the condition being tested.
			syscall.Kill(pid, syscall.SIGURG)
			time.Sleep(100 * time.Microsecond)
		}
	}()

	const clients = 8
	var wg sync.WaitGroup
	errCh := make(chan string, clients)
	deadline := time.Now().Add(2 * time.Second)
	var total int64
	var totalMu sync.Mutex

	for id := 0; id < clients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", ts.addr, 5*time.Second)
			if err != nil {
				errCh <- fmt.Sprintf("client %d: dial: %v", id, err)
				return
			}
			defer conn.Close()
			conn.SetDeadline(deadline.Add(10 * time.Second))
			r := bufio.NewReaderSize(conn, 16*1024)

			n := int64(0)
			for time.Now().Before(deadline) {
				if _, err := conn.Write(encode("INCR", "storm")); err != nil {
					errCh <- fmt.Sprintf("client %d after %d ops: write: %v", id, n, err)
					return
				}
				var sb strings.Builder
				if err := readReply(r, &sb); err != nil {
					errCh <- fmt.Sprintf("client %d after %d ops: read: %v", id, n, err)
					return
				}
				if !strings.HasPrefix(sb.String(), ":") {
					errCh <- fmt.Sprintf("client %d: INCR = %q", id, sb.String())
					return
				}
				n++
			}
			totalMu.Lock()
			total += n
			totalMu.Unlock()
		}(id)
	}
	wg.Wait()
	close(stop)
	storm.Wait()
	close(errCh)

	for msg := range errCh {
		t.Error(msg)
	}
	if t.Failed() {
		return
	}

	// Run must not have returned: the reactors are still alive.
	select {
	case err := <-ts.done:
		ts.done <- err // put it back for the cleanup
		t.Fatalf("the server exited during the signal storm: %v", err)
	default:
	}

	c := dial(t, ts.addr)
	want := fmt.Sprintf(":%d\r\n", total+1)
	if got := c.do("INCR", "storm"); got != want {
		t.Fatalf("INCR = %q, want %q: %d increments were acknowledged but not applied",
			got, want, total)
	}
	t.Logf("%d commands served across %d clients under a SIGURG storm", total, clients)
}

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------

// TestShutdownWakesABlockedReactor is the self-pipe test. A reactor sitting in
// epoll_wait with an infinite timeout cannot observe a flag; without a pipe fd in
// its epoll set it would only notice the shutdown when the next client happened
// to connect.
func TestShutdownWakesABlockedReactor(t *testing.T) {
	ts := newTestServer(t, 3)

	// Make sure every reactor is genuinely idle and blocked in epoll_wait
	// before asking it to stop.
	c := dial(t, ts.addr)
	if got := c.do("PING"); got != "+PONG\r\n" {
		t.Fatalf("PING = %q", got)
	}
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	ts.srv.Shutdown()
	if err := ts.wait(); err != nil {
		t.Fatalf("Run returned %v", err)
	}
	elapsed := time.Since(start)
	t.Logf("3 idle reactors shut down in %v", elapsed)

	if elapsed > 5*time.Second {
		t.Fatalf("shutdown took %v; a reactor was not woken from epoll_wait", elapsed)
	}

	// The listening sockets must be gone, so the port is immediately reusable.
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", ts.addr, 250*time.Millisecond)
		if err != nil {
			break
		}
		conn.Close()
		if time.Now().After(deadline) {
			t.Fatal("the server still accepts connections after Run returned")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	ts := newTestServer(t, 2)
	ts.srv.Shutdown()
	ts.srv.Shutdown()
	ts.srv.Shutdown()
	if err := ts.wait(); err != nil {
		t.Fatalf("Run returned %v", err)
	}
}

func TestNewServerReportsABadAddressInsteadOfLogging(t *testing.T) {
	prevHost, prevPort := config.Host, config.Port
	t.Cleanup(func() { config.Host, config.Port = prevHost, prevPort })

	config.Host = "not-an-ip"
	config.Port = 6379
	if _, err := NewServer(); err == nil {
		t.Fatal("NewServer succeeded with an invalid bind address")
	}

	config.Host = "127.0.0.1"
	config.Port = 1 // privileged: bind must fail for an unprivileged test run
	if os.Geteuid() != 0 {
		if _, err := NewServer(); err == nil {
			t.Fatal("NewServer succeeded binding to port 1 as an unprivileged user")
		}
	}
}

// ---------------------------------------------------------------------------
// command behaviour over the wire
// ---------------------------------------------------------------------------

// TestServerReportsItsOwnStatistics keeps INFO honest: it is the only way an
// operator sees hits, misses, evictions and dropped AOF records, so a wrong
// number there is worse than no number.
func TestServerReportsItsOwnStatistics(t *testing.T) {
	ts := newTestServer(t, 1)
	c := dial(t, ts.addr)

	c.do("SET", "hit", "v")
	for i := 0; i < 10; i++ {
		c.do("GET", "hit") // hits
	}
	for i := 0; i < 5; i++ {
		c.do("GET", "absent") // misses
	}

	info := c.do("INFO")
	for _, want := range []string{
		"keyspace_hits:", "keyspace_misses:", "connected_clients:",
		"total_commands_processed:", "db0:keys=",
	} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO is missing %q", want)
		}
	}
	t.Logf("INFO:\n%s", strings.SplitN(info, "\r\n", 2)[1])
}

// TestExpiryIsObservedOverTheWire covers the read path's lazy expiry and the
// background sweep together, through the same interface a client uses.
func TestExpiryIsObservedOverTheWire(t *testing.T) {
	ts := newTestServer(t, 1)
	c := dial(t, ts.addr)

	if got := c.do("SET", "ephemeral", "v", "PX", "60"); got != "+OK\r\n" {
		t.Fatalf("SET PX = %q", got)
	}
	if got := c.do("GET", "ephemeral"); got != bulk("v") {
		t.Fatalf("GET before expiry = %q", got)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if got := c.do("GET", "ephemeral"); got == "$-1\r\n" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the key never expired")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := c.do("TTL", "ephemeral"); got != ":-2\r\n" {
		t.Fatalf("TTL of an expired key = %q, want :-2", got)
	}
}

// TestKeysWithAPathologicalPatternDoesNotHang covers the glob matcher. A
// recursive implementation backtracks exponentially on a pattern like
// `a*a*a*a*a*b`, so a single KEYS from one client froze the whole server.
func TestKeysWithAPathologicalPatternDoesNotHang(t *testing.T) {
	ts := newTestServer(t, 1)
	c := dial(t, ts.addr)

	c.do("SET", strings.Repeat("a", 64), "v")

	// Run KEYS on its own connection so a hang shows up as a timeout here
	// rather than wedging the connection used for the liveness check. The
	// goroutine reports through channels because t.Fatal is only legal on the
	// test's own goroutine.
	type result struct {
		reply string
		err   error
	}
	done := make(chan result, 1)
	go func() {
		conn, err := net.DialTimeout("tcp", ts.addr, 5*time.Second)
		if err != nil {
			done <- result{err: err}
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(30 * time.Second))
		if _, err := conn.Write(encode("KEYS", strings.Repeat("a*", 20)+"b")); err != nil {
			done <- result{err: err}
			return
		}
		var sb strings.Builder
		if err := readReply(bufio.NewReader(conn), &sb); err != nil {
			done <- result{err: err}
			return
		}
		done <- result{reply: sb.String()}
	}()

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("KEYS: %v", res.err)
		}
		if res.reply != "*0\r\n" {
			t.Fatalf("KEYS = %q, want an empty array", res.reply)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("KEYS with a pathological pattern did not return within 5s")
	}

	if got := c.do("PING"); got != "+PONG\r\n" {
		t.Fatalf("PING after a pathological KEYS = %q", got)
	}
}
