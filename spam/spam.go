// Command loadgen is a self-contained RESP load generator.
//
// It exists because the benchmark numbers in the README have to be reproducible
// on a machine that does not have redis-benchmark installed, and because
// measuring a pipelined workload correctly requires keeping one outstanding
// batch per connection rather than one command at a time.
//
// Usage:
//
//	go run ./spam -addr localhost:7378 -conns 50 -pipeline 8 -duration 10s
//
// The previous version of this file was a "spam" utility that slept 500 ms
// between commands, printed every command to stdout, and built requests with
// strings.Split(fmt.Sprintf("SET %s %d", ...), " ") -- so it could not generate
// meaningful load and could not send a value containing a space. It also had
// unreachable code after an infinite loop, which `go vet` flagged.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	addr      = flag.String("addr", "localhost:7378", "server address")
	conns     = flag.Int("conns", 50, "concurrent connections")
	pipeline  = flag.Int("pipeline", 1, "commands per batch (pipeline depth)")
	duration  = flag.Duration("duration", 10*time.Second, "how long to run")
	keyspace  = flag.Int("keyspace", 100_000, "distinct keys to touch")
	valueSize = flag.Int("value-size", 16, "value length in bytes for SET")
	getRatio  = flag.Float64("get-ratio", 0.5, "fraction of commands that are GET")
	warmup    = flag.Duration("warmup", 1*time.Second, "discard samples for this long")
	label     = flag.String("label", "", "label printed with the results")
)

// result is one connection's contribution to the totals.
type result struct {
	batches   int64
	commands  int64
	errors    int64
	latencies []time.Duration
}

var (
	stop     atomic.Bool
	measured atomic.Bool
)

func main() {
	flag.Parse()
	log.SetFlags(0)

	if *pipeline < 1 {
		log.Fatal("-pipeline must be >= 1")
	}
	if *conns < 1 {
		log.Fatal("-conns must be >= 1")
	}

	// Fail fast on an unreachable server rather than reporting zero throughput.
	probe, err := net.DialTimeout("tcp", *addr, 2*time.Second)
	if err != nil {
		log.Fatalf("cannot reach %s: %v", *addr, err)
	}
	probe.Close()

	results := make([]result, *conns)
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Each connection gets its own PRNG so no lock is shared and the
			// generator itself never becomes the bottleneck.
			rng := rand.New(rand.NewSource(int64(i)*2862933555777941757 + 1))
			runConn(i, rng, &results[i])
		}(i)
	}

	// Warm-up excludes the first interval from the latency samples: the first
	// requests on a fresh connection pay for TCP slow start, and on the server
	// side for the first allocation of every per-connection buffer.
	go func() {
		time.Sleep(*warmup)
		measured.Store(true)
		time.Sleep(*duration)
		stop.Store(true)
	}()

	wg.Wait()
	report(time.Since(start)-*warmup, results)
}

// runConn drives one connection until the deadline.
func runConn(id int, rng *rand.Rand, res *result) {
	c, err := net.Dial("tcp", *addr)
	if err != nil {
		res.errors++
		return
	}
	defer c.Close()

	if tcp, ok := c.(*net.TCPConn); ok {
		// Without this the client's own Nagle delay dominates the measurement
		// and every result looks like ~40 ms regardless of the server.
		tcp.SetNoDelay(true)
	}

	br := bufio.NewReaderSize(c, 64*1024)
	value := strings.Repeat("x", *valueSize)

	// Pre-size the sample slice; growing it mid-run would attribute allocation
	// time to the server.
	res.latencies = make([]time.Duration, 0, 1<<16)

	req := make([]byte, 0, 4096)

	for !stop.Load() {
		req = req[:0]
		expect := 0
		for i := 0; i < *pipeline; i++ {
			k := "key:" + strconv.Itoa(rng.Intn(*keyspace))
			if rng.Float64() < *getRatio {
				req = appendCommand(req, "GET", k)
			} else {
				req = appendCommand(req, "SET", k, value)
			}
			expect++
		}

		sent := time.Now()
		if _, err := c.Write(req); err != nil {
			res.errors++
			return
		}
		if err := readReplies(br, expect); err != nil {
			res.errors++
			return
		}
		elapsed := time.Since(sent)

		if measured.Load() {
			res.batches++
			res.commands += int64(expect)
			if len(res.latencies) < cap(res.latencies) {
				res.latencies = append(res.latencies, elapsed)
			}
		}
	}
}

// appendCommand writes a RESP array. Length-prefixed rather than
// space-separated, so a value containing spaces or binary data is sent exactly.
func appendCommand(dst []byte, tokens ...string) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(tokens)), 10)
	dst = append(dst, '\r', '\n')
	for _, t := range tokens {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(t)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, t...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

// readReplies consumes exactly n RESP replies.
//
// Counting replies is what makes a pipelined measurement honest: reading "some
// bytes" and calling the batch done would measure the network, not the server.
func readReplies(br *bufio.Reader, n int) error {
	for i := 0; i < n; i++ {
		if err := readReply(br); err != nil {
			return err
		}
	}
	return nil
}

func readReply(br *bufio.Reader) error {
	line, err := br.ReadString('\n')
	if err != nil {
		return err
	}
	if len(line) < 3 {
		return fmt.Errorf("short reply %q", line)
	}
	switch line[0] {
	case '+', '-', ':':
		return nil
	case '$':
		n, err := strconv.Atoi(strings.TrimRight(line[1:], "\r\n"))
		if err != nil {
			return err
		}
		if n < 0 {
			return nil // null bulk string
		}
		// +2 for the trailing CRLF.
		if _, err := br.Discard(n + 2); err != nil {
			return err
		}
		return nil
	case '*':
		n, err := strconv.Atoi(strings.TrimRight(line[1:], "\r\n"))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := readReply(br); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unexpected reply type %q", line[0])
	}
}

// report prints throughput and the latency distribution.
func report(elapsed time.Duration, results []result) {
	var totalCmds, totalBatches, totalErrors int64
	var all []time.Duration
	for i := range results {
		totalCmds += results[i].commands
		totalBatches += results[i].batches
		totalErrors += results[i].errors
		all = append(all, results[i].latencies...)
	}

	if totalCmds == 0 {
		log.Printf("no commands completed (%d connection errors)", totalErrors)
		os.Exit(1)
	}

	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		i := int(p / 100 * float64(len(all)))
		if i >= len(all) {
			i = len(all) - 1
		}
		return all[i]
	}

	name := *label
	if name == "" {
		name = fmt.Sprintf("conns=%d pipeline=%d", *conns, *pipeline)
	}

	fmt.Printf("--- %s ---\n", name)
	fmt.Printf("duration        : %.2fs\n", elapsed.Seconds())
	fmt.Printf("connections     : %d\n", *conns)
	fmt.Printf("pipeline depth  : %d\n", *pipeline)
	fmt.Printf("commands        : %d\n", totalCmds)
	fmt.Printf("errors          : %d\n", totalErrors)
	fmt.Printf("throughput      : %.0f ops/sec\n", float64(totalCmds)/elapsed.Seconds())
	// Batch latency, not per-command latency: with a pipeline of N the client
	// waits once for N replies, so dividing by N would invent a number no
	// request actually experienced.
	fmt.Printf("batch latency   : p50=%v p95=%v p99=%v max=%v\n",
		round(pct(50)), round(pct(95)), round(pct(99)), round(pct(99.99)))
	fmt.Println()
}

func round(d time.Duration) time.Duration {
	if d > time.Millisecond {
		return d.Round(10 * time.Microsecond)
	}
	return d.Round(time.Microsecond)
}
