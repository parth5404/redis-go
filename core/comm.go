package core

import (
	"errors"
	"syscall"
)

// FDComm adapts a raw file descriptor to io.ReadWriter so the evaluator can
// write replies straight to the socket, with no net.Conn in the hot path.
type FDComm struct {
	Fd int
}

// Read and Write both retry on EINTR. The Go runtime preempts goroutines with
// SIGURG, which interrupts a blocking syscall and surfaces as EINTR; treating
// that as a read error would drop a perfectly healthy client connection.

func (f *FDComm) Read(b []byte) (int, error) {
	for {
		n, err := syscall.Read(f.Fd, b)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return n, err
	}
}

func (f *FDComm) Write(b []byte) (int, error) {
	// A short write is possible on a socket, so keep going until the whole
	// reply has been handed to the kernel.
	written := 0
	for written < len(b) {
		n, err := syscall.Write(f.Fd, b[written:])
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return written, err
		}
		if n <= 0 {
			return written, syscall.EIO
		}
		written += n
	}
	return written, nil
}
