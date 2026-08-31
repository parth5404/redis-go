package server

import (
	"errors"
	"fmt"
	"github/redis.go/config"
	"github/redis.go/core"
	"log"
	"net"
	"syscall"
	"time"
)

// maxClients sizes both the listen backlog and the epoll event buffer, so a
// burst of connections is absorbed by the kernel rather than refused.
const maxClients = 20_000

var events []syscall.EpollEvent = make([]syscall.EpollEvent, maxClients)
var cronFrequency time.Duration = 1 * time.Second

// listenAddr resolves the configured host and port into a sockaddr.
//
// This is built here rather than in a package-level var on purpose: package
// initialisation runs before main calls flag.Parse, so a var would capture the
// compiled-in defaults and every --host/--port flag would be silently ignored.
func listenAddr() (*syscall.SockaddrInet4, error) {
	ip := net.ParseIP(config.Host)
	if ip == nil {
		return nil, fmt.Errorf("invalid host %q", config.Host)
	}
	ipv4 := ip.To4()
	if ipv4 == nil {
		return nil, fmt.Errorf("host %q is not an IPv4 address", config.Host)
	}
	return &syscall.SockaddrInet4{
		Port: config.Port,
		Addr: [4]byte{ipv4[0], ipv4[1], ipv4[2], ipv4[3]},
	}, nil
}

func RunAsyncTCP() error {
	serverSockaddr, err := listenAddr()
	if err != nil {
		log.Print(err)
		return err
	}

	serverFd, err := syscall.Socket(syscall.AF_INET, syscall.O_NONBLOCK|syscall.SOCK_STREAM, 0)
	if err != nil {
		log.Print(err)
		return err
	}
	defer syscall.Close(serverFd)

	// Without SO_REUSEADDR a restart fails for as long as the previous
	// listener's sockets sit in TIME_WAIT.
	if err = syscall.SetsockoptInt(serverFd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		log.Print(err.Error())
		return err
	}
	if err = syscall.Bind(serverFd, serverSockaddr); err != nil {
		log.Printf("bind %s:%d: %v", config.Host, config.Port, err)
		return err
	}
	if err = syscall.Listen(serverFd, maxClients); err != nil {
		log.Print(err.Error())
		return err
	}
	epfd, err := syscall.EpollCreate1(0)
	if err != nil {
		log.Print(err.Error())
		return err
	}
	if err = syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, serverFd, &syscall.EpollEvent{
		Events: syscall.EPOLLIN,
		Fd:     int32(serverFd),
	}); err != nil {
		log.Print(err.Error())
		return err
	}
	log.Printf("redis-go listening on %s:%d", config.Host, config.Port)

	// Run the expiration job in a dedicated background goroutine
	go func() {
		log.Println("Started the expiry go routine")
		ticker := time.NewTicker(cronFrequency)
		for range ticker.C {
			core.DelExpireKeys()
		}
	}()

	for {
		n, err := syscall.EpollWait(epfd, events[:], -1)
		if err != nil {
			// The Go runtime preempts goroutines by delivering SIGURG, which
			// interrupts this blocking wait and surfaces as EINTR. It means
			// "a signal arrived", not "the loop failed" — treating it as fatal
			// killed the server at random under sustained load.
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			log.Print(err.Error())
			return err
		}
		for i := 0; i < n; i++ {
			if events[i].Fd == int32(serverFd) {
				clientfd, _, err := syscall.Accept4(serverFd, 0)
				if err != nil {
					// The listener is non-blocking, so a spurious wake-up
					// reports EAGAIN with no connection waiting.
					if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EINTR) {
						continue
					}
					log.Println("accept:", err)
					continue
				}
				con_clients++
				//todo
				//assigne this new client to an seprate IO thread
				if err = syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, clientfd, &syscall.EpollEvent{
					Events: syscall.EPOLLIN,
					Fd:     int32(clientfd),
				}); err != nil {
					log.Print(err.Error())
					return err
				}
			} else {
				comm := core.FDComm{Fd: int(events[i].Fd)}
				///todo
				//same I/O thread for read cmd
				cmds, err := readCmds(&comm)
				if err != nil {
					syscall.Close(int(events[i].Fd))
					con_clients -= 1
					continue
				}
				//single threaded response making
				respond(&comm, cmds)
			}

		}
	}
}
