package server

import (
	"github/redis.go/config"
	"github/redis.go/core"
	"log"
	"net"
	"syscall"
	"time"
)

var ipv4 net.IP = net.ParseIP(config.Host)
var serverSockaddr *syscall.SockaddrInet4 = &syscall.SockaddrInet4{
	Port: config.Port,
	Addr: [4]byte{ipv4[0], ipv4[1], ipv4[2], ipv4[3]},
}
var events []syscall.EpollEvent = make([]syscall.EpollEvent, 20_000)
var cronFrequency time.Duration = 1 * time.Second
var lastCronExecTime time.Time = time.Now()
var EPOLLIN uint32 = 1 << 31

func RunAsyncTCP() error {

	serverFd, err := syscall.Socket(syscall.AF_INET, syscall.O_NONBLOCK|syscall.SOCK_STREAM, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer syscall.Close(serverFd)
	if err = syscall.Bind(serverFd, serverSockaddr); err != nil {
		log.Print(err.Error())
		return err
	}
	if err = syscall.Listen(serverFd, 20_000); err != nil {
		log.Print(err.Error())
		return err
	}
	epfd, err := syscall.EpollCreate1(0)
	if err != nil {
		log.Print(err.Error())
		return err
	}
	if err = syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, serverFd, &syscall.EpollEvent{
		Events: syscall.EPOLLIN | EPOLLIN,
		Fd:     int32(serverFd),
	}); err != nil {
		log.Print(err.Error())
		return err
	}
	log.Println("Server Started")
	for {
		if time.Now().After(lastCronExecTime.Add(cronFrequency)) {
			core.DelExpireKeys()
			lastCronExecTime = time.Now()
		}
		n, err := syscall.EpollWait(epfd, events[:], -1)
		if err != nil {
			log.Print(err.Error())
			return err
		}
		for i := 0; i < n; i++ {
			if events[i].Fd == int32(serverFd) {
				for {
					clientfd, _, err := syscall.Accept4(serverFd, syscall.O_NONBLOCK)
					if err != nil {
						// Condition 1: End of Queue
						if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
							break
						}

						// Condition 2: Middle Client Lost after event creation and before TCP handshake
						if err == syscall.ECONNABORTED || err == syscall.EPROTO {
							continue 
						}

						// Condition 3: ANy other Unknown error
						log.Println("Fatal error accepting connection:", err)
						break
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
