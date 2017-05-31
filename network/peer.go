package network

import (
	"fmt"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/logger"
	"net"
	"strconv"
	"strings"
	"time"
)

var (
	log = logger.GetLogger("network")
)

const (
	maxTokenLen = 20
)

type closeFunc func(net.Conn)

type NetWorkConfig struct {
	NetWork       string
	Token         string
	ListenAddr    string
	ListenTimeout time.Duration
	ReadTimeout   time.Duration
	ServerChanCap int
	DialTimeout   time.Duration
	WriteTimeout  time.Duration
	KeepAlive     time.Duration
	ClientChanCap int
}

func ResolveAddr(network, addr string) (net.Addr, error) {
	switch network {
	case "", "tcp":
		return net.ResolveTCPAddr(network, addr)
	default:
		return nil, net.UnknownNetworkError(network)
	}
}

func AddrToUint64(ip string, port int) (uint64, error) {
	var n uint64
	s := strings.Split(ip, ".")
	if len(s) != 4 {
		return 0, fmt.Errorf("network: unrecognize ip '%s'", ip)
	}
	b0, err := strconv.Atoi(s[0])
	if err != nil {
		return n, fmt.Errorf("network: convert '%s' to node id: %v", ip, err)
	}
	b1, err := strconv.Atoi(s[1])
	if err != nil {
		return n, fmt.Errorf("network: convert '%s' to node id: %v", ip, err)
	}
	b2, err := strconv.Atoi(s[2])
	if err != nil {
		return n, fmt.Errorf("network: convert '%s' to node id: %v", ip, err)
	}
	b3, err := strconv.Atoi(s[3])
	if err != nil {
		return n, fmt.Errorf("network: convert '%s' to node id: %v", ip, err)
	}

	n |= uint64(b0) << 24
	n |= uint64(b1) << 16
	n |= uint64(b2) << 8
	n |= uint64(b3)

	return n<<16 | uint64(port), nil
}

func Uint64ToAddr(i uint64) string {
	var b [4]byte
	p := i & 0xffff
	b[0] = byte((i >> 16) & 0xff)
	b[1] = byte((i >> 24) & 0xff)
	b[2] = byte((i >> 32) & 0xff)
	b[3] = byte(i >> 40)
	return fmt.Sprintf("%v.%v.%v.%v:%d", b[3], b[2], b[1], b[0], p)
}

func (cfg *NetWorkConfig) validate() error {
	switch cfg.NetWork {
	case "tcp":
	default:
		return net.UnknownNetworkError(cfg.NetWork)
	}

	if len(cfg.Token) > maxTokenLen {
		return fmt.Errorf("network: token %s, length should less than %d", cfg.Token, maxTokenLen)
	}
	setToken(cfg.Token)
	return nil
}

func NewPeerNetWork(cfg NetWorkConfig, receiver comm.Receiver) (comm.NetWork, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	network := &tcpPeerNetWork{
		cfg: &cfg,
	}
	var err error
	switch cfg.NetWork {
	case "tcp":
		network.server, err = newTcpPeerServer(receiver, cfg.ListenAddr, cfg.ListenTimeout, cfg.ReadTimeout, cfg.ServerChanCap)
		if err != nil {
			return nil, err
		}
		network.client = newTcpPeerClient(cfg.DialTimeout, cfg.KeepAlive, cfg.WriteTimeout, cfg.ClientChanCap)
	}
	return network, nil
}

type tcpPeerNetWork struct {
	cfg    *NetWorkConfig
	server *tcpPeerServer
	client *tcpPeerClient
}

func (t *tcpPeerNetWork) Start(stopped <-chan struct{}) error {
	t.server.start()
	log.Debug("Tcp peer network started.")
	return nil
}

func (t *tcpPeerNetWork) StopServer() {
	if t.server != nil {
		t.server.Stop()
		t.server = nil
	}
}

func (t *tcpPeerNetWork) StopClient() {
	if t.client != nil {
		t.client.Stop()
		t.client = nil
	}
}

func (t *tcpPeerNetWork) Stop() {
	if t.server != nil {
		t.server.Stop()
		t.server = nil
	}
	if t.client != nil {
		t.client.Stop()
		t.client = nil
	}
	log.Debug("Tcp peer network stopped.")
}

func (t *tcpPeerNetWork) SendMessage(addr string, message []byte) error {
	return t.client.sendMessage(addr, message)
}
