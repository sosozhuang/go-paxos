package network

import (
	"github.com/sosozhuang/paxos/comm"
	"io"
	"net"
	"sync"
	"time"
	"fmt"
)

type tcpPeerServer struct {
	//addr        string
	receiver    comm.Receiver
	timeout     time.Duration
	readTimeout time.Duration
	//cap         int
	listener *net.TCPListener
	wg       sync.WaitGroup
	ch       chan []byte
}

func newTcpPeerServer(receiver comm.Receiver, addr string, timeout, readTimeout time.Duration, cap int) (*tcpPeerServer, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tcp peer server resolver address error: %s", err)
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, fmt.Errorf("tcp peer server listen address error: %s", err)
	}
	return &tcpPeerServer{
		//addr: addr,
		receiver:    receiver,
		timeout:     timeout,
		readTimeout: readTimeout,
		listener:    ln,
		ch:          make(chan []byte, cap),
	}, nil
}

func (t *tcpPeerServer) Start(stopped <-chan struct{}) {
	go t.handleMessage()
	go t.accept(stopped)
}

func (t *tcpPeerServer) accept(stopped <-chan struct{}) {
	t.wg.Add(1)
	defer t.stop()
	f := func(net.Conn) { t.wg.Done() }
	delay := time.Millisecond * 5
	for {
		select {
		case <-stopped:
			return
		default:
		}
		t.listener.SetDeadline(time.Now().Add(t.timeout))
		conn, err := t.listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			} else if ok && opErr.Temporary() {
				if delay < time.Second {
					delay *= 2
				}
				time.Sleep(delay)
				continue
			}
			log.Error("", err)
		}
		t.wg.Add(1)
		sc := newTcpServerConn(conn, t.readTimeout, t.ch, f)
		//go sc.receiveMessage()
		go sc.handleRead(stopped)
		delay = time.Millisecond * 5
	}
}

func (t *tcpPeerServer) handleMessage() {
	for msg := range t.ch {
		t.receiver.ReceiveMessage(msg)
	}
}

func (t *tcpPeerServer) stop() {
	if t.listener != nil {
		if err := t.listener.Close(); err != nil {
			log.Error("", err)
		}
		t.listener = nil
	}
	t.wg.Done()
}

func (t *tcpPeerServer) Stop() {
	t.wg.Wait()
	close(t.ch)
}

type tcpServerConn struct {
	conn    net.Conn
	timeout time.Duration
	ch      chan<- []byte
	closeFunc
}

func newTcpServerConn(conn net.Conn, timeout time.Duration, ch chan<- []byte, f closeFunc) *tcpServerConn {
	return &tcpServerConn{
		conn:      conn,
		timeout:   timeout,
		ch:        ch,
		closeFunc: f,
	}
}

func (t *tcpServerConn) handleRead(stopped <-chan struct{}) {
	defer t.close()
	in := make([]byte, 0)
	b := make([]byte, 1024)
	for {
		select {
		case <-stopped:
			return
		default:
		}
		t.conn.SetReadDeadline(time.Now().Add(t.timeout))
		n, err := t.conn.Read(b)
		if err != nil {
			if err != io.EOF {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue
				}
			}
			if n > 0 {
				in = unpack(append(in, b[:n]...), t.ch)
			}
			log.Error(err)
			return
		}

		in = unpack(append(in, b[:n]...), t.ch)

	}

}

//func (t *tcpServerConn) receiveMessage() {
//	for msg := range t.ch {
//		t.Node.ReceiveMessage(msg)
//	}
//}

func (t *tcpServerConn) close() {
	if t.conn != nil {
		if err := t.conn.Close(); err != nil {
			log.Error(err)
		}
		t.conn = nil
	}
	//close(t.ch)
	if t.closeFunc != nil {
		t.closeFunc(t.conn)
	}
}
