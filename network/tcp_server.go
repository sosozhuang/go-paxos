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
	stopped chan struct{}
}

func newTcpPeerServer(receiver comm.Receiver, addr string, timeout, readTimeout time.Duration, cap int) (*tcpPeerServer, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("peer server: resolver %s: %v", addr, err)
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, fmt.Errorf("peer server: listen %v: %v", tcpAddr, err)
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

func (t *tcpPeerServer) start() {
	t.stopped = make(chan struct{})
	go t.handleMessage()
	go t.accept()
	log.Debug("Tcp peer server started.")
}

func (t *tcpPeerServer) accept() {
	t.wg.Add(1)
	defer t.stop()
	f := func(net.Conn) { t.wg.Done() }
	delay := time.Millisecond * 5
	for {
		select {
		case <-t.stopped:
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
			log.Errorf("Tcp peer server accept connetions error: %v.", err)
		}
		t.wg.Add(1)
		sc := newTcpServerConn(conn, t.readTimeout, t.ch, f)
		//go sc.receiveMessage()
		go sc.handleRead(t.stopped)
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
			log.Errorf("Tcp peer server close listener error: %v.", err)
		}
		t.listener = nil
	}
	t.wg.Done()
}

func (t *tcpPeerServer) Stop() {
	if t.stopped != nil {
		close(t.stopped)
	}
	t.wg.Wait()
	close(t.ch)
	log.Debug("Tcp peer server stopped.")
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
				log.Errorf("Tcp peer server connection read error: %v.", err)
			}
			if n > 0 {
				in = unpack(append(in, b[:n]...), t.ch)
			}
			return
		}

		in = unpack(append(in, b[:n]...), t.ch)

	}

}

func (t *tcpServerConn) close() {
	if t.conn != nil {
		if err := t.conn.Close(); err != nil {
			log.Errorf("Tcp peer server connection close error: %v.", err)
		}
		t.conn = nil
	}
	//close(t.ch)
	if t.closeFunc != nil {
		t.closeFunc(t.conn)
	}
}
