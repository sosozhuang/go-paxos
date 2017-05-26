package network

import (
	"errors"
	"net"
	"sync"
	"time"
)

type tcpPeerClient struct {
	conns        map[string]*tcpClientConn
	mu           sync.RWMutex
	wg           sync.WaitGroup
	timeout      time.Duration
	writeTimeout time.Duration
	keepAlive    time.Duration
	chTimeout    time.Duration
	cap          int
}

func newTcpPeerClient(timeout, keepAlive, writeTimeout time.Duration, cap int) *tcpPeerClient {
	return &tcpPeerClient{
		conns:        make(map[string]*tcpClientConn),
		timeout:      timeout,
		writeTimeout: writeTimeout,
		keepAlive:    keepAlive,
		chTimeout:    time.Second * 3,
		cap:          cap,
	}
}

func (t *tcpPeerClient) createClientConn(addr string) (*tcpClientConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if conn, ok := t.conns[addr]; ok {
		return conn, nil
	}

	conn, err := newTcpClientConn(addr, t.timeout, t.writeTimeout, t.keepAlive, t.chTimeout, t.cap)
	if err != nil {
		return nil, err
	}
	conn.closeFunc = func(net.Conn) {
		t.mu.Lock()
		defer func() {
			t.mu.Unlock()
			t.wg.Done()
		}()
		delete(t.conns, addr)
	}
	t.conns[addr] = conn
	return conn, nil
}

func (t *tcpPeerClient) sendMessage(addr string, msg []byte) (err error) {
	conn, ok := t.getClientConn(addr)
	if !ok {
		conn, err = t.createClientConn(addr)
		if err != nil {
			log.Error(err)
			return
		}
		t.wg.Add(1)
		go conn.handleWrite()
	}

	return conn.sendMessage(pack(msg))

}

func (t *tcpPeerClient) getClientConn(addr string) (conn *tcpClientConn, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	conn, ok = t.conns[addr]
	return
}

//func (t *tcpPeerClient) Start() {
//	var err error
//	for r := range t.ch {
//		conn, ok := t.getClientConn(r.addr)
//		if ! ok {
//			conn, err = t.createClientConn(r.addr)
//			if err != nil {
//				log.Error(err)
//				continue
//			}
//			go conn.handleWrite()
//		}
//
//		if err = conn.sendMessage(r.message); err != nil {
//			log.Error(err)
//		}
//	}
//}

func (t *tcpPeerClient) Stop() {
	//close(t.ch)
	t.mu.RLock()
	conns := make(map[string]*tcpClientConn, len(t.conns))
	for k, v := range t.conns {
		conns[k] = v
	}
	t.mu.RUnlock()
	for _, conn := range conns {
		if conn != nil {
			conn.close()
		}
	}
	t.wg.Wait()
}

type tcpClientConn struct {
	addr         string
	conn         net.Conn
	timeout      time.Duration
	writeTimeout time.Duration
	keepAlive    time.Duration
	chTimeout    time.Duration
	ch           chan []byte
	closeFunc
}

func newTcpClientConn(addr string, timeout, writeTimeout, keepAlive, chTimeout time.Duration, cap int) (c *tcpClientConn, err error) {
	c = &tcpClientConn{
		addr:         addr,
		timeout:      timeout,
		writeTimeout: writeTimeout,
		keepAlive:    keepAlive,
		chTimeout:    chTimeout,
		ch:           make(chan []byte, cap),
	}
	defer func() {
		if err != nil {
			c.close()
			c = nil
		}
	}()

	err = c.createConn()
	return
}

func (t *tcpClientConn) createConn() error {
	if t.conn != nil {
		if err := t.conn.Close(); err != nil {
			log.Error(err)
		}
	}
	d := net.Dialer{
		Timeout:   t.timeout,
		KeepAlive: t.keepAlive,
	}

	c, err := d.Dial("tcp", t.addr)
	if err != nil {
		return err
	}
	c.(*net.TCPConn).SetKeepAlive(true)
	t.conn = c
	return nil
}

func (t *tcpClientConn) sendMessage(msg []byte) error {
	select {
	case t.ch <- msg:
		return nil
	case <-time.After(t.chTimeout):
		return errors.New("time out")
	}
}

func (t *tcpClientConn) handleWrite() {
	defer func() {
		if t.conn != nil {
			if err := t.conn.Close(); err != nil {
				log.Error(err)
			}
		}
		if t.closeFunc != nil {
			t.closeFunc(t.conn)
		}
		t.conn = nil
	}()
	for msg := range t.ch {
		t.conn.SetWriteDeadline(time.Now().Add(t.writeTimeout))
		_, err := t.conn.Write(msg)
		if err != nil {
			if err = t.createConn(); err != nil {
				log.Error(err)
				break
			}
		}
	}
}

func (t *tcpClientConn) close() {
	close(t.ch)
}
