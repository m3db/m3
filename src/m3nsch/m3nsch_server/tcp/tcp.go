package tcp

import (
	"net"
	"time"
)

// NewTCPListener is Listener specifically for TCP
func NewTCPListener(listenAddress string, keepAlivePeriod time.Duration) (net.Listener, error) {
	l, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return nil, err
	}
	return tcpKeepAliveListener{l.(*net.TCPListener), keepAlivePeriod}, err
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away. This is cargo culted from http/server.go
type tcpKeepAliveListener struct {
	*net.TCPListener
	keepAlivePeriod time.Duration
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(ln.keepAlivePeriod)
	return tc, nil
}
