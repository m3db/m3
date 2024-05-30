// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Package server implements a network server.
package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	xnet "github.com/m3db/m3/src/x/net"
	"github.com/m3db/m3/src/x/retry"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// Server is a server capable of listening to incoming traffic and closing itself
// when it's shut down.
type Server interface {
	// ListenAndServe forever listens to new incoming connections and
	// handles data from those connections.
	ListenAndServe() error

	// Serve accepts and handles incoming connections on the listener l forever.
	Serve(l net.Listener) error

	// Close closes the server.
	Close()
}

// Handler can handle the data received on connection.
// It's used in Server once a connection was established.
type Handler interface {
	// Handle handles the data received on the connection, this function
	// should be blocking until the connection is closed or received error.
	Handle(conn net.Conn)

	// Close closes the handler.
	Close()
}

type serverMetrics struct {
	openConnections tally.Gauge
}

func newServerMetrics(scope tally.Scope) serverMetrics {
	return serverMetrics{
		openConnections: scope.Gauge("open-connections"),
	}
}

type addConnectionFn func(conn net.Conn) bool
type removeConnectionFn func(conn net.Conn)

type server struct {
	sync.Mutex

	address                      string
	listener                     net.Listener
	log                          *zap.Logger
	retryOpts                    retry.Options
	reportInterval               time.Duration
	tcpConnectionKeepAlive       bool
	tcpConnectionKeepAlivePeriod time.Duration

	closed       bool
	closedChan   chan struct{}
	numConns     int32
	conns        []net.Conn
	wgConns      sync.WaitGroup
	metrics      serverMetrics
	handler      Handler
	listenerOpts xnet.ListenerOptions
	tlsOpts      TLSOptions
	certPool     *x509.CertPool

	addConnectionFn    addConnectionFn
	removeConnectionFn removeConnectionFn
}

// NewServer creates a new server.
func NewServer(address string, handler Handler, opts Options) Server {
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()

	s := &server{
		address:                      address,
		log:                          instrumentOpts.Logger(),
		retryOpts:                    opts.RetryOptions(),
		reportInterval:               instrumentOpts.ReportInterval(),
		tcpConnectionKeepAlive:       opts.TCPConnectionKeepAlive(),
		tcpConnectionKeepAlivePeriod: opts.TCPConnectionKeepAlivePeriod(),
		closedChan:                   make(chan struct{}),
		metrics:                      newServerMetrics(scope),
		handler:                      handler,
		listenerOpts:                 opts.ListenerOptions(),
		tlsOpts:                      opts.TLSOptions(),
		certPool:                     x509.NewCertPool(),
	}

	// Set up the connection functions.
	s.addConnectionFn = s.addConnection
	s.removeConnectionFn = s.removeConnection

	// Start reporting metrics.
	go s.reportMetrics()

	return s
}

func (s *server) ListenAndServe() error {
	listener, err := s.listenerOpts.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	return s.Serve(listener)
}

func (s *server) Serve(l net.Listener) error {
	s.address = l.Addr().String()
	s.listener = l
	go s.serve()
	return nil
}

func (s *server) getTLSConfig() (*tls.Config, error) {
	if s.tlsOpts.ClientCAFile() != "" {
		certs, err := os.ReadFile(s.tlsOpts.ClientCAFile())
		if err != nil {
			return nil, fmt.Errorf("read bundle error: %w", err)
		}
		if ok := s.certPool.AppendCertsFromPEM(certs); !ok {
			return nil, fmt.Errorf("cannot append cert to cert pool")
		}
	}
	clientAuthType := tls.NoClientCert
	if s.tlsOpts.MutualTLSEnabled() {
		clientAuthType = tls.RequireAndVerifyClientCert
	}
	tlsConfig := &tls.Config{
		ClientCAs: s.certPool,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(s.tlsOpts.CertFile(), s.tlsOpts.KeyFile())
			if err != nil {
				return nil, fmt.Errorf("load x509 key pair error: %w", err)
			}
			return &cert, nil
		},
		ClientAuth: clientAuthType,
	}
	return tlsConfig, nil
}

func (s *server) maybeUpgradeToTLS(conn SecuredConn) (SecuredConn, error) {
	if s.tlsOpts.Mode() == TLSDisabled {
		return conn, nil
	}
	isTLSConnection, err := conn.IsTLS()
	if err != nil {
		return nil, err
	}
	if isTLSConnection {
		tlsConfig, err := s.getTLSConfig()
		if err != nil {
			return nil, err
		}
		conn = conn.UpgradeToTLS(tlsConfig)
		if err != nil {
			return nil, err
		}
	} else if s.tlsOpts.Mode() == TLSEnforced {
		return nil, fmt.Errorf("not a tls connection")
	}
	return conn, nil
}

func (s *server) serve() {
	connCh, errCh := xnet.StartForeverAcceptLoop(s.listener, s.retryOpts)
	for conn := range connCh {
		conn := newSecuredConn(conn)
		if tcpConn, ok := conn.GetConn().(*net.TCPConn); ok {
			tcpConn.SetKeepAlive(s.tcpConnectionKeepAlive)
			if s.tcpConnectionKeepAlivePeriod != 0 {
				tcpConn.SetKeepAlivePeriod(s.tcpConnectionKeepAlivePeriod)
			}
		}
		if !s.addConnectionFn(conn) {
			conn.Close()
		} else {
			s.wgConns.Add(1)
			go func() {
				defer conn.Close()
				defer s.removeConnectionFn(conn)
				defer s.wgConns.Done()

				securedConn, err := s.maybeUpgradeToTLS(conn)
				if err != nil {
					return
				}
				s.handler.Handle(securedConn)
			}()
		}
	}
	err := <-errCh
	s.log.Error("server unexpectedly closed", zap.Error(err))
}

func (s *server) Close() {
	s.Lock()
	if s.closed {
		s.Unlock()
		return
	}
	s.closed = true

	close(s.closedChan)
	openConns := make([]net.Conn, len(s.conns))
	copy(openConns, s.conns)
	s.Unlock()

	// Close all open connections.
	for _, conn := range openConns {
		conn.Close()
	}

	// Close the listener.
	if s.listener != nil {
		s.listener.Close()
	}

	// Wait for all connection handlers to finish.
	s.wgConns.Wait()

	// Close the handler.
	s.handler.Close()
}

func (s *server) addConnection(conn net.Conn) bool {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return false
	}
	s.conns = append(s.conns, conn)
	atomic.AddInt32(&s.numConns, 1)
	return true
}

func (s *server) removeConnection(conn net.Conn) {
	s.Lock()
	defer s.Unlock()

	numConns := len(s.conns)
	for i := 0; i < numConns; i++ {
		if s.conns[i] == conn {
			// Move the last connection to i and reduce the number of connections by 1.
			s.conns[i] = s.conns[numConns-1]
			s.conns = s.conns[:numConns-1]
			atomic.AddInt32(&s.numConns, -1)
			return
		}
	}
}

func (s *server) reportMetrics() {
	t := time.NewTicker(s.reportInterval)

	for {
		select {
		case <-t.C:
			s.metrics.openConnections.Update(float64(atomic.LoadInt32(&s.numConns)))
		case <-s.closedChan:
			t.Stop()
			return
		}
	}
}
