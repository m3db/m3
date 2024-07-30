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

package server

import (
	"crypto/tls"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/m3db/m3/src/x/retry"
	xtls "github.com/m3db/m3/src/x/tls"

	"github.com/stretchr/testify/require"
)

const (
	testListenAddress = "127.0.0.1:0"
)

func isKeepAlive(conn net.Conn) (bool, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return false, nil
	}
	file, err := tcpConn.File()
	if err != nil {
		return false, err
	}
	defer file.Close() // nolint: errcheck,gosec

	fd := int(file.Fd())

	value, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE)
	if err != nil {
		return false, err
	}
	if value == 1 {
		return true, nil
	}
	return false, nil
}

func waitFor(checkFn func() bool, numChecks int, waitTime time.Duration) {
	checks := 0
	for !checkFn() && checks < numChecks {
		time.Sleep(waitTime)
		checks++
	}
}

func testPlainTCPServer(addr string) (*server, *mockHandler, *int32, *int32) {
	return testServer(addr, xtls.Disabled, false)
}

// nolint: unparam
func testServer(addr string, tlsMode xtls.ServerMode, mTLSEnabled bool) (*server, *mockHandler, *int32, *int32) {
	var (
		numAdded   int32
		numRemoved int32
	)

	tlsOpts := xtls.NewOptions().
		SetServerMode(tlsMode).
		SetMutualTLSEnabled(mTLSEnabled).
		SetCAFile("./testdata/rootCA.crt").
		SetCertFile("./testdata/server.crt").
		SetKeyFile("./testdata/server.key")

	opts := NewOptions().
		SetRetryOptions(retry.NewOptions().SetMaxRetries(2)).
		SetTCPConnectionKeepAlive(true).
		SetTCPConnectionKeepAlivePeriod(time.Hour)
	opts = opts.SetInstrumentOptions(opts.InstrumentOptions().SetReportInterval(time.Second)).SetTLSOptions(tlsOpts)

	h := newMockHandler()
	s := NewServer(addr, h, opts).(*server)

	s.addConnectionFn = func(conn net.Conn) bool {
		atomic.AddInt32(&numAdded, 1)
		ret := s.addConnection(conn)
		return ret
	}

	s.removeConnectionFn = func(conn net.Conn) {
		atomic.AddInt32(&numRemoved, 1)
		s.removeConnection(conn)
	}

	return s, h, &numAdded, &numRemoved
}

func TestServerListenAndClose(t *testing.T) {
	s, h, numAdded, numRemoved := testPlainTCPServer(testListenAddress)

	var (
		numClients  = 9
		expectedRes []string
	)

	err := s.ListenAndServe()
	require.NoError(t, err)
	listenAddr := s.listener.Addr().String()

	for i := 0; i < numClients; i++ {
		conn, err := net.Dial("tcp", listenAddr)
		require.NoError(t, err)

		msg := fmt.Sprintf("msg%d", i)
		expectedRes = append(expectedRes, msg)

		_, err = conn.Write([]byte(msg))
		require.NoError(t, err)
	}

	for h.called() < numClients {
		time.Sleep(100 * time.Millisecond)
	}
	waitFor(func() bool { return h.called() == numClients }, 5, 100*time.Millisecond)

	require.False(t, h.isClosed())

	s.Close()

	require.True(t, h.isClosed())
	require.Equal(t, int32(numClients), atomic.LoadInt32(numAdded))
	require.Equal(t, int32(numClients), atomic.LoadInt32(numRemoved))
	require.Equal(t, numClients, h.called())
	require.Equal(t, expectedRes, h.res())
}

func TestServe(t *testing.T) {
	s, _, _, _ := testPlainTCPServer(testListenAddress)

	l, err := net.Listen("tcp", testListenAddress)
	require.NoError(t, err)

	err = s.Serve(l)
	require.NoError(t, err)
	require.Equal(t, l, s.listener)
	require.Equal(t, l.Addr().String(), s.address)

	s.Close()
}

func TestTLS(t *testing.T) {
	tests := []struct {
		name                   string
		tlsMode                xtls.ServerMode
		numClients             int
		expectedServerCalls    int
		dialFn                 func(i int, listenAddr string) (net.Conn, error)
		appendExpectedResultFn func(expecteResult []string, i int, msg string) []string
	}{
		{
			name:                "TLS permissive mode",
			tlsMode:             xtls.Permissive,
			numClients:          9,
			expectedServerCalls: 9,
			dialFn: func(i int, listenAddr string) (net.Conn, error) {
				if i%2 == 0 {
					return net.Dial("tcp", listenAddr)
				}
				return tls.Dial("tcp", listenAddr, &tls.Config{InsecureSkipVerify: true}) // #nosec G402
			},
			appendExpectedResultFn: func(expectedResult []string, i int, msg string) []string {
				return append(expectedResult, msg)
			},
		},
		{
			name:                "TLS enforced mode",
			tlsMode:             xtls.Enforced,
			numClients:          10,
			expectedServerCalls: 5,
			dialFn: func(i int, listenAddr string) (net.Conn, error) {
				if i%2 == 0 {
					return net.Dial("tcp", listenAddr)
				}
				return tls.Dial("tcp", listenAddr, &tls.Config{InsecureSkipVerify: true}) // #nosec G402
			},
			appendExpectedResultFn: func(expectedResult []string, i int, msg string) []string {
				if i%2 == 1 {
					return append(expectedResult, msg)
				}
				return expectedResult
			},
		},
		{
			name:                "Mutual TLS",
			tlsMode:             xtls.Enforced,
			numClients:          9,
			expectedServerCalls: 9,
			dialFn: func(i int, listenAddr string) (net.Conn, error) {
				cert, err := tls.LoadX509KeyPair("./testdata/client.crt", "./testdata/client.key")
				require.NoError(t, err)
				// #nosec G402
				return tls.Dial(
					"tcp",
					listenAddr,
					&tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{cert}},
				)
			},
			appendExpectedResultFn: func(expecteResult []string, i int, msg string) []string {
				return append(expecteResult, msg)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, h, numAdded, numRemoved := testServer(testListenAddress, tt.tlsMode, false)
			var expectedRes []string
			err := s.ListenAndServe()
			require.NoError(t, err)
			listenAddr := s.listener.Addr().String()
			for i := 0; i < tt.numClients; i++ {
				conn, err := tt.dialFn(i, listenAddr)
				require.NoError(t, err)
				waitFor(func() bool { return len(s.conns) == 1 }, 5, 100*time.Millisecond)
				keepAlive, err := isKeepAlive(s.conns[0].(*securedConn).Conn)
				require.True(t, keepAlive)
				require.NoError(t, err)

				msg := fmt.Sprintf("msg%d", i)
				expectedRes = tt.appendExpectedResultFn(expectedRes, i, msg)

				_, err = conn.Write([]byte(msg))
				require.NoError(t, err)
			}
			waitFor(func() bool { return h.called() == tt.expectedServerCalls }, 5, 100*time.Millisecond)
			require.False(t, h.isClosed())

			s.Close()

			require.True(t, h.isClosed())
			require.Equal(t, int32(tt.numClients), atomic.LoadInt32(numAdded))
			require.Equal(t, int32(tt.numClients), atomic.LoadInt32(numRemoved))
			require.Equal(t, tt.expectedServerCalls, h.called())
			require.Equal(t, expectedRes, h.res())
		})
	}
}

type mockHandler struct {
	sync.Mutex

	n        int
	closed   bool
	received []string
}

func newMockHandler() *mockHandler { return &mockHandler{} }

func (h *mockHandler) Handle(conn net.Conn) {
	h.Lock()
	b := make([]byte, 16)

	n, _ := conn.Read(b)
	h.n++
	h.received = append(h.received, string(b[:n]))
	h.Unlock()
}

func (h *mockHandler) Close() {
	h.Lock()
	h.closed = true
	h.Unlock()
}

func (h *mockHandler) isClosed() bool {
	h.Lock()
	defer h.Unlock()

	return h.closed
}

func (h *mockHandler) called() int {
	h.Lock()
	defer h.Unlock()

	return h.n
}

func (h *mockHandler) res() []string {
	h.Lock()
	defer h.Unlock()

	sort.Strings(h.received)
	return h.received
}
