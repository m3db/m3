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
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/m3db/m3/src/x/retry"
	xtls "github.com/m3db/m3/src/x/tls"

	"github.com/stretchr/testify/require"
)

const (
	testBenchListenAddress = "127.0.0.1:0"
)

func newMockKeepAliveHandler() *mockKeepAliveHandler { return &mockKeepAliveHandler{} }

type mockKeepAliveHandler struct {
	mockHandler
}

func (h *mockKeepAliveHandler) Handle(conn net.Conn) {
	defer conn.Close() // nolint: errcheck

	reader := bufio.NewReader(conn)

	for {
		data, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		_, err = conn.Write([]byte(data))
		if err != nil {
			break
		}
	}
}

// nolint: unparam
func testBenchServer(addr string, h Handler, tlsMode xtls.ServerMode, mTLSEnabled bool) *server {
	tlsOpts := xtls.NewOptions().
		SetServerMode(tlsMode).
		SetMutualTLSEnabled(mTLSEnabled).
		SetCAFile("./testdata/rootCA.crt").
		SetCertFile("./testdata/server.crt").
		SetKeyFile("./testdata/server.key")

	opts := NewOptions().SetRetryOptions(retry.NewOptions().SetMaxRetries(2))
	opts = opts.SetInstrumentOptions(opts.InstrumentOptions().SetReportInterval(time.Second)).SetTLSOptions(tlsOpts)

	s := NewServer(addr, h, opts).(*server)

	s.addConnectionFn = func(conn net.Conn) bool {
		ret := s.addConnection(conn)
		return ret
	}

	s.removeConnectionFn = func(conn net.Conn) {
		s.removeConnection(conn)
	}

	return s
}

func dial(listenAddr string, tlsMode xtls.ServerMode, certs []tls.Certificate, b *testing.B) (net.Conn, error) {
	if tlsMode == xtls.Disabled {
		return net.Dial("tcp", listenAddr)
	}
	return tls.Dial("tcp", listenAddr, &tls.Config{InsecureSkipVerify: true, Certificates: certs}) // #nosec G402
}

func benchmarkServer(tlsMode xtls.ServerMode, mTLSEnabled bool, b *testing.B) {
	handler := newMockHandler()
	server := testBenchServer(testBenchListenAddress, handler, tlsMode, mTLSEnabled)
	b.Cleanup(func() { server.Close(); handler.Close() })
	err := server.ListenAndServe()
	require.NoError(b, err)
	cert, err := tls.LoadX509KeyPair("./testdata/client.crt", "./testdata/client.key")
	require.NoError(b, err)
	for n := 0; n < b.N; n++ {
		conn, err := dial(server.listener.Addr().String(), tlsMode, []tls.Certificate{cert}, b)
		require.NoError(b, err)
		msg := fmt.Sprintf("msg%d", n)
		_, err = conn.Write([]byte(msg))
		require.NoError(b, err)
	}
	waitFor(func() bool { return handler.called() == b.N }, 5, 100*time.Millisecond)
	require.Equal(b, b.N, handler.called())
}

func BenchmarkPlainTCPServer(b *testing.B) {
	benchmarkServer(xtls.Disabled, false, b)
}

func BenchmarkTLSServer(b *testing.B) {
	benchmarkServer(xtls.Enforced, false, b)
}

func BenchmarkMTLSServer(b *testing.B) {
	benchmarkServer(xtls.Enforced, true, b)
}

func benchmarkKeepAliveServer(tlsMode xtls.ServerMode, mTLSEnabled bool, b *testing.B) {
	handler := newMockKeepAliveHandler()
	server := testBenchServer(testBenchListenAddress, handler, tlsMode, mTLSEnabled)
	b.Cleanup(func() { server.Close(); handler.Close() })
	err := server.ListenAndServe()
	require.NoError(b, err)
	cert, err := tls.LoadX509KeyPair("./testdata/client.crt", "./testdata/client.key")
	require.NoError(b, err)
	conn, err := dial(server.listener.Addr().String(), tlsMode, []tls.Certificate{cert}, b)
	require.NoError(b, err)
	for n := 0; n < b.N; n++ {
		msg := fmt.Sprintf("msg%d", n)
		_, err = conn.Write([]byte(msg))
		require.NoError(b, err)
	}
}

func BenchmarkKeepAlivePlainTCPServer(b *testing.B) {
	benchmarkKeepAliveServer(xtls.Disabled, false, b)
}

func BenchmarkKeepAliveTLSServer(b *testing.B) {
	benchmarkKeepAliveServer(xtls.Enforced, false, b)
}

func BenchmarkKeepAliveMTLSServer(b *testing.B) {
	benchmarkKeepAliveServer(xtls.Enforced, true, b)
}
