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
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func testTCPServer(connCh chan *securedConn, errCh chan error) (net.Listener, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	go func(net.Listener, chan *securedConn, chan error) {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
		} else {
			securedConn := newSecuredConn(conn)
			isTLS, err := securedConn.IsTLS()
			if err != nil {
				errCh <- err
				return
			}
			if isTLS {
				certs, err := tls.LoadX509KeyPair("testdata/server.crt", "testdata/server.key")
				if err != nil {
					errCh <- err
					return
				}
				tlsConfig := tls.Config{Certificates: []tls.Certificate{certs}, MinVersion: tls.VersionTLS13}
				securedConn = securedConn.UpgradeToTLS(&tlsConfig)
				tlsConn := securedConn.Conn.(*tls.Conn)
				if err = tlsConn.Handshake(); err != nil {
					errCh <- err
				}
			}
			connCh <- securedConn
		}
	}(listener, connCh, errCh)
	return listener, nil
}

func TestPlainTCPConnection(t *testing.T) {
	connCh := make(chan *securedConn)
	errCh := make(chan error)
	listener, err := testTCPServer(connCh, errCh)
	require.NoError(t, err)
	defer listener.Close() // nolint: errcheck

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	data := []byte("not a tls connection")
	_, err = clientConn.Write(data)
	require.NoError(t, err)

	var conn *securedConn
	select {
	case newConn := <-connCh:
		conn = newConn
	case newErr := <-errCh:
		err = newErr
	}
	require.NoError(t, err)
	defer conn.Close() // nolint: errcheck

	isTLS, err := conn.IsTLS()
	require.NoError(t, err)
	require.False(t, isTLS)
	result := make([]byte, len(data))
	_, err = conn.Read(result)
	require.NoError(t, err)
	require.Equal(t, data, result)
}

func TestTLSConnection(t *testing.T) {
	connCh := make(chan *securedConn)
	errCh := make(chan error)
	listener, err := testTCPServer(connCh, errCh)
	require.NoError(t, err)
	defer listener.Close() // nolint: errcheck

	clientConn, err := tls.Dial("tcp", listener.Addr().String(), &tls.Config{InsecureSkipVerify: true}) // #nosec G402
	require.NoError(t, err)
	defer clientConn.Close() // nolint: errcheck

	data := []byte("tls connection")
	_, err = clientConn.Write(data)
	require.NoError(t, err)

	var serverConn *securedConn
	select {
	case newConn := <-connCh:
		serverConn = newConn
	case newErr := <-errCh:
		err = newErr
	}
	require.NoError(t, err)
	defer serverConn.Close() // nolint: errcheck

	isTLS, err := serverConn.IsTLS()
	require.NoError(t, err)
	require.True(t, isTLS)

	result := make([]byte, len(data))
	_, err = serverConn.Read(result)
	require.NoError(t, err)
	require.Equal(t, data, result)
}
