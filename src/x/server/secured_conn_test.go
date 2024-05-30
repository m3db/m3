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

func testTCPServer(connCh chan net.Conn, errCh chan error) (net.Listener, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	go func(net.Listener, chan net.Conn, chan error) {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
		} else {
			connCh <- conn
		}
	}(listener, connCh, errCh)
	return listener, nil
}

func TestPlainTCPConnection(t *testing.T) {
	connCh := make(chan net.Conn)
	errCh := make(chan error)
	listener, err := testTCPServer(connCh, errCh)
	require.NoError(t, err)
	defer listener.Close()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	_, err = clientConn.Write([]byte("not a tls connection"))
	require.NoError(t, err)

	var conn SecuredConn
	select {
	case newConn := <-connCh:
		conn = newSecuredConn(newConn)
	case newErr := <-errCh:
		err = newErr
	}
	require.NoError(t, err)
	defer conn.Close()

	isTLS, err := conn.IsTLS()
	require.NoError(t, err)
	require.False(t, isTLS)
}

func TestTLSConnection(t *testing.T) {
	connCh := make(chan net.Conn)
	errCh := make(chan error)
	listener, err := testTCPServer(connCh, errCh)
	require.NoError(t, err)
	defer listener.Close()

	tcpConn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	tlsConn := tls.Client(tcpConn, &tls.Config{InsecureSkipVerify: true})
	defer tlsConn.Close()
	go tlsConn.Handshake()

	var conn SecuredConn
	select {
	case newConn := <-connCh:
		conn = newSecuredConn(newConn)
	case newErr := <-errCh:
		err = newErr
	}
	require.NoError(t, err)
	defer conn.Close()

	isTLS, err := conn.IsTLS()
	require.NoError(t, err)
	require.True(t, isTLS)
}
