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
	"bufio"
	"net"
)

// TLSHandshakeFirstByte is the first byte of a tls connection handshake
const TLSHandshakeFirstByte = 0x16

func newBufferedConn(conn net.Conn) BufferedConn {
	return &bufferedConn{
		r:    bufio.NewReader(conn),
		Conn: conn,
	}
}

// BufferedConn represents the buffered connection
type BufferedConn interface {
	net.Conn
	IsTLS() (bool, error)
	Peek(int) ([]byte, error)
	GetConn() net.Conn
}

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

// IsTLS returns is the connection is TLS or not
func (b *bufferedConn) IsTLS() (bool, error) {
	connBytes, err := b.Peek(1)
	if err != nil {
		return false, err
	}
	isTLS := len(connBytes) > 0 && connBytes[0] == TLSHandshakeFirstByte
	return isTLS, nil
}

// Peek returns the next n bytes without advancing the reader
func (b *bufferedConn) Peek(n int) ([]byte, error) {
	return b.r.Peek(n)
}

// Read reads n bytes
func (b *bufferedConn) Read(n []byte) (int, error) {
	return b.r.Read(n)
}

// GetConn returns net.Conn connection
func (b *bufferedConn) GetConn() net.Conn {
	return b.Conn
}
