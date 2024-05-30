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
	"crypto/tls"
	"net"
)

// TLSHandshakeFirstByte is the first byte of a tls connection handshake
const TLSHandshakeFirstByte = 0x16

func newSecuredConn(conn net.Conn) SecuredConn {
	return &securedConn{
		r:     bufio.NewReader(conn),
		Conn:  conn,
		isTLS: nil,
	}
}

// SecuredConn represents the secured connection
type SecuredConn interface {
	net.Conn
	IsTLS() (bool, error)
	GetConn() net.Conn
	UpgradeToTLS(*tls.Config) SecuredConn
}

type securedConn struct {
	net.Conn
	r     *bufio.Reader
	isTLS *bool
}

// IsTLS returns is the connection is TLS or not.
// It peeks at the first byte and checks
// if it is equal to the TLS handshake first byte
// https://www.rfc-editor.org/rfc/rfc5246#appendix-A.1
func (b *securedConn) IsTLS() (bool, error) {
	if b.isTLS != nil {
		return *b.isTLS, nil
	}
	connBytes, err := b.r.Peek(1)
	if err != nil {
		return false, err
	}
	isTLS := len(connBytes) > 0 && connBytes[0] == TLSHandshakeFirstByte
	b.isTLS = &isTLS
	return isTLS, nil
}

func (b *securedConn) UpgradeToTLS(tlsConfig *tls.Config) SecuredConn {
	tlsConn := tls.Server(b, tlsConfig)
	t := true
	return &securedConn{
		r:     bufio.NewReader(tlsConn),
		Conn:  tlsConn,
		isTLS: &t,
	}
}

// Read reads n bytes
func (b *securedConn) Read(n []byte) (int, error) {
	// Before reading we need to ensure if we know the type of the connection.
	// After reading data it will be impossible to determine
	// if the connection has the TLS layer or not.
	if b.isTLS == nil {
		if _, err := b.IsTLS(); err != nil {
			return 0, err
		}
	}
	return b.r.Read(n)
}

// GetConn returns net.Conn connection
func (b *securedConn) GetConn() net.Conn {
	return b.Conn
}
