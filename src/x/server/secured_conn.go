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
)

// TLSHandshakeFirstByte is the first byte of a tls connection handshake
const TLSHandshakeFirstByte = 0x16

func newSecuredConn(conn net.Conn) *securedConn {
	return &securedConn{
		Conn: conn,
	}
}

type securedConn struct {
	net.Conn
	isTLS           *bool
	peekedByte      byte
	peekedByteIsSet bool
}

// IsTLS returns is the connection is TLS or not.
// It peeks at the first byte and checks
// if it is equal to the TLS handshake first byte
// https://www.rfc-editor.org/rfc/rfc5246#appendix-A.1
func (s *securedConn) IsTLS() (bool, error) {
	if s.isTLS != nil {
		return *s.isTLS, nil
	}
	firstByte, err := s.peek()
	if err != nil {
		return false, err
	}
	isTLS := firstByte == TLSHandshakeFirstByte
	s.isTLS = &isTLS

	return isTLS, nil
}

func (s *securedConn) UpgradeToTLS(tlsConfig *tls.Config) *securedConn {
	tlsConn := tls.Server(s, tlsConfig)
	t := true
	return &securedConn{
		Conn:  tlsConn,
		isTLS: &t,
	}
}

// Read reads n bytes
func (s *securedConn) Read(n []byte) (int, error) {
	// Before reading we need to ensure if we know the type of the connection.
	// After reading data it will be impossible to determine
	// if the connection has the TLS layer or not.
	if s.isTLS == nil {
		if _, err := s.IsTLS(); err != nil {
			return 0, err
		}
	}
	if s.peekedByteIsSet && len(n) > 0 {
		n[0] = s.peekedByte
		s.peekedByteIsSet = false
		n, err := s.Conn.Read(n[1:])
		return n + 1, err
	}
	return s.Conn.Read(n)
}

func (s *securedConn) peek() (byte, error) {
	if s.peekedByteIsSet {
		return s.peekedByte, nil
	}

	var buf [1]byte
	_, err := s.Conn.Read(buf[:])
	if err != nil {
		return 0, err
	}
	s.peekedByte = buf[0]
	s.peekedByteIsSet = true
	return buf[0], nil
}
