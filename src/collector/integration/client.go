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

package integration

import (
	"net"
	"time"
)

type client struct {
	address        string
	connectTimeout time.Duration
	conn           net.Conn
}

func newClient(address string, connectTimeout time.Duration) *client {
	return &client{
		address:        address,
		connectTimeout: connectTimeout,
	}
}

func (c *client) connect() error {
	conn, err := net.DialTimeout("tcp", c.address, c.connectTimeout)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *client) testConnection() bool {
	if err := c.connect(); err != nil {
		return false
	}
	c.conn.Close()
	c.conn = nil
	return true
}

func (c *client) close() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}
