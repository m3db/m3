// Copyright (c) 2022 Uber Technologies, Inc.
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

// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bridge

import (
	"io"
	"net"
	"sync"
)

// Dialer makes TCP connections.
type Dialer interface {
	Dial() (net.Conn, error)
}

// Bridge proxies connections between listener and dialer, making it possible
// to disconnect grpc network connections without closing the logical grpc connection.
type Bridge struct {
	dialer Dialer
	l      net.Listener
	conns  map[*bridgeConn]struct{}

	stopc      chan struct{}
	pausec     chan struct{}
	blackholec chan struct{}
	wg         sync.WaitGroup

	mu sync.Mutex
}

// New constructs a bridge listening to the given listener and connecting using the given dialer.
func New(dialer Dialer, listener net.Listener) (*Bridge, error) {
	b := &Bridge{
		// Bridge "port" is ("%05d%05d0", port, pid) since go1.8 expects the port to be a number
		dialer:     dialer,
		l:          listener,
		conns:      make(map[*bridgeConn]struct{}),
		stopc:      make(chan struct{}),
		pausec:     make(chan struct{}),
		blackholec: make(chan struct{}),
	}
	close(b.pausec)
	b.wg.Add(1)
	go b.serveListen()
	return b, nil
}

// Close stops the bridge.
func (b *Bridge) Close() {
	//nolint:errcheck
	b.l.Close()
	b.mu.Lock()
	select {
	case <-b.stopc:
	default:
		close(b.stopc)
	}
	b.mu.Unlock()
	b.wg.Wait()
}

// DropConnections drops connections to the bridge.
func (b *Bridge) DropConnections() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for bc := range b.conns {
		bc.Close()
	}
	b.conns = make(map[*bridgeConn]struct{})
}

// PauseConnections pauses all connections.
func (b *Bridge) PauseConnections() {
	b.mu.Lock()
	b.pausec = make(chan struct{})
	b.mu.Unlock()
}

// UnpauseConnections unpauses all connections.
func (b *Bridge) UnpauseConnections() {
	b.mu.Lock()
	select {
	case <-b.pausec:
	default:
		close(b.pausec)
	}
	b.mu.Unlock()
}

func (b *Bridge) serveListen() {
	defer func() {
		//nolint:errcheck
		b.l.Close()
		b.mu.Lock()
		for bc := range b.conns {
			bc.Close()
		}
		b.mu.Unlock()
		b.wg.Done()
	}()

	for {
		inc, ierr := b.l.Accept()
		if ierr != nil {
			return
		}
		b.mu.Lock()
		pausec := b.pausec
		b.mu.Unlock()
		select {
		case <-b.stopc:
			//nolint:errcheck
			inc.Close()
			return
		case <-pausec:
		}

		outc, oerr := b.dialer.Dial()
		if oerr != nil {
			//nolint:errcheck
			inc.Close()
			return
		}

		bc := &bridgeConn{inc, outc, make(chan struct{})}
		b.wg.Add(1)
		b.mu.Lock()
		b.conns[bc] = struct{}{}
		go b.serveConn(bc)
		b.mu.Unlock()
	}
}

func (b *Bridge) serveConn(bc *bridgeConn) {
	defer func() {
		close(bc.donec)
		bc.Close()
		b.mu.Lock()
		delete(b.conns, bc)
		b.mu.Unlock()
		b.wg.Done()
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		//nolint:errcheck
		b.ioCopy(bc.out, bc.in)
		bc.close()
		wg.Done()
	}()
	go func() {
		//nolint:errcheck
		b.ioCopy(bc.in, bc.out)
		bc.close()
		wg.Done()
	}()
	wg.Wait()
}

type bridgeConn struct {
	in    net.Conn
	out   net.Conn
	donec chan struct{}
}

func (bc *bridgeConn) Close() {
	bc.close()
	<-bc.donec
}

func (bc *bridgeConn) close() {
	//nolint:errcheck
	bc.in.Close()
	//nolint:errcheck
	bc.out.Close()
}

// Blackhole stops connections to the bridge.
func (b *Bridge) Blackhole() {
	b.mu.Lock()
	close(b.blackholec)
	b.mu.Unlock()
}

// Unblackhole stops connections to the bridge.
func (b *Bridge) Unblackhole() {
	b.mu.Lock()
	for bc := range b.conns {
		bc.Close()
	}
	b.conns = make(map[*bridgeConn]struct{})
	b.blackholec = make(chan struct{})
	b.mu.Unlock()
}

// ref. https://github.com/golang/go/blob/master/src/io/io.go copyBuffer
func (b *Bridge) ioCopy(dst io.Writer, src io.Reader) (err error) {
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-b.blackholec:
			//nolint:errcheck
			io.Copy(io.Discard, src)
			return nil
		default:
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if ew != nil {
				return ew
			}
			if nr != nw {
				return io.ErrShortWrite
			}
		}
		if er != nil {
			err = er
			break
		}
	}
	return err
}
