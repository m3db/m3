// Copyright (c) 2015 Uber Technologies, Inc.

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

package testutils

import (
	"net"
	"testing"

	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/atomic"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type frameRelay struct {
	t           *testing.T
	destination string
	relayFunc   func(outgoing bool, f *tchannel.Frame) *tchannel.Frame
}

func (r frameRelay) listen() (listenHostPort string, cancel func()) {
	var closed atomic.Uint32

	conn, err := net.Listen("tcp", ":0")
	require.NoError(r.t, err, "net.Listen failed")

	go func() {
		for {
			c, err := conn.Accept()
			if err != nil {
				if closed.Load() == 0 {
					r.t.Errorf("Accept failed: %v", err)
				}
				return
			}

			r.relayConn(c)
		}
	}()

	return conn.Addr().String(), func() {
		closed.Inc()
		conn.Close()
	}
}

func (r frameRelay) relayConn(c net.Conn) {
	outC, err := net.Dial("tcp", r.destination)

	if assert.NoError(r.t, err, "relay connection failed") {
		go r.relayBetween(true /* outgoing */, c, outC)
		go r.relayBetween(false /* outgoing */, outC, c)
	}
}

func (r frameRelay) relayBetween(outgoing bool, c net.Conn, outC net.Conn) {
	frame := tchannel.NewFrame(tchannel.MaxFramePayloadSize)
	for {
		if !assert.NoError(r.t, frame.ReadIn(c), "read frame failed") {
			return
		}

		frame = r.relayFunc(outgoing, frame)
		if !assert.NoError(r.t, frame.WriteOut(outC), "write frame failed") {
			return
		}
	}
}

// FrameRelay sets up a relay that can modify frames using relayFunc.
func FrameRelay(t *testing.T, destination string, relayFunc func(outgoing bool, f *tchannel.Frame) *tchannel.Frame) (listenHostPort string, cancel func()) {
	return frameRelay{t, destination, relayFunc}.listen()
}
