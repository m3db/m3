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
package agent

import (
	"net"
	"sync"
	"testing"
	"time"

	hb "github.com/m3db/m3em/generated/proto/heartbeat"

	"github.com/m3db/m3x/instrument"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestInvalidMessageType(t *testing.T) {
	hbService := &mockHeartbeatServer{}
	hbListener, err := net.Listen("tcp", "127.0.0.1:0")
	hbServer := grpc.NewServer(grpc.MaxConcurrentStreams(16384))
	require.NoError(t, err)
	hb.RegisterHeartbeaterServer(hbServer, hbService)
	go hbServer.Serve(hbListener)
	defer hbServer.Stop()

	var (
		lock   sync.Mutex
		recErr error
	)
	iopts := instrument.NewOptions()
	opts := heartbeatOpts{
		endpoint: hbListener.Addr().String(),
		nowFn:    time.Now,
		errorFn: func(err error) {
			lock.Lock()
			recErr = err
			lock.Unlock()
		},
	}

	h, err := newHeartbeater(mockRunner{}, opts, iopts)
	require.NoError(t, err)
	require.NoError(t, h.start(100*time.Millisecond))
	defer h.stop()
	invalidMsg := heartbeatMsg{
		stop:             false,
		processTerminate: false,
		overwritten:      false,
	}
	h.msgChan <- invalidMsg
	time.Sleep(300 * time.Millisecond)
	lock.Lock()
	require.Error(t, recErr)
	lock.Unlock()
}

type mockRunner struct{}

func (mr mockRunner) Running() bool {
	return true
}
