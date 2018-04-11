// Copyright (c) 2018 Uber Technologies, In
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

package writer

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSignalResetConnection(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	w := newRetryableConnection(
		lis.Addr().String(),
		testConnectionOptions().SetResetDelay(time.Minute),
	)
	require.Equal(t, 0, len(w.resetCh))

	var called int
	w.connectFn = func(addr string) (net.Conn, error) {
		called++
		return nil, nil
	}

	w.NotifyReset()
	require.Equal(t, 1, len(w.resetCh))
	require.NoError(t, w.resetWithConnectFn(w.connectFn))
	require.Equal(t, 0, called)

	now := time.Now()
	w.nowFn = func() time.Time { return now.Add(1 * time.Hour) }
	require.Equal(t, 1, len(w.resetCh))
	require.NoError(t, w.resetWithConnectFn(w.connectFn))
	require.Equal(t, 1, called)
	require.Equal(t, 1, len(w.resetCh))
	// Reset won't do anything as it is too soon since last reset.
	require.NoError(t, w.resetWithConnectFn(w.connectFn))
	require.Equal(t, 1, called)

	w.nowFn = func() time.Time { return now.Add(2 * time.Hour) }
	require.NoError(t, w.resetWithConnectFn(w.connectFn))
	require.Equal(t, 2, called)
}

func TestResetConnection(t *testing.T) {
	w := newRetryableConnection("badAddress", testConnectionOptions())
	require.Equal(t, 1, len(w.resetCh))
	_, err := w.Write([]byte("foo"))
	require.Error(t, err)
	require.Equal(t, errConnNotInitialized, err)

	var called int
	conn := new(net.TCPConn)
	w.connectFn = func(addr string) (net.Conn, error) {
		called++
		require.Equal(t, "badAddress", addr)
		return conn, nil
	}
	w.resetWithConnectFn(w.connectWithRetry)
	require.Equal(t, 1, called)
}

func TestRetryableConnectionBackgroundReset(t *testing.T) {
	w := newRetryableConnection("badAddress", testConnectionOptions())
	require.Equal(t, 1, len(w.resetCh))

	var lock sync.Mutex
	var called int
	conn := new(net.TCPConn)
	w.connectFn = func(addr string) (net.Conn, error) {
		lock.Lock()
		defer lock.Unlock()

		called++
		require.Equal(t, "badAddress", addr)
		return conn, nil
	}

	now := time.Now()
	w.nowFn = func() time.Time { return now.Add(1 * time.Hour) }
	w.Init()
	for {
		lock.Lock()
		c := called
		lock.Unlock()
		if c > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	w.Close()
}
