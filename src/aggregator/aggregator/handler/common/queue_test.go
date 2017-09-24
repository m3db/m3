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

package common

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/retry"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const (
	testFakeServerAddr = "nonexistent"
)

func TestNewQueueEmptyServerList(t *testing.T) {
	_, err := newQueue(nil, testQueueOptions())
	require.Equal(t, errEmptyServerList, err)
}

func TestQueueHandleClosed(t *testing.T) {
	q, err := newQueue([]string{testFakeServerAddr}, testQueueOptions())
	require.NoError(t, err)

	q.Close()
	require.Equal(t, errQueueClosed, q.Enqueue(testRefCountedBuffer()))
}

func TestQueueHandleQueueFull(t *testing.T) {
	opts := testQueueOptions().SetQueueSize(3)
	q, err := newQueue([]string{testFakeServerAddr}, opts)
	require.NoError(t, err)

	// Fill up the queue.
	for i := 0; i < 10; i++ {
		select {
		case q.bufCh <- testRefCountedBuffer():
		default:
		}
	}

	// Enqueue the buffer and expect it to be queued.
	require.NoError(t, q.Enqueue(testRefCountedBuffer()))
}

func TestQueueForwardToConnNoConnectionClosed(t *testing.T) {
	opts := testQueueOptions().SetQueueSize(3)
	q, err := newQueue([]string{testFakeServerAddr}, opts)
	require.NoError(t, err)
	// Queue up a nil buffer and close the queue.
	q.bufCh <- nil
	q.Close()
}

func TestQueueForwardToConnWithConnectionClosed(t *testing.T) {
	opts := testQueueOptions().SetQueueSize(10)
	servers := []string{testFakeServerAddr}
	q, err := newQueue(servers, opts)
	require.NoError(t, err)

	var (
		res         [][]byte
		expected    [][]byte
		numConnects int32
		errConnect  = errors.New("error connecting")
	)
	q.tryConnectFn = func(addr string) (*net.TCPConn, error) {
		if atomic.AddInt32(&numConnects, 1) == 1 {
			return nil, errConnect
		}
		return &net.TCPConn{}, nil
	}
	q.writeToConnFn = func(conn *net.TCPConn, data []byte) (int, error) {
		res = append(res, data)
		return 0, nil
	}
	q.initConnections(servers)

	// Wait for the mock functions to take effect.
	for atomic.LoadInt32(&numConnects) <= 1 {
	}

	// Enqueue some buffers.
	for i := 0; i < 10; i++ {
		data := []byte{byte(i), byte(i + 1)}
		expected = append(expected, data)
		buf := testRefCountedBuffer()
		_, err := buf.Buffer().Buffer().Write(data)
		require.NoError(t, err)
		require.NoError(t, q.Enqueue(buf))
	}

	// Expect all the buffers to be processed.
	q.Close()
	require.Equal(t, expected, res)
}

func TestQueueForwardToConnReenqueueSuccess(t *testing.T) {
	opts := testQueueOptions().SetQueueSize(10)
	servers := []string{testFakeServerAddr}
	q, err := newQueue(servers, opts)
	require.NoError(t, err)

	var (
		res         [][]byte
		numConnects int32
		numWrites   int32
		errConnect  = errors.New("error connecting")
		errWrite    = errors.New("write error")
	)
	q.tryConnectFn = func(addr string) (*net.TCPConn, error) {
		if atomic.AddInt32(&numConnects, 1) == 1 {
			return nil, errConnect
		}
		return &net.TCPConn{}, nil
	}
	q.writeToConnFn = func(conn *net.TCPConn, data []byte) (int, error) {
		if atomic.AddInt32(&numWrites, 1) == 1 {
			return 0, errWrite
		}
		res = append(res, data)
		return 0, nil
	}
	q.initConnections(servers)

	// Wait for the mock functions to take effect.
	for atomic.LoadInt32(&numConnects) <= 1 {
	}

	// Enqueue some buffers.
	buf := testRefCountedBuffer()
	_, err = buf.Buffer().Buffer().Write([]byte{0x3, 0x4, 0x5})
	require.NoError(t, err)
	require.NoError(t, q.Enqueue(buf))

	// Wait for buffer to be re-enqueued.
	for atomic.LoadInt32(&numWrites) <= 1 {
	}

	// Expect all the buffers to be processed.
	q.Close()
	require.Equal(t, [][]byte{[]byte{0x3, 0x4, 0x5}}, res)
}

func TestQueueForwardToConnReenqueueQueueFull(t *testing.T) {
	opts := testQueueOptions().SetQueueSize(1)

	servers := []string{testFakeServerAddr}
	q, err := newQueue(servers, opts)
	require.NoError(t, err)

	var (
		res         [][]byte
		numConnects int32
		numWrites   int32
		errConnect  = errors.New("error connecting")
		errWrite    = errors.New("write error")
	)
	q.tryConnectFn = func(addr string) (*net.TCPConn, error) {
		if atomic.AddInt32(&numConnects, 1) == 1 {
			return nil, errConnect
		}
		return &net.TCPConn{}, nil
	}
	q.writeToConnFn = func(conn *net.TCPConn, data []byte) (int, error) {
		if atomic.AddInt32(&numWrites, 1) == 1 {
			// Fill up the queue.
			buf := testRefCountedBuffer()
			_, err = buf.Buffer().Buffer().Write([]byte{0x1, 0x2})
			require.NoError(t, err)
			q.bufCh <- buf
			return 0, errWrite
		}
		res = append(res, data)
		return 0, nil
	}
	q.initConnections(servers)

	// Wait for the mock functions to take effect.
	for atomic.LoadInt32(&numConnects) <= 1 {
	}

	// Enqueue some buffers.
	buf := testRefCountedBuffer()
	_, err = buf.Buffer().Buffer().Write([]byte{0x3, 0x4, 0x5})
	require.NoError(t, err)
	require.NoError(t, q.Enqueue(buf))

	// Wait for buffer to be re-enqueued.
	for atomic.LoadInt32(&numWrites) <= 1 {
	}

	// Expect the first buffer to be dropped.
	q.Close()
	require.Equal(t, [][]byte{[]byte{0x1, 0x2}}, res)
}

func TestQueueClose(t *testing.T) {
	q, err := newQueue([]string{testFakeServerAddr}, testQueueOptions())
	require.NoError(t, err)

	// Closing the queue sets the flag.
	q.Close()
	require.True(t, q.closed)

	// Closing the queue a second time is a no op.
	q.Close()
	require.True(t, q.closed)
}

func TestTryConnectTimeout(t *testing.T) {
	errTimeout := errors.New("error timing out")
	q := &queue{metrics: newQueueMetrics(tally.NoopScope)}
	q.dialWithTimeoutFn = func(string, string, time.Duration) (net.Conn, error) {
		return nil, errTimeout
	}
	_, err := q.tryConnect(testFakeServerAddr)
	require.Equal(t, errTimeout, err)
}

func TestTryConnectSuccess(t *testing.T) {
	q := &queue{metrics: newQueueMetrics(tally.NoopScope)}
	q.dialWithTimeoutFn = func(string, string, time.Duration) (net.Conn, error) {
		return &net.TCPConn{}, nil
	}
	_, err := q.tryConnect(testFakeServerAddr)
	require.NoError(t, err)
}

func testRefCountedBuffer() *RefCountedBuffer {
	return NewRefCountedBuffer(msgpack.NewBufferedEncoder())
}

func testQueueOptions() QueueOptions {
	retrierOpts := retry.NewOptions().
		SetInitialBackoff(time.Millisecond).
		SetBackoffFactor(1).
		SetForever(true)
	connectionOpts := NewConnectionOptions().
		SetConnectionKeepAlive(true).
		SetConnectTimeout(100 * time.Millisecond).
		SetReconnectRetryOptions(retrierOpts)
	return NewQueueOptions().
		SetConnectionOptions(connectionOpts).
		SetQueueSize(4096)
}

type enqueueFn func(buffer *RefCountedBuffer) error

type mockQueue struct {
	enqueueFn enqueueFn
}

func (q *mockQueue) Enqueue(buffer *RefCountedBuffer) error {
	return q.enqueueFn(buffer)
}

func (q *mockQueue) Close() {}
