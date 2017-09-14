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

package handler

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/retry"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const (
	testFakeServerAddr = "nonexistent"
)

func TestNewForwardHandlerEmptyServerList(t *testing.T) {
	_, err := newForwardHandler(nil, testForwardHandlerOptions())
	require.Equal(t, errEmptyServerList, err)
}

func TestForwardHandlerHandleClosed(t *testing.T) {
	h, err := newForwardHandler([]string{testFakeServerAddr}, testForwardHandlerOptions())
	require.NoError(t, err)

	h.Close()
	require.Equal(t, errHandlerClosed, h.Handle(testRefCountedBuffer()))
}

func TestForwardHandlerHandleQueueFull(t *testing.T) {
	opts := testForwardHandlerOptions().SetQueueSize(3)
	handler, err := newForwardHandler([]string{testFakeServerAddr}, opts)
	require.NoError(t, err)

	// Fill up the queue.
	for i := 0; i < 10; i++ {
		select {
		case handler.bufCh <- testRefCountedBuffer():
		default:
		}
	}

	// Handle the buffer and expect it to be queued.
	require.NoError(t, handler.Handle(testRefCountedBuffer()))
}

func TestForwardHandlerForwardToConnNoConnectionClosed(t *testing.T) {
	opts := testForwardHandlerOptions().SetQueueSize(3)
	handler, err := newForwardHandler([]string{testFakeServerAddr}, opts)
	require.NoError(t, err)
	// Queue up a nil buffer and close the handler.
	handler.bufCh <- nil
	handler.Close()
}

func TestForwardHandlerForwardToConnWithConnectionClosed(t *testing.T) {
	opts := testForwardHandlerOptions().SetQueueSize(10)
	servers := []string{testFakeServerAddr}
	h, err := newForwardHandler(servers, opts)
	require.NoError(t, err)

	var (
		res         [][]byte
		expected    [][]byte
		numConnects int32
		errConnect  = errors.New("error connecting")
	)
	h.tryConnectFn = func(addr string) (*net.TCPConn, error) {
		if atomic.AddInt32(&numConnects, 1) == 1 {
			return nil, errConnect
		}
		return &net.TCPConn{}, nil
	}
	h.writeToConnFn = func(conn *net.TCPConn, data []byte) (int, error) {
		res = append(res, data)
		return 0, nil
	}
	h.initConnections(servers)

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
		require.NoError(t, h.Handle(buf))
	}

	// Expect all the buffers to be processed.
	h.Close()
	require.Equal(t, expected, res)
}

func TestForwardHandlerForwardToConnReenqueueSuccess(t *testing.T) {
	opts := testForwardHandlerOptions().SetQueueSize(10)
	servers := []string{testFakeServerAddr}
	h, err := newForwardHandler(servers, opts)
	require.NoError(t, err)

	var (
		res         [][]byte
		numConnects int32
		numWrites   int32
		errConnect  = errors.New("error connecting")
		errWrite    = errors.New("write error")
	)
	h.tryConnectFn = func(addr string) (*net.TCPConn, error) {
		if atomic.AddInt32(&numConnects, 1) == 1 {
			return nil, errConnect
		}
		return &net.TCPConn{}, nil
	}
	h.writeToConnFn = func(conn *net.TCPConn, data []byte) (int, error) {
		if atomic.AddInt32(&numWrites, 1) == 1 {
			return 0, errWrite
		}
		res = append(res, data)
		return 0, nil
	}
	h.initConnections(servers)

	// Wait for the mock functions to take effect.
	for atomic.LoadInt32(&numConnects) <= 1 {
	}

	// Enqueue some buffers.
	buf := testRefCountedBuffer()
	_, err = buf.Buffer().Buffer().Write([]byte{0x3, 0x4, 0x5})
	require.NoError(t, err)
	require.NoError(t, h.Handle(buf))

	// Wait for buffer to be re-enqueued.
	for atomic.LoadInt32(&numWrites) <= 1 {
	}

	// Expect all the buffers to be processed.
	h.Close()
	require.Equal(t, [][]byte{[]byte{0x3, 0x4, 0x5}}, res)
}

func TestForwardHandlerForwardToConnReenqueueQueueFull(t *testing.T) {
	opts := testForwardHandlerOptions().SetQueueSize(1)

	servers := []string{testFakeServerAddr}
	h, err := newForwardHandler(servers, opts)
	require.NoError(t, err)

	var (
		res         [][]byte
		numConnects int32
		numWrites   int32
		errConnect  = errors.New("error connecting")
		errWrite    = errors.New("write error")
	)
	h.tryConnectFn = func(addr string) (*net.TCPConn, error) {
		if atomic.AddInt32(&numConnects, 1) == 1 {
			return nil, errConnect
		}
		return &net.TCPConn{}, nil
	}
	h.writeToConnFn = func(conn *net.TCPConn, data []byte) (int, error) {
		if atomic.AddInt32(&numWrites, 1) == 1 {
			// Fill up the queue.
			buf := testRefCountedBuffer()
			_, err = buf.Buffer().Buffer().Write([]byte{0x1, 0x2})
			require.NoError(t, err)
			h.bufCh <- buf
			return 0, errWrite
		}
		res = append(res, data)
		return 0, nil
	}
	h.initConnections(servers)

	// Wait for the mock functions to take effect.
	for atomic.LoadInt32(&numConnects) <= 1 {
	}

	// Enqueue some buffers.
	buf := testRefCountedBuffer()
	_, err = buf.Buffer().Buffer().Write([]byte{0x3, 0x4, 0x5})
	require.NoError(t, err)
	require.NoError(t, h.Handle(buf))

	// Wait for buffer to be re-enqueued.
	for atomic.LoadInt32(&numWrites) <= 1 {
	}

	// Expect the first buffer to be dropped.
	h.Close()
	require.Equal(t, [][]byte{[]byte{0x1, 0x2}}, res)
}

func TestForwardHandlerClose(t *testing.T) {
	handler, err := newForwardHandler([]string{testFakeServerAddr}, testForwardHandlerOptions())
	require.NoError(t, err)

	// Close the handler sets the flag.
	handler.Close()
	require.True(t, handler.closed)

	// Close the handler a second time is a no op.
	handler.Close()
	require.True(t, handler.closed)
}

func TestTryConnectTimeout(t *testing.T) {
	errTimeout := errors.New("error timing out")
	h := &forwardHandler{metrics: newForwardHandlerMetrics(tally.NoopScope)}
	h.dialWithTimeoutFn = func(string, string, time.Duration) (net.Conn, error) {
		return nil, errTimeout
	}
	_, err := h.tryConnect(testFakeServerAddr)
	require.Equal(t, errTimeout, err)
}

func TestTryConnectSuccess(t *testing.T) {
	h := &forwardHandler{metrics: newForwardHandlerMetrics(tally.NoopScope)}
	h.dialWithTimeoutFn = func(string, string, time.Duration) (net.Conn, error) {
		return &net.TCPConn{}, nil
	}
	_, err := h.tryConnect(testFakeServerAddr)
	require.NoError(t, err)
}

func testRefCountedBuffer() *aggregator.RefCountedBuffer {
	return aggregator.NewRefCountedBuffer(msgpack.NewBufferedEncoder())
}

func testForwardHandlerOptions() ForwardHandlerOptions {
	retrierOpts := retry.NewOptions().
		SetInitialBackoff(time.Millisecond).
		SetBackoffFactor(1).
		SetForever(true)
	return NewForwardHandlerOptions().
		SetConnectionKeepAlive(true).
		SetConnectTimeout(100 * time.Millisecond).
		SetQueueSize(4096).
		SetReconnectRetrier(retry.NewRetrier(retrierOpts))
}
