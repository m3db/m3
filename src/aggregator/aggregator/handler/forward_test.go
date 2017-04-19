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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/retry"

	"github.com/stretchr/testify/require"
)

const (
	testFakeServerAddr  = "nonexistent"
	testLocalServerAddr = "127.0.0.1:0"
)

func TestNewForwardHandlerEmptyServerList(t *testing.T) {
	_, err := NewForwardHandler(nil, testForwardHandlerOptions())
	require.Equal(t, errEmptyServerList, err)
}

func TestForwardHandlerHandleClosed(t *testing.T) {
	h, err := NewForwardHandler([]string{testFakeServerAddr}, testForwardHandlerOptions())
	require.NoError(t, err)

	h.Close()
	require.Equal(t, errHandlerClosed, h.Handle(nil))
}

func TestForwardHandlerHandleQueueFull(t *testing.T) {
	opts := testForwardHandlerOptions().SetQueueSize(3)
	h, err := NewForwardHandler([]string{testFakeServerAddr}, opts)
	require.NoError(t, err)

	// Fill up the queue.
	handler := h.(*forwardHandler)
	for i := 0; i < 10; i++ {
		select {
		case handler.bufCh <- msgpack.NewBufferedEncoder():
		default:
		}
	}

	// Handle the buffer and expect it to be queued.
	require.NoError(t, handler.Handle(msgpack.NewBufferedEncoder()))
}

func TestForwardHandlerForwardToConnNoConnectionClosed(t *testing.T) {
	opts := testForwardHandlerOptions().SetQueueSize(3)
	h, err := NewForwardHandler([]string{testFakeServerAddr}, opts)
	require.NoError(t, err)

	// Queue up a nil buffer and close the handler.
	handler := h.(*forwardHandler)
	handler.bufCh <- nil
	handler.Close()
}

func TestForwardHandlerForwardToConnWithConnectionClosed(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	l, err := net.Listen(tcpProtocol, testLocalServerAddr)
	require.NoError(t, err)
	serverAddr := l.Addr().String()

	var (
		accepted int32
		stop     int32
		res      [][]byte
		expected [][]byte
	)

	// Start tcp server.
	go func() {
		defer wg.Done()

		conn, err := l.Accept()
		require.NoError(t, err)
		atomic.StoreInt32(&accepted, 1)
		for atomic.LoadInt32(&stop) == 1 {
		}
		conn.Close()
	}()

	// Set up mock functions.
	opts := testForwardHandlerOptions().SetQueueSize(10)
	h, err := NewForwardHandler([]string{serverAddr}, opts)
	require.NoError(t, err)
	handler := h.(*forwardHandler)
	handler.Lock()
	handler.writeToConnFn = func(conn *net.TCPConn, data []byte) (int, error) {
		res = append(res, data)
		return 0, nil
	}
	handler.Unlock()

	// Wait for the mock functions to take effect.
	for atomic.LoadInt32(&accepted) != 1 {
	}

	// Enqueue some buffers.
	for i := 0; i < 10; i++ {
		data := []byte{byte(i), byte(i + 1)}
		expected = append(expected, data)
		buf := msgpack.NewBufferedEncoder()
		_, err := buf.Buffer().Write(data)
		require.NoError(t, err)
		require.NoError(t, h.Handle(buf))
	}

	// Expect all the buffers to be processed.
	h.Close()
	require.Equal(t, expected, res)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
}

func TestForwardHandlerForwardToConnReenqueueSuccess(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	l, err := net.Listen(tcpProtocol, testLocalServerAddr)
	require.NoError(t, err)
	serverAddr := l.Addr().String()

	// Set up mock functions.
	var (
		accepted int32
		stop     int32
		res      [][]byte
		count    int32
		errWrite = errors.New("write error")
	)
	opts := testForwardHandlerOptions().SetQueueSize(10)
	h, err := NewForwardHandler([]string{serverAddr}, opts)
	require.NoError(t, err)
	handler := h.(*forwardHandler)
	handler.Lock()
	handler.writeToConnFn = func(conn *net.TCPConn, data []byte) (int, error) {
		if atomic.AddInt32(&count, 1) == 1 {
			return 0, errWrite
		}
		res = append(res, data)
		return 0, nil
	}
	handler.Unlock()

	// Start tcp server.
	go func() {
		defer wg.Done()

		conn, err := l.Accept()
		require.NoError(t, err)
		atomic.StoreInt32(&accepted, 1)
		for atomic.LoadInt32(&stop) == 1 {
		}
		conn.Close()
	}()

	// Wait for the mock functions to take effect.
	for atomic.LoadInt32(&accepted) != 1 {
	}

	// Enqueue some buffers.
	buf := msgpack.NewBufferedEncoder()
	_, err = buf.Buffer().Write([]byte{0x3, 0x4, 0x5})
	require.NoError(t, err)
	require.NoError(t, h.Handle(buf))

	// Wait for buffer to be re-enqueued.
	for atomic.LoadInt32(&count) < 2 {
	}

	// Expect all the buffers to be processed.
	h.Close()
	require.Equal(t, [][]byte{[]byte{0x3, 0x4, 0x5}}, res)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
}

func TestForwardHandlerForwardToConnReenqueueQueueFull(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	l, err := net.Listen(tcpProtocol, testLocalServerAddr)
	require.NoError(t, err)
	serverAddr := l.Addr().String()

	// Set up mock functions.
	var (
		accepted int32
		stop     int32
		res      [][]byte
		count    int32
		errWrite = errors.New("write error")
	)
	opts := testForwardHandlerOptions().SetQueueSize(1)
	h, err := NewForwardHandler([]string{serverAddr}, opts)
	require.NoError(t, err)
	handler := h.(*forwardHandler)
	handler.Lock()
	handler.writeToConnFn = func(conn *net.TCPConn, data []byte) (int, error) {
		if atomic.AddInt32(&count, 1) == 1 {
			// Fill up the queue.
			buf := msgpack.NewBufferedEncoder()
			_, err = buf.Buffer().Write([]byte{0x1, 0x2})
			require.NoError(t, err)
			handler.bufCh <- buf
			return 0, errWrite
		}
		res = append(res, data)
		return 0, nil
	}
	handler.Unlock()

	// Start tcp server.
	go func() {
		defer wg.Done()

		conn, err := l.Accept()
		require.NoError(t, err)
		atomic.StoreInt32(&accepted, 1)
		for atomic.LoadInt32(&stop) == 1 {
		}
		conn.Close()
	}()

	// Wait for the mock functions to take effect.
	for atomic.LoadInt32(&accepted) != 1 {
	}

	// Enqueue some buffers.
	buf := msgpack.NewBufferedEncoder()
	_, err = buf.Buffer().Write([]byte{0x3, 0x4, 0x5})
	require.NoError(t, err)
	require.NoError(t, h.Handle(buf))

	// Wait for buffer to be re-enqueued.
	for atomic.LoadInt32(&count) < 2 {
	}

	// Expect the first buffer to be dropped.
	h.Close()
	require.Equal(t, [][]byte{[]byte{0x1, 0x2}}, res)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
}

func TestForwardHandlerClose(t *testing.T) {
	h, err := NewForwardHandler([]string{testFakeServerAddr}, testForwardHandlerOptions())
	require.NoError(t, err)

	// Close the handler sets the flag.
	h.Close()
	require.True(t, h.(*forwardHandler).closed)

	// Close the handler a second time is a no op.
	h.Close()
	require.True(t, h.(*forwardHandler).closed)
}

func TestTryConnectTimeout(t *testing.T) {
	errTimeout := errors.New("error timing out")
	h := &forwardHandler{}
	h.dialWithTimeoutFn = func(string, string, time.Duration) (net.Conn, error) {
		return nil, errTimeout
	}
	_, err := h.tryConnect(testFakeServerAddr)
	require.Equal(t, errTimeout, err)
}

func TestTryConnectSuccess(t *testing.T) {
	h := &forwardHandler{}
	h.dialWithTimeoutFn = func(string, string, time.Duration) (net.Conn, error) {
		return &net.TCPConn{}, nil
	}
	_, err := h.tryConnect(testFakeServerAddr)
	require.NoError(t, err)
}

func testForwardHandlerOptions() ForwardHandlerOptions {
	retrierOpts := xretry.NewOptions().
		SetInitialBackoff(time.Millisecond).
		SetBackoffFactor(1).
		SetForever(true)
	return NewForwardHandlerOptions().
		SetConnectionKeepAlive(true).
		SetConnectTimeout(100 * time.Millisecond).
		SetQueueSize(4096).
		SetReconnectRetrier(xretry.NewRetrier(retrierOpts))
}
