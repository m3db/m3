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

package msgpack

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3x/clock"

	"github.com/stretchr/testify/require"
)

const (
	testFakeServerAddr  = "nonexistent"
	testLocalServerAddr = "127.0.0.1:0"
)

var (
	errTestConnect = errors.New("connect error")
	errTestWrite   = errors.New("write error")
)

func TestConnectionWriteNoReconnect(t *testing.T) {
	conn := newConnection(testFakeServerAddr, testConnectionOptions())
	conn.connectWithLockFn = func() error { return errTestConnect }

	require.Equal(t, errNoActiveConnection, conn.Write(nil))
	require.Equal(t, 1, conn.numFailures)
}

func TestConnectionWriteReconnectMultiplyThreshold(t *testing.T) {
	conn := newConnection(testFakeServerAddr, testConnectionOptions())
	conn.numFailures = 2
	conn.connectWithLockFn = func() error { return errTestConnect }

	require.Equal(t, errNoActiveConnection, conn.Write(nil))
	require.Equal(t, 3, conn.numFailures)
	require.Equal(t, 4, conn.threshold)
}

func TestConnectionWriteReconnectMaxThresholdReached(t *testing.T) {
	conn := newConnection(testFakeServerAddr, testConnectionOptions())
	conn.numFailures = 4
	conn.threshold = 3
	conn.connectWithLockFn = func() error { return errTestConnect }

	require.Equal(t, errNoActiveConnection, conn.Write(nil))
	require.Equal(t, 5, conn.numFailures)
	require.Equal(t, 6, conn.threshold)
}

func TestConnectionWriteReconnectSuccessWriteSuccess(t *testing.T) {
	conn := newConnection(testFakeServerAddr, testConnectionOptions())
	conn.numFailures = 3
	conn.connectWithLockFn = func() error { return nil }
	conn.writeWithLockFn = func([]byte) error { return nil }

	require.NoError(t, conn.Write(nil))
	require.Equal(t, 0, conn.numFailures)
	require.Equal(t, 2, conn.threshold)
}

func TestConnectionWriteReconnectSuccessWriteFailReconnectSuccess(t *testing.T) {
	conn := newConnection(testFakeServerAddr, testConnectionOptions())
	conn.numFailures = 3
	conn.connectWithLockFn = func() error { return nil }
	var count int
	conn.writeWithLockFn = func([]byte) error {
		count++
		if count == 1 {
			return errTestWrite
		}
		return nil
	}

	require.NoError(t, conn.Write(nil))
	require.Equal(t, 0, conn.numFailures)
	require.Equal(t, 2, conn.threshold)
}

func TestConnectionWriteReconnectSuccessWriteFailReconnectFail(t *testing.T) {
	conn := newConnection(testFakeServerAddr, testConnectionOptions())
	conn.numFailures = 3
	conn.writeWithLockFn = func([]byte) error { return errTestWrite }
	var count int
	conn.connectWithLockFn = func() error {
		count++
		if count == 1 {
			return nil
		}
		return errTestConnect
	}

	require.Equal(t, errTestWrite, conn.Write(nil))
	require.Equal(t, 1, conn.numFailures)
	require.Equal(t, 2, conn.threshold)
}

func TestConnectWriteToServer(t *testing.T) {
	data := []byte("foobar")

	// Start tcp server.
	var wg sync.WaitGroup
	wg.Add(1)

	l, err := net.Listen(tcpProtocol, testLocalServerAddr)
	require.NoError(t, err)
	serverAddr := l.Addr().String()

	go func() {
		defer wg.Done()

		// Ignore the first testing connection.
		conn, err := l.Accept()
		require.NoError(t, err)
		require.NoError(t, conn.Close())

		// Read from the second connection.
		conn, err = l.Accept()
		require.NoError(t, err)
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		require.NoError(t, err)
		require.Equal(t, data, buf[:n])
		conn.Close() // nolint: errcheck
	}()

	// Wait until the server starts up.
	testConn, err := net.DialTimeout(tcpProtocol, serverAddr, time.Minute)
	require.NoError(t, err)
	require.NoError(t, testConn.Close())

	// Create a new connection and assert we can write successfully.
	opts := testConnectionOptions().SetInitReconnectThreshold(0)
	conn := newConnection(serverAddr, opts)
	require.NoError(t, conn.Write(data))
	require.Equal(t, 0, conn.numFailures)

	// Stop the server.
	l.Close() // nolint: errcheck
	wg.Wait()

	// Close the connection
	conn.Close()
	require.Nil(t, conn.conn)
}

func testConnectionOptions() ConnectionOptions {
	return NewConnectionOptions().
		SetClockOptions(clock.NewOptions()).
		SetConnectionKeepAlive(true).
		SetConnectionTimeout(100 * time.Millisecond).
		SetInitReconnectThreshold(2).
		SetMaxReconnectThreshold(6).
		SetReconnectThresholdMultiplier(2).
		SetWriteTimeout(100 * time.Millisecond)
}
