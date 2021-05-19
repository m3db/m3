// Copyright (c) 2018 Uber Technologies, Inc.
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

package client

import (
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/x/clock"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

const (
	testFakeServerAddr        = "nonexistent"
	testLocalServerAddr       = "127.0.0.1:0"
	testRandomSeeed           = 831992
	testMinSuccessfulTests    = 1000
	testReconnectThreshold    = 1024
	testMaxReconnectThreshold = 8096
)

var (
	errTestConnect = errors.New("connect error")
	errTestWrite   = errors.New("write error")
)

func TestConnectionDontReconnectProperties(t *testing.T) {
	props := testConnectionProperties()
	props.Property(
		`When the number of failures is less than or equal to the threshold and the time since last `+
			`connection is less than the maximum duration writes should:
	  - not attempt to reconnect
	  - increment the number of failures`,
		prop.ForAll(
			func(numFailures int32) (bool, error) {
				conn := newConnection(testFakeServerAddr, testConnectionOptions())
				conn.connectWithLockFn = func() error { return errTestConnect }
				conn.numFailures = int(numFailures)
				conn.threshold = testReconnectThreshold

				if err := conn.Write(nil); err != errNoActiveConnection {
					return false, fmt.Errorf("unexpected error: %v", err)
				}

				expected := int(numFailures + 1)
				if conn.numFailures != expected {
					return false, fmt.Errorf(
						"expected the number of failures to be: %v, but found: %v", expected, conn.numFailures,
					)
				}

				return true, nil
			},
			gen.Int32Range(0, testReconnectThreshold),
		))

	props.TestingRun(t)
}

func TestConnectionNumFailuresThresholdReconnectProperty(t *testing.T) {
	props := testConnectionProperties()
	props.Property(
		"When number of failures is greater than the threshold, it is multiplied",
		prop.ForAll(
			func(threshold int32) (bool, error) {
				conn := newConnection(testFakeServerAddr, testConnectionOptions())
				conn.connectWithLockFn = func() error { return errTestConnect }
				conn.threshold = int(threshold)
				conn.multiplier = 2
				conn.numFailures = conn.threshold + 1
				conn.maxThreshold = testMaxReconnectThreshold

				expectedNewThreshold := conn.threshold * conn.multiplier
				if expectedNewThreshold > conn.maxThreshold {
					expectedNewThreshold = conn.maxThreshold
				}
				if err := conn.Write(nil); !errors.Is(err, errTestConnect) {
					return false, fmt.Errorf("unexpected error: %w", err)
				}

				require.Equal(t, expectedNewThreshold, conn.threshold)
				return true, nil
			},
			gen.Int32Range(1, testMaxReconnectThreshold),
		))
	props.Property(
		"When the number of failures is greater than the threshold writes should attempt to reconnect",
		prop.ForAll(
			func(threshold int32) (bool, error) {
				conn := newConnection(testFakeServerAddr, testConnectionOptions())
				conn.connectWithLockFn = func() error { return errTestConnect }
				conn.threshold = int(threshold)
				conn.numFailures = conn.threshold + 1
				conn.maxThreshold = 2 * conn.numFailures

				if err := conn.Write(nil); !errors.Is(err, errTestConnect) {
					return false, fmt.Errorf("unexpected error: %w", err)
				}
				return true, nil
			},
			gen.Int32Range(1, testMaxReconnectThreshold),
		))
	props.Property(
		"When the number of failures is greater than the max threshold writes must not attempt to reconnect",
		prop.ForAll(
			func(threshold int32) (bool, error) {
				conn := newConnection(testFakeServerAddr, testConnectionOptions())
				conn.connectWithLockFn = func() error { return errTestConnect }
				// Exhausted max threshold
				conn.threshold = int(threshold)
				conn.maxThreshold = conn.threshold
				conn.numFailures = conn.maxThreshold + 1

				if err := conn.Write(nil); !errors.Is(err, errNoActiveConnection) {
					return false, fmt.Errorf("unexpected error: %w", err)
				}
				return true, nil
			},
			gen.Int32Range(1, testMaxReconnectThreshold),
		))
	props.Property(
		`When the number of failures is greater than the max threshold
		 but time since last connection attempt is greater than the maximum duration
		 then writes should attempt to reconnect`,
		prop.ForAll(
			func(delay int64) (bool, error) {
				conn := newConnection(testFakeServerAddr, testConnectionOptions())
				conn.connectWithLockFn = func() error { return errTestConnect }
				// Exhausted max threshold
				conn.threshold = 1
				conn.maxThreshold = conn.threshold
				conn.numFailures = conn.maxThreshold + 1

				now := time.Now()
				conn.nowFn = func() time.Time { return now }
				conn.lastConnectAttemptNanos = now.UnixNano() - delay
				conn.maxDuration = time.Duration(delay)

				if err := conn.Write(nil); !errors.Is(err, errTestConnect) {
					return false, fmt.Errorf("unexpected error: %w", err)
				}
				return true, nil
			},
			gen.Int64Range(1, math.MaxInt64),
		))

	props.TestingRun(t)
}

func TestConnectionMaxDurationReconnectProperty(t *testing.T) {
	props := testConnectionProperties()
	props.Property(
		"When the time since last connection is greater than the maximum duration writes should attempt to reconnect",
		prop.ForAll(
			func(delay int64) (bool, error) {
				conn := newConnection(testFakeServerAddr, testConnectionOptions())
				conn.connectWithLockFn = func() error { return errTestConnect }
				now := time.Now()
				conn.nowFn = func() time.Time { return now }
				conn.lastConnectAttemptNanos = now.UnixNano() - delay
				conn.maxDuration = time.Duration(delay)

				if err := conn.Write(nil); err != errTestConnect {
					return false, fmt.Errorf("unexpected error: %v", err)
				}
				return true, nil
			},
			gen.Int64Range(1, math.MaxInt64),
		))

	props.TestingRun(t)
}

func TestConnectionReconnectProperties(t *testing.T) {
	props := testConnectionProperties()
	props.Property(
		`When there is no active connection and a write cannot establish one it should:
		- set number of failures to threshold + 2
	  - update the threshold to be min(threshold*multiplier, maxThreshold)`,
		prop.ForAll(
			func(threshold, multiplier int32) (bool, error) {
				conn := newConnection(testFakeServerAddr, testConnectionOptions())
				conn.connectWithLockFn = func() error { return errTestConnect }
				conn.threshold = int(threshold)
				conn.numFailures = conn.threshold + 1
				conn.multiplier = int(multiplier)
				conn.maxThreshold = testMaxReconnectThreshold

				if err := conn.Write(nil); err != errTestConnect {
					return false, fmt.Errorf("unexpected error: %v", err)
				}

				if conn.numFailures != int(threshold+2) {
					return false, fmt.Errorf(
						"expected the number of failures to be %d, but found: %v", threshold+2, conn.numFailures,
					)
				}

				expected := int(threshold * multiplier)
				if expected > testMaxReconnectThreshold {
					expected = testMaxReconnectThreshold
				}

				if conn.threshold != expected {
					return false, fmt.Errorf(
						"expected the new threshold to be %v, but found: %v", expected, conn.threshold,
					)
				}

				return true, nil
			},
			gen.Int32Range(1, testMaxReconnectThreshold),
			gen.Int32Range(1, 16),
		))

	props.TestingRun(t)
}

func TestConnectionWriteSucceedsOnSecondAttempt(t *testing.T) {
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

func TestConnectionWriteFailsOnSecondAttempt(t *testing.T) {
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

	require.Equal(t, errTestConnect, conn.Write(nil))
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
	require.NotNil(t, conn.conn)

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

func testConnectionProperties() *gopter.Properties {
	params := gopter.DefaultTestParameters()
	params.Rng.Seed(testRandomSeeed)
	params.MinSuccessfulTests = testMinSuccessfulTests
	return gopter.NewProperties(params)
}
