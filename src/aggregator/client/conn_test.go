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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/x/clock"
	xtls "github.com/m3db/m3/src/x/tls"

	"github.com/golang/mock/gomock"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/assert"
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
				conn := newConnection(testFakeServerAddr,
					testConnectionOptions().
						SetMaxReconnectDuration(time.Duration(math.MaxInt64)),
				)
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
				conn.maxDuration = math.MaxInt64
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

type keepAlivableConn struct {
	net.Conn
	keepAlivable
}

func TestConnectWithCustomDialer(t *testing.T) {
	testData := []byte("foobar")
	testConnectionTimeout := 5 * time.Second

	testWithConn := func(t *testing.T, netConn net.Conn) {
		type args struct {
			Ctx     context.Context
			Network string
			Address string
		}
		var capturedArgs args
		dialer := func(ctx context.Context, network string, address string) (net.Conn, error) {
			capturedArgs = args{
				Ctx:     ctx,
				Network: network,
				Address: address,
			}
			return netConn, nil
		}
		opts := testConnectionOptions().
			SetContextDialer(dialer).
			SetConnectionTimeout(testConnectionTimeout)
		addr := "127.0.0.1:5555"

		conn := newConnection(addr, opts)
		start := time.Now()
		require.NoError(t, conn.Write(testData))

		assert.Equal(t, addr, capturedArgs.Address)
		assert.Equal(t, tcpProtocol, capturedArgs.Network)

		deadline, ok := capturedArgs.Ctx.Deadline()
		require.True(t, ok)
		// Start is taken *before* we try to connect, so the deadline must = start + <some_time> + testDialTimeout.
		// Therefore deadline - start >= testDialTimeout.
		assert.True(t, deadline.Sub(start) >= testConnectionTimeout)
	}

	t.Run("non keep alivable conn", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockConn := NewMockConn(ctrl)

		mockConn.EXPECT().Write(testData)
		mockConn.EXPECT().SetWriteDeadline(gomock.Any())
		testWithConn(t, mockConn)
	})

	t.Run("keep alivable conn", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockConn := NewMockConn(ctrl)

		mockConn.EXPECT().Write(testData)
		mockConn.EXPECT().SetWriteDeadline(gomock.Any())

		mockKeepAlivable := NewMockkeepAlivable(ctrl)
		mockKeepAlivable.EXPECT().SetKeepAlive(true)

		testWithConn(t, keepAlivableConn{
			Conn:         mockConn,
			keepAlivable: mockKeepAlivable,
		})
	})
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

func TestTLSConnectWriteToServer(t *testing.T) {
	data := []byte("foobar")

	// Start tls server.
	var wg sync.WaitGroup
	wg.Add(1)

	serverCert, err := tls.LoadX509KeyPair("./testdata/server.crt", "./testdata/server.key")
	require.NoError(t, err)
	certPool := x509.NewCertPool()
	certs, err := os.ReadFile("./testdata/rootCA.crt")
	require.NoError(t, err)
	certPool.AppendCertsFromPEM(certs)
	l, err := tls.Listen(tcpProtocol, testLocalServerAddr, &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    certPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	})
	t.Cleanup(func() { l.Close() }) // nolint: errcheck
	require.NoError(t, err)
	serverAddr := l.Addr().String()

	go func() {
		defer wg.Done()

		// Ignore the first testing connection.
		conn, err := l.Accept()
		require.NoError(t, err)
		tlsConn, ok := conn.(*tls.Conn)
		require.True(t, ok)
		err = tlsConn.Handshake()
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

	clientCert, err := tls.LoadX509KeyPair("./testdata/client.crt", "./testdata/client.key")
	require.NoError(t, err)
	// Wait until the server starts up.
	dialer := net.Dialer{Timeout: time.Minute}
	// #nosec G402
	testConn, err := tls.DialWithDialer(&dialer, tcpProtocol, serverAddr, &tls.Config{
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{clientCert},
		RootCAs:            certPool,
	})
	require.NoError(t, err)
	require.NoError(t, testConn.Close())

	// Create a new connection and assert we can write successfully.
	opts := testTLSConnectionOptions().SetInitReconnectThreshold(0)
	conn := newConnection(serverAddr, opts)
	require.NoError(t, conn.Write(data))
	require.Equal(t, 0, conn.numFailures)
	require.NotNil(t, conn.conn)

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

func testTLSConnectionOptions() ConnectionOptions {
	tlsOptions := xtls.NewOptions().
		SetClientEnabled(true).
		SetInsecureSkipVerify(true).
		SetCAFile("./testdata/rootCA.crt").
		SetCertFile("./testdata/client.crt").
		SetKeyFile("./testdata/client.key").
		SetCertificatesTTL(time.Second)
	return testConnectionOptions().SetTLSOptions(tlsOptions)
}

func testConnectionProperties() *gopter.Properties {
	params := gopter.DefaultTestParameters()
	params.Rng.Seed(testRandomSeeed)
	params.MinSuccessfulTests = testMinSuccessfulTests
	return gopter.NewProperties(params)
}
