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

package writer

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/retry"
	xtest "github.com/m3db/m3/src/x/test"
)

var (
	testMsg = msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 100,
			Id:    200,
		},
		Value: []byte("foooooooo"),
	}

	testEncoder = proto.NewEncoder(nil)
)

func TestNewConsumerWriter(t *testing.T) {
	defer leaktest.Check(t)()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockRouter := NewMockackRouter(ctrl)

	opts := testOptions()

	w := newConsumerWriter(lis.Addr().String(), mockRouter, opts, testConsumerWriterMetrics()).(*consumerWriterImpl)
	require.Equal(t, 0, len(w.resetCh))

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncoderOptions(), opts.DecoderOptions())
		wg.Done()
	}()

	require.NoError(t, write(w, &testMsg))

	wg.Add(1)
	mockRouter.EXPECT().
		Ack(newMetadataFromProto(testMsg.Metadata)).
		Do(func(interface{}) { wg.Done() }).
		Return(nil)

	w.Init()
	wg.Wait()

	w.Close()
	// Make sure the connection is closed after closing the consumer writer.
	_, err = w.writeState.conns[0].conn.Read([]byte{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "closed network connection")
}

// TODO: tests for multiple connection writers.

func TestConsumerWriterSignalResetConnection(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	w := newConsumerWriter(lis.Addr().String(), nil, testOptions(), testConsumerWriterMetrics()).(*consumerWriterImpl)
	require.Equal(t, 0, len(w.resetCh))

	var called int
	w.connectFn = func(addr string) (io.ReadWriteCloser, error) {
		called++
		return uninitializedReadWriter{}, nil
	}

	w.notifyReset(nil)
	require.Equal(t, 1, len(w.resetCh))
	require.True(t, w.resetTooSoon())

	now := time.Now()
	w.nowFn = func() time.Time { return now.Add(1 * time.Hour) }
	require.Equal(t, 1, len(w.resetCh))
	require.False(t, w.resetTooSoon())
	require.NoError(t, w.resetWithConnectFn(w.newConnectFn(connectOptions{retry: false})))
	require.Equal(t, 1, called)
	require.Equal(t, 1, len(w.resetCh))

	// Reset won't do anything as it is too soon since last reset.
	require.True(t, w.resetTooSoon())

	w.nowFn = func() time.Time { return now.Add(2 * time.Hour) }
	require.False(t, w.resetTooSoon())
	require.NoError(t, w.resetWithConnectFn(w.newConnectFn(connectOptions{retry: false})))
	require.Equal(t, 2, called)
}

func TestConsumerWriterResetConnection(t *testing.T) {
	w := newConsumerWriter("badAddress", nil, testOptions(), testConsumerWriterMetrics()).(*consumerWriterImpl)
	require.Equal(t, 1, len(w.resetCh))
	err := write(w, &testMsg)
	require.Error(t, err)
	require.Equal(t, errInvalidConnection, err)

	var called int
	conn := new(net.TCPConn)
	w.connectFn = func(addr string) (io.ReadWriteCloser, error) {
		called++
		require.Equal(t, "badAddress", addr)
		return conn, nil
	}
	w.resetWithConnectFn(w.newConnectFn(connectOptions{retry: true}))
	require.Equal(t, 1, called)
}

func TestConsumerWriterRetryableConnectionBackgroundReset(t *testing.T) {
	w := newConsumerWriter("badAddress", nil, testOptions(), testConsumerWriterMetrics()).(*consumerWriterImpl)
	require.Equal(t, 1, len(w.resetCh))

	var lock sync.Mutex
	var called int
	conn := new(net.TCPConn)
	w.connectFn = func(addr string) (io.ReadWriteCloser, error) {
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

func TestConsumerWriterWriteErrorTriggerReset(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	w := newConsumerWriter("badAddr", nil, opts.SetConnectionOptions(
		opts.ConnectionOptions().SetWriteBufferSize(1000),
	), testConsumerWriterMetrics()).(*consumerWriterImpl)
	<-w.resetCh
	require.Equal(t, 0, len(w.resetCh))
	err := write(w, &testMsg)
	require.Error(t, err)
	require.Equal(t, errInvalidConnection, err)
	require.Equal(t, 0, len(w.resetCh))
	w.writeState.Lock()
	w.writeState.validConns = true
	w.writeState.Unlock()

	err = write(w, &testMsg)
	require.NoError(t, err)
	for {
		// The writer will need to wait until buffered size to try the flush
		// and then realize the connection is broken.
		err = write(w, &testMsg)
		if err != nil {
			break
		}
	}
	require.Error(t, err)
	require.Equal(t, errInvalidConnection, err)
	require.Equal(t, 1, len(w.resetCh))
}

func TestConsumerWriterFlushWriteAfterFlushErrorTriggerReset(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	w := newConsumerWriter("badAddr", nil, opts.SetConnectionOptions(
		opts.ConnectionOptions().SetWriteBufferSize(1000),
	), testConsumerWriterMetrics()).(*consumerWriterImpl)
	<-w.resetCh
	require.Equal(t, 0, len(w.resetCh))
	err := write(w, &testMsg)
	require.Error(t, err)
	require.Equal(t, errInvalidConnection, err)
	require.Equal(t, 0, len(w.resetCh))
	w.writeState.Lock()
	w.writeState.validConns = true
	w.writeState.Unlock()

	// The write will be buffered in the bufio.Writer, and will
	// not return err because it has not tried to flush yet.
	require.NoError(t, write(w, &testMsg))

	w.writeState.Lock()
	require.Error(t, w.writeState.conns[0].w.Flush())
	w.writeState.Unlock()

	// Flush err will be stored in bufio.Writer, the next time
	// Write is called, the err will be returned.
	err = write(w, &testMsg)
	require.Error(t, err)
	require.Equal(t, errInvalidConnection, err)
	require.Equal(t, 1, len(w.resetCh))
}

func TestConsumerWriterReadErrorTriggerReset(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	w := newConsumerWriter("badAddr", nil, opts, testConsumerWriterMetrics()).(*consumerWriterImpl)
	<-w.resetCh
	w.writeState.Lock()
	w.writeState.validConns = true
	w.writeState.Unlock()
	require.Equal(t, 0, len(w.resetCh))
	err := w.readAcks(0)
	require.Error(t, err)
	require.Equal(t, errInvalidConnection, err)
	require.Equal(t, 1, len(w.resetCh))
	w.Close()
}

func TestAutoReset(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockRouter := NewMockackRouter(ctrl)

	opts := testOptions()

	w := newConsumerWriter(
		"badAddress",
		mockRouter,
		opts,
		testConsumerWriterMetrics(),
	).(*consumerWriterImpl)
	require.Equal(t, 1, len(w.resetCh))
	require.Error(t, write(w, &testMsg))

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	go func() {
		testConsumeAndAckOnConnection(t, serverConn, opts.EncoderOptions(), opts.DecoderOptions())
	}()

	w.connectFn = func(addr string) (io.ReadWriteCloser, error) {
		return clientConn, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	mockRouter.EXPECT().
		Ack(newMetadataFromProto(testMsg.Metadata)).
		Do(func(interface{}) { wg.Done() }).
		Return(nil)

	w.Init()

	start := time.Now()
	for time.Since(start) < 15*time.Second {
		w.writeState.Lock()
		validConns := w.writeState.validConns
		w.writeState.Unlock()
		if validConns {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, write(w, &testMsg))
	wg.Wait()

	w.Close()
}

func TestConsumerWriterClose(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	w := newConsumerWriter(lis.Addr().String(), nil, nil, testConsumerWriterMetrics()).(*consumerWriterImpl)
	require.Equal(t, 0, len(w.resetCh))
	w.Close()
	// Safe to close again.
	w.Close()
	_, ok := <-w.doneCh
	require.False(t, ok)
}

func TestConsumerWriterCloseWhileDecoding(t *testing.T) {
	defer leaktest.Check(t)()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	opts := testOptions()

	w := newConsumerWriter(lis.Addr().String(), nil, opts, testConsumerWriterMetrics()).(*consumerWriterImpl)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		require.Error(t, w.writeState.conns[0].decoder.Decode(&testMsg))
	}()
	wg.Wait()
	time.Sleep(time.Second)
	w.Close()
}

func TestConsumerWriterResetWhileDecoding(t *testing.T) {
	defer leaktest.Check(t)()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	opts := testOptions()

	w := newConsumerWriter(lis.Addr().String(), nil, opts, testConsumerWriterMetrics()).(*consumerWriterImpl)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()

		w.writeState.Lock()
		conn := w.writeState.conns[0]
		w.writeState.Unlock()

		require.Error(t, conn.decoder.Decode(&testMsg))
	}()
	wg.Wait()
	time.Sleep(time.Second)
	w.reset(resetOptions{
		connections: []io.ReadWriteCloser{new(net.TCPConn)},
		at:          w.nowFn(),
		validConns:  true,
	})
}

// Interface solely for mocking.
//nolint:deadcode,unused
type contextDialer interface {
	DialContext(ctx context.Context, network string, addr string) (net.Conn, error)
}

type keepAlivableConn struct {
	net.Conn
	keepAlivable
}

func TestConsumerWriter_connectNoRetry(t *testing.T) {
	type testDeps struct {
		Ctrl       *gomock.Controller
		MockDialer *MockcontextDialer
		Listener   net.Listener
	}

	newTestWriter := func(deps testDeps, opts Options) *consumerWriterImpl {
		return newConsumerWriter(
			deps.Listener.Addr().String(),
			nil,
			opts,
			testConsumerWriterMetrics(),
		).(*consumerWriterImpl)
	}

	mustClose := func(t *testing.T, c io.ReadWriteCloser) {
		require.NoError(t, c.Close())
	}

	setup := func(t *testing.T) testDeps {
		ctrl := gomock.NewController(t)

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, lis.Close())
		})

		return testDeps{
			Ctrl:       ctrl,
			Listener:   lis,
			MockDialer: NewMockcontextDialer(ctrl),
		}
	}
	type dialArgs struct {
		Ctx     context.Context
		Network string
		Addr    string
	}

	// Other tests in this file cover the case where dialer isn't set explicitly (default).
	t.Run("uses net.Dialer where dialer is unset", func(t *testing.T) {
		defer leaktest.Check(t)()
		tdeps := setup(t)
		opts := testOptions()
		w := newTestWriter(tdeps, opts.SetConnectionOptions(opts.ConnectionOptions().SetContextDialer(nil)))
		conn, err := w.connectNoRetryWithTimeout(tdeps.Listener.Addr().String())
		require.NoError(t, err)
		defer mustClose(t, conn)

		_, err = conn.Write([]byte("test"))
		require.NoError(t, err)
	})
	t.Run("uses dialer and respects timeout", func(t *testing.T) {
		defer leaktest.Check(t)()

		tdeps := setup(t)
		var capturedArgs dialArgs
		tdeps.MockDialer.EXPECT().DialContext(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, network string, addr string) (net.Conn, error) {
				capturedArgs.Ctx = ctx
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		).MinTimes(1)

		const testDialTimeout = 45 * time.Second
		opts := testOptions()
		opts = opts.SetConnectionOptions(opts.ConnectionOptions().
			SetContextDialer(tdeps.MockDialer.DialContext).
			SetDialTimeout(testDialTimeout),
		)

		start := time.Now()
		w := newTestWriter(tdeps, opts)
		conn, err := w.connectNoRetry(tdeps.Listener.Addr().String())

		require.NoError(t, err)
		defer mustClose(t, conn)

		deadline, ok := capturedArgs.Ctx.Deadline()
		require.True(t, ok)
		// Start is taken *before* we try to connect, so the deadline must = start + <some_time> + testDialTimeout.
		// Therefore deadline - start >= testDialTimeout.
		assert.True(t, deadline.Sub(start) >= testDialTimeout)
	})

	t.Run("sets KeepAlive where possible", func(t *testing.T) {
		tdeps := setup(t)
		// Deep mocking here is solely because it's not easy to get the keep alive off an actual TCP connection
		// (have to drop down to the syscall layer).
		const testKeepAlive = 56 * time.Minute
		mockConn := NewMockkeepAlivable(tdeps.Ctrl)
		mockConn.EXPECT().SetKeepAlivePeriod(testKeepAlive).Times(2)
		mockConn.EXPECT().SetKeepAlive(true).Times(2)

		tdeps.MockDialer.EXPECT().DialContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(keepAlivableConn{
			keepAlivable: mockConn,
		}, nil).Times(2)

		opts := testOptions()
		opts = opts.SetConnectionOptions(opts.ConnectionOptions().
			SetKeepAlivePeriod(testKeepAlive).
			SetContextDialer(tdeps.MockDialer.DialContext),
		)
		w := newTestWriter(tdeps, opts)
		_, err := w.connectNoRetryWithTimeout("foobar")
		require.NoError(t, err)
	})

	t.Run("handles non TCP connections gracefully", func(t *testing.T) {
		tdeps := setup(t)
		tdeps.MockDialer.EXPECT().DialContext(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, network string, addr string) (net.Conn, error) {
				srv, client := net.Pipe()
				require.NoError(t, srv.Close())
				return client, nil
			},
		).MinTimes(1)

		defer leaktest.Check(t)()

		opts := testOptions()
		opts = opts.SetConnectionOptions(opts.ConnectionOptions().
			SetContextDialer(tdeps.MockDialer.DialContext),
		)

		w := newConsumerWriter(
			"foobar",
			nil,
			opts,
			testConsumerWriterMetrics(),
		).(*consumerWriterImpl)
		conn, err := w.connectNoRetryWithTimeout("foobar")
		require.NoError(t, err)
		defer mustClose(t, conn)

		_, isTCPConn := conn.Conn.(*net.TCPConn)
		assert.False(t, isTCPConn)
	})
}

func testOptions() Options {
	return NewOptions().
		SetTopicName("topicName").
		SetTopicWatchInitTimeout(100 * time.Millisecond).
		SetPlacementWatchInitTimeout(100 * time.Millisecond).
		SetMessagePoolOptions(pool.NewObjectPoolOptions().SetSize(1)).
		SetMessageQueueNewWritesScanInterval(100 * time.Millisecond).
		SetMessageQueueFullScanInterval(200 * time.Millisecond).
		SetMessageRetryNanosFn(
			NextRetryNanosFn(
				retry.NewOptions().
					SetInitialBackoff(100 * time.Millisecond).
					SetMaxBackoff(500 * time.Millisecond),
			),
		).
		SetAckErrorRetryOptions(retry.NewOptions().SetInitialBackoff(200 * time.Millisecond).SetMaxBackoff(time.Second)).
		SetConnectionOptions(testConnectionOptions())
}

func testConnectionOptions() ConnectionOptions {
	return NewConnectionOptions().
		SetNumConnections(1).
		SetRetryOptions(retry.NewOptions().SetInitialBackoff(200 * time.Millisecond).SetMaxBackoff(time.Second)).
		SetFlushInterval(100 * time.Millisecond).
		SetResetDelay(100 * time.Millisecond)
}

func testConsumeAndAckOnConnection(
	t *testing.T,
	conn net.Conn,
	encOpts proto.Options,
	decOpts proto.Options,
) {
	serverEncoder := proto.NewEncoder(encOpts)
	serverDecoder := proto.NewDecoder(conn, decOpts, 10)
	var msg msgpb.Message
	assert.NoError(t, serverDecoder.Decode(&msg))

	err := serverEncoder.Encode(&msgpb.Ack{
		Metadata: []msgpb.Metadata{
			msg.Metadata,
		},
	})
	assert.NoError(t, err)
	_, err = conn.Write(serverEncoder.Bytes())
	assert.NoError(t, err)
}

func testConsumeAndAckOnConnectionListener(
	t *testing.T,
	lis net.Listener,
	encOpts proto.Options,
	decOpts proto.Options,
) {
	conn, err := lis.Accept()
	require.NoError(t, err)
	defer conn.Close()

	testConsumeAndAckOnConnection(t, conn, encOpts, decOpts)
}

func testConsumerWriterMetrics() consumerWriterMetrics {
	return newConsumerWriterMetrics(tally.NoopScope)
}

func write(w consumerWriter, m proto.Marshaler) error {
	err := testEncoder.Encode(m)
	if err != nil {
		return err
	}
	return w.Write(0, testEncoder.Bytes())
}
