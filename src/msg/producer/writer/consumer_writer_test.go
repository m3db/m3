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
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/protocol/proto"
	xio "github.com/m3db/m3/src/x/io"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/retry"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
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

	testOpts := []Options{
		testOptions(),
		testOptionsUsingSnappyCompression(),
	}

	for _, opts := range testOpts {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer lis.Close()

		ctrl := xtest.NewController(t)
		defer ctrl.Finish()

		mockRouter := NewMockackRouter(ctrl)

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

func testOptions() Options {
	return NewOptions().
		SetTopicName("topicName").
		SetTopicWatchInitTimeout(100 * time.Millisecond).
		SetPlacementWatchInitTimeout(100 * time.Millisecond).
		SetMessagePoolOptions(pool.NewObjectPoolOptions().SetSize(1)).
		SetMessageQueueNewWritesScanInterval(100 * time.Millisecond).
		SetMessageQueueFullScanInterval(200 * time.Millisecond).
		SetMessageRetryOptions(retry.NewOptions().SetInitialBackoff(100 * time.Millisecond).SetMaxBackoff(500 * time.Millisecond)).
		SetAckErrorRetryOptions(retry.NewOptions().SetInitialBackoff(200 * time.Millisecond).SetMaxBackoff(time.Second)).
		SetConnectionOptions(testConnectionOptions())
}

func testOptionsUsingSnappyCompression() Options {
	return testOptions().SetConnectionOptions(testConnectionOptionsUsingSnappyCompression())
}

func testConnectionOptions() ConnectionOptions {
	return NewConnectionOptions().
		SetNumConnections(1).
		SetRetryOptions(retry.NewOptions().SetInitialBackoff(200 * time.Millisecond).SetMaxBackoff(time.Second)).
		SetFlushInterval(100 * time.Millisecond).
		SetResetDelay(100 * time.Millisecond)
}

func testConnectionOptionsUsingSnappyCompression() ConnectionOptions {
	return testConnectionOptions().SetCompression(xio.SnappyCompression)
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
