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

	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/protocol/proto"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var testMsg = msgpb.Message{
	Metadata: msgpb.Metadata{
		Shard: 100,
		Id:    200,
	},
	Value: []byte("foooooooo"),
}

func TestNewConsumerWriter(t *testing.T) {
	defer leaktest.Check(t)()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRouter := NewMockackRouter(ctrl)
	opts := testOptions()
	w := newConsumerWriter(lis.Addr().String(), mockRouter, opts, testConsumerWriterMetrics()).(*consumerWriterImpl)
	require.Equal(t, 0, len(w.resetCh))

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncodeDecoderOptions())
		wg.Done()
	}()

	require.NoError(t, w.Write(&testMsg))

	wg.Add(1)
	mockRouter.EXPECT().
		Ack(newMetadataFromProto(testMsg.Metadata)).
		Do(func(interface{}) { wg.Done() }).
		Return(nil)

	w.Init()
	wg.Wait()

	w.Close()
}

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

	w.notifyReset()
	require.Equal(t, 1, len(w.resetCh))
	require.True(t, w.resetTooSoon())

	now := time.Now()
	w.nowFn = func() time.Time { return now.Add(1 * time.Hour) }
	require.Equal(t, 1, len(w.resetCh))
	require.False(t, w.resetTooSoon())
	require.NoError(t, w.resetWithConnectFn(w.connectFn))
	require.Equal(t, 1, called)
	require.Equal(t, 1, len(w.resetCh))

	// Reset won't do anything as it is too soon since last reset.
	require.True(t, w.resetTooSoon())

	w.nowFn = func() time.Time { return now.Add(2 * time.Hour) }
	require.False(t, w.resetTooSoon())
	require.NoError(t, w.resetWithConnectFn(w.connectFn))
	require.Equal(t, 2, called)
}

func TestConsumerWriterResetConnection(t *testing.T) {
	w := newConsumerWriter("badAddress", nil, testOptions(), testConsumerWriterMetrics()).(*consumerWriterImpl)
	require.Equal(t, 1, len(w.resetCh))
	err := w.Write(&testMsg)
	require.Error(t, err)
	require.Equal(t, errInvalidConnection, err)

	var called int
	conn := new(net.TCPConn)
	w.connectFn = func(addr string) (io.ReadWriteCloser, error) {
		called++
		require.Equal(t, "badAddress", addr)
		return conn, nil
	}
	w.resetWithConnectFn(w.connectWithRetry)
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
	err := w.Write(&testMsg)
	require.Error(t, err)
	require.Equal(t, errInvalidConnection, err)
	require.Equal(t, 0, len(w.resetCh))
	w.validConn.Store(true)
	err = w.Write(&testMsg)
	require.NoError(t, err)
	for {
		// The writer will need to wait until buffered size to try the flush
		// and then realize the connection is broken.
		err = w.Write(&testMsg)
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
	err := w.Write(&testMsg)
	require.Error(t, err)
	require.Equal(t, errInvalidConnection, err)
	require.Equal(t, 0, len(w.resetCh))
	w.validConn.Store(true)

	// The write will be buffered in the bufio.Writer, and will
	// not return err because it has not tried to flush yet.
	require.NoError(t, w.Write(&testMsg))

	require.Error(t, w.rw.Flush())

	// Flush err will be stored in bufio.Writer, the next time
	// Write is called, the err will be returned.
	err = w.Write(&testMsg)
	require.Error(t, err)
	require.Equal(t, errInvalidConnection, err)
	require.Equal(t, 1, len(w.resetCh))
}

func TestConsumerWriterReadErrorTriggerReset(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	w := newConsumerWriter("badAddr", nil, opts, testConsumerWriterMetrics()).(*consumerWriterImpl)
	<-w.resetCh
	w.validConn.Store(true)
	require.Equal(t, 0, len(w.resetCh))
	err := w.readAcks()
	require.Error(t, err)
	require.Equal(t, errInvalidConnection, err)
	require.Equal(t, 1, len(w.resetCh))
	w.Close()
}

func TestAutoReset(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
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
	require.Error(t, w.Write(&testMsg))

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	go func() {
		testConsumeAndAckOnConnection(t, serverConn, opts.EncodeDecoderOptions())
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

	var u uninitializedReadWriter
	for {
		w.encodeLock.Lock()
		c := w.conn
		w.encodeLock.Unlock()
		if c != u {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, w.Write(&testMsg))
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

	w := newConsumerWriter(lis.Addr().String(), nil, testOptions(), testConsumerWriterMetrics()).(*consumerWriterImpl)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		require.Error(t, w.encdec.Decode(&testMsg))
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

	w := newConsumerWriter(lis.Addr().String(), nil, testOptions(), testConsumerWriterMetrics()).(*consumerWriterImpl)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		w.decodeLock.Lock()
		require.Error(t, w.encdec.Decode(&testMsg))
		w.decodeLock.Unlock()
	}()
	wg.Wait()
	time.Sleep(time.Second)
	w.reset(new(net.TCPConn))
}

func testOptions() Options {
	return NewOptions().
		SetTopicName("topicName").
		SetTopicWatchInitTimeout(100 * time.Millisecond).
		SetPlacementWatchInitTimeout(100 * time.Millisecond).
		SetMessagePoolOptions(pool.NewObjectPoolOptions().SetSize(1)).
		SetMessageQueueScanInterval(100 * time.Millisecond).
		SetMessageRetryOptions(retry.NewOptions().SetInitialBackoff(100 * time.Millisecond).SetMaxBackoff(500 * time.Millisecond)).
		SetAckErrorRetryOptions(retry.NewOptions().SetInitialBackoff(200 * time.Millisecond).SetMaxBackoff(time.Second)).
		SetConnectionOptions(testConnectionOptions())
}

func testConnectionOptions() ConnectionOptions {
	return NewConnectionOptions().
		SetRetryOptions(retry.NewOptions().SetInitialBackoff(200 * time.Millisecond).SetMaxBackoff(time.Second)).
		SetFlushInterval(100 * time.Millisecond).
		SetResetDelay(100 * time.Millisecond)
}

func testConsumeAndAckOnConnection(
	t *testing.T,
	conn net.Conn,
	opts proto.EncodeDecoderOptions,
) {
	server := proto.NewEncodeDecoder(
		conn,
		opts,
	)

	var msg msgpb.Message
	assert.NoError(t, server.Decode(&msg))

	assert.NoError(t, server.Encode(&msgpb.Ack{
		Metadata: []msgpb.Metadata{
			msg.Metadata,
		},
	}))
}

func testConsumeAndAckOnConnectionListener(
	t *testing.T,
	lis net.Listener,
	opts proto.EncodeDecoderOptions,
) {
	conn, err := lis.Accept()
	require.NoError(t, err)
	defer conn.Close()

	testConsumeAndAckOnConnection(t, conn, opts)
}

func testConsumerWriterMetrics() consumerWriterMetrics {
	return newConsumerWriterMetrics(tally.NoopScope)
}
