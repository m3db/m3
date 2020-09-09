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

package consumer

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/protocol/proto"
	xio "github.com/m3db/m3/src/x/io"
	"github.com/m3db/m3/src/x/pool"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testMsg1 = msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 100,
			Id:    200,
		},
		Value: []byte("foooooooo"),
	}

	testMsg2 = msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 0,
			Id:    45678,
		},
		Value: []byte("barrrrrrr"),
	}
)

func TestConsumerWithMessagePool(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	l, err := NewListener("127.0.0.1:0", opts)
	require.NoError(t, err)
	defer l.Close()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	c, err := l.Accept()
	require.NoError(t, err)

	err = produce(conn, &testMsg1, opts)
	require.NoError(t, err)

	m1, err := c.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Value, m1.Bytes())

	// Acking m1 making it available for reuse.
	m1.Ack()

	err = produce(conn, &testMsg2, opts)
	require.NoError(t, err)

	m2, err := c.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg2.Metadata, m2.(*message).Message.Metadata)
	require.Equal(t, testMsg2.Value, m2.Bytes())

	require.Equal(t, m1, m2)

	err = produce(conn, &testMsg1, opts)
	require.NoError(t, err)

	m3, err := c.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Metadata, m3.(*message).Message.Metadata)
	require.Equal(t, testMsg1.Value, m3.Bytes())

	// m2 was not acked so m3 will alloc a new message from pool.
	require.NotEqual(t, m2, m3)
}

func TestConsumerAckReusedMessage(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions().SetAckBufferSize(100)
	l, err := NewListener("127.0.0.1:0", opts)
	require.NoError(t, err)
	defer l.Close()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	c, err := l.Accept()
	require.NoError(t, err)

	err = produce(conn, &testMsg1, opts)
	require.NoError(t, err)

	m1, err := c.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Value, m1.Bytes())

	// Acking m1 making it available for reuse.
	m1.Ack()

	err = produce(conn, &testMsg2, opts)
	require.NoError(t, err)

	m2, err := c.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg2.Metadata, m2.(*message).Message.Metadata)
	require.Equal(t, testMsg2.Value, m2.Bytes())

	require.Equal(t, m1, m2)
	m2.Ack()

	cc := c.(*consumer)
	require.Equal(t, 2, len(cc.ackPb.Metadata))
	require.Equal(t, testMsg1.Metadata, cc.ackPb.Metadata[0])
	require.Equal(t, testMsg2.Metadata, cc.ackPb.Metadata[1])
}

func TestConsumerAckError(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	l, err := NewListener("127.0.0.1:0", opts)
	require.NoError(t, err)
	defer l.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	c, err := l.Accept()
	require.NoError(t, err)

	mockEncoder := proto.NewMockEncoder(ctrl)
	cc := c.(*consumer)
	cc.encoder = mockEncoder

	err = produce(conn, &testMsg1, opts)
	require.NoError(t, err)

	m, err := cc.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Value, m.Bytes())

	mockEncoder.EXPECT().Encode(gomock.Any()).Return(errors.New("mock encode err"))
	m.Ack()

	_, err = cc.Message()
	require.Error(t, err)
}

func TestConsumerMessageError(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	l, err := NewListener("127.0.0.1:0", opts)
	require.NoError(t, err)
	defer l.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	c, err := l.Accept()
	require.NoError(t, err)

	mockDecoder := proto.NewMockDecoder(ctrl)
	cc := c.(*consumer)
	cc.decoder = mockDecoder

	err = produce(conn, &testMsg1, opts)
	require.NoError(t, err)

	mockDecoder.EXPECT().Decode(gomock.Any()).Return(errors.New("mock encode err"))

	_, err = cc.Message()
	require.Error(t, err)
}

func TestConsumerAckBuffer(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions().SetAckBufferSize(2)
	l, err := NewListener("127.0.0.1:0", opts)
	require.NoError(t, err)
	defer l.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	c, err := l.Accept()
	require.NoError(t, err)

	mockEncoder := proto.NewMockEncoder(ctrl)
	cc := c.(*consumer)
	cc.encoder = mockEncoder

	err = produce(conn, &testMsg1, opts)
	require.NoError(t, err)

	err = produce(conn, &testMsg2, opts)
	require.NoError(t, err)

	m1, err := cc.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Value, m1.Bytes())

	m2, err := cc.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg2.Value, m2.Bytes())

	// First ack won't trigger encode because we buffer 2 acks before writing out.
	m1.Ack()

	// Second ack will trigger encode.
	mockEncoder.EXPECT().Encode(gomock.Any())
	mockEncoder.EXPECT().Bytes()
	m2.Ack()
}

func TestConsumerAckAfterClosed(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions().SetAckBufferSize(1)
	l, err := NewListener("127.0.0.1:0", opts)
	require.NoError(t, err)
	defer l.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	c, err := l.Accept()
	require.NoError(t, err)

	mockEncoder := proto.NewMockEncoder(ctrl)
	cc := c.(*consumer)
	cc.encoder = mockEncoder

	err = produce(conn, &testMsg1, opts)
	require.NoError(t, err)

	err = produce(conn, &testMsg2, opts)
	require.NoError(t, err)

	m1, err := cc.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Value, m1.Bytes())

	m2, err := cc.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg2.Value, m2.Bytes())

	mockEncoder.EXPECT().Encode(gomock.Any())
	mockEncoder.EXPECT().Bytes()
	m1.Ack()

	cc.Close()
	// Second ack will not trigger encode since the consumer is closed.
	m2.Ack()
}

func TestConsumerTimeBasedFlush(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions().SetAckBufferSize(2)
	l, err := NewListener("127.0.0.1:0", opts)
	require.NoError(t, err)
	defer l.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	c, err := l.Accept()
	require.NoError(t, err)

	mockEncoder := proto.NewMockEncoder(ctrl)
	cc := c.(*consumer)
	cc.encoder = mockEncoder

	err = produce(conn, &testMsg1, opts)
	require.NoError(t, err)

	err = produce(conn, &testMsg2, opts)
	require.NoError(t, err)

	m1, err := cc.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Value, m1.Bytes())

	m1.Ack()
	require.Equal(t, 1, len(cc.ackPb.Metadata))

	mockEncoder.EXPECT().Encode(gomock.Any())
	mockEncoder.EXPECT().Bytes()
	cc.Init()
	cc.Close()
}

func TestConsumerFlushAcksOnClose(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions().SetAckBufferSize(2)
	l, err := NewListener("127.0.0.1:0", opts)
	require.NoError(t, err)
	defer l.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	c, err := l.Accept()
	require.NoError(t, err)
	c.Init()

	mockEncoder := proto.NewMockEncoder(ctrl)
	cc := c.(*consumer)
	cc.encoder = mockEncoder

	err = produce(conn, &testMsg1, opts)
	require.NoError(t, err)

	err = produce(conn, &testMsg2, opts)
	require.NoError(t, err)

	m1, err := cc.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Value, m1.Bytes())

	m1.Ack()
	require.Equal(t, 1, len(cc.ackPb.Metadata))

	mockEncoder.EXPECT().Encode(gomock.Any()).Return(nil)
	mockEncoder.EXPECT().Bytes()
	cc.Close()
}

func TestListenerMultipleConnection(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	l, err := NewListener("127.0.0.1:0", opts)
	require.NoError(t, err)
	defer l.Close()

	require.Equal(t, "tcp", l.Addr().Network())
	testProduceAndReceiveAck(t, testMsg1, l, opts)
	testProduceAndReceiveAck(t, testMsg2, l, opts)
}

func testProduceAndReceiveAck(t *testing.T, testMsg msgpb.Message, l Listener, opts Options) {
	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	produce(conn, &testMsg, opts)
	require.NoError(t, err)

	c, err := l.Accept()
	require.NoError(t, err)
	defer c.Close()

	m, err := c.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg.Value, m.Bytes())

	m.Ack()
	var ack msgpb.Ack
	err = proto.NewDecoder(conn, opts.DecoderOptions(), 10).Decode(&ack)
	require.NoError(t, err)
	require.Equal(t, 1, len(ack.Metadata))
	require.Equal(t, testMsg.Metadata, ack.Metadata[0])
}

func testOptions() Options {
	opts := NewOptions()
	return opts.
		SetAckBufferSize(1).
		SetAckFlushInterval(50 * time.Millisecond).
		SetMessagePoolOptions(MessagePoolOptions{
			PoolOptions: pool.NewObjectPoolOptions().SetSize(1),
		}).
		SetConnectionWriteBufferSize(1)
}

func testOptionsUsingSnappyCompression() Options {
	opts := NewOptions()
	rwOpts := xio.NewOptions().
		SetResettableReaderFn(xio.SnappyResettableReaderFn()).
		SetResettableWriterFn(xio.SnappyResettableWriterFn())
	return opts.
		SetAckBufferSize(1).
		SetAckFlushInterval(50 * time.Millisecond).
		SetMessagePoolOptions(MessagePoolOptions{
			PoolOptions: pool.NewObjectPoolOptions().SetSize(1),
		}).
		SetConnectionWriteBufferSize(1).
		SetCompression(xio.SnappyCompression).
		SetEncoderOptions(opts.EncoderOptions().SetRWOptions(rwOpts)).
		SetDecoderOptions(opts.DecoderOptions().SetRWOptions(rwOpts))
}

func produce(w io.Writer, m proto.Marshaler, opts Options) error {
	encoder := proto.NewEncoder(opts.EncoderOptions())
	err := encoder.Encode(m)
	if err != nil {
		return err
	}
	writerOpts := xio.ResettableWriterOptions{WriteBufferSize: opts.ConnectionWriteBufferSize()}
	writerFn := opts.EncoderOptions().RWOptions().ResettableWriterFn()(w, writerOpts)
	_, err = writerFn.Write(encoder.Bytes())
	err = writerFn.Flush()
	return err
}
