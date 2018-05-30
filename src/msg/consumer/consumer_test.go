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
	"net"
	"testing"

	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/protocol/proto"
	"github.com/m3db/m3x/pool"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var testMsg1 = msgpb.Message{
	Metadata: msgpb.Metadata{
		Shard: 100,
		Id:    200,
	},
	Value: []byte("foooooooo"),
}

var testMsg2 = msgpb.Message{
	Metadata: msgpb.Metadata{
		Shard: 0,
		Id:    45678,
	},
	Value: []byte("barrrrrrr"),
}

func TestConsumerWithMessagePool(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	l, err := NewListener("127.0.0.1:0", opts)
	require.NoError(t, err)
	defer l.Close()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	producer := proto.NewEncodeDecoder(conn, opts.EncodeDecoderOptions())

	c, err := l.Accept()
	require.NoError(t, err)

	err = producer.Encode(&testMsg1)
	require.NoError(t, err)

	m1, err := c.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Value, m1.Bytes())

	// Acking m1 making it available for reuse.
	m1.Ack()

	err = producer.Encode(&testMsg2)
	require.NoError(t, err)

	m2, err := c.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg2.Metadata, m2.(*message).Message.Metadata)
	require.Equal(t, testMsg2.Value, m2.Bytes())

	require.Equal(t, m1, m2)

	err = producer.Encode(&testMsg1)
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
	producer := proto.NewEncodeDecoder(conn, opts.EncodeDecoderOptions())

	c, err := l.Accept()
	require.NoError(t, err)

	err = producer.Encode(&testMsg1)
	require.NoError(t, err)

	m1, err := c.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Value, m1.Bytes())

	// Acking m1 making it available for reuse.
	m1.Ack()

	err = producer.Encode(&testMsg2)
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
	producer := proto.NewEncodeDecoder(conn, opts.EncodeDecoderOptions())

	c, err := l.Accept()
	require.NoError(t, err)

	mockEncdec := proto.NewMockEncodeDecoder(ctrl)
	cc := c.(*consumer)
	encdec := cc.encdec
	cc.encdec = mockEncdec

	err = producer.Encode(&testMsg1)
	require.NoError(t, err)

	mockEncdec.EXPECT().Decode(gomock.Any()).DoAndReturn(
		func(m proto.Unmarshaler) error {
			return encdec.Decode(m)
		},
	).Times(2)

	m, err := cc.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Value, m.Bytes())

	mockEncdec.EXPECT().Encode(gomock.Any()).Return(errors.New("mock encode err"))
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
	producer := proto.NewEncodeDecoder(conn, opts.EncodeDecoderOptions())

	c, err := l.Accept()
	require.NoError(t, err)

	mockEncdec := proto.NewMockEncodeDecoder(ctrl)

	cc := c.(*consumer)
	cc.encdec = mockEncdec

	err = producer.Encode(&testMsg1)
	require.NoError(t, err)

	mockEncdec.EXPECT().Decode(gomock.Any()).Return(errors.New("mock encode err"))

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
	producer := proto.NewEncodeDecoder(conn, opts.EncodeDecoderOptions())

	c, err := l.Accept()
	require.NoError(t, err)

	mockEncdec := proto.NewMockEncodeDecoder(ctrl)

	cc := c.(*consumer)
	encdec := cc.encdec
	cc.encdec = mockEncdec

	err = producer.Encode(&testMsg1)
	require.NoError(t, err)

	err = producer.Encode(&testMsg2)
	require.NoError(t, err)

	mockEncdec.EXPECT().Decode(gomock.Any()).DoAndReturn(
		func(m proto.Unmarshaler) error {
			return encdec.Decode(m)
		},
	).Times(2)

	m1, err := cc.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Value, m1.Bytes())

	m2, err := cc.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg2.Value, m2.Bytes())

	// First ack won't trigger encode because we buffer 2 acks before writing out.
	m1.Ack()

	// Second ack will trigger encode.
	mockEncdec.EXPECT().Encode(gomock.Any()).Return(nil)
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
	producer := proto.NewEncodeDecoder(conn, opts.EncodeDecoderOptions())

	c, err := l.Accept()
	require.NoError(t, err)

	mockEncdec := proto.NewMockEncodeDecoder(ctrl)

	cc := c.(*consumer)
	encdec := cc.encdec
	cc.encdec = mockEncdec

	err = producer.Encode(&testMsg1)
	require.NoError(t, err)

	err = producer.Encode(&testMsg2)
	require.NoError(t, err)

	mockEncdec.EXPECT().Decode(gomock.Any()).DoAndReturn(
		func(m proto.Unmarshaler) error {
			return encdec.Decode(m)
		},
	).Times(2)

	m1, err := cc.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Value, m1.Bytes())

	m2, err := cc.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg2.Value, m2.Bytes())

	mockEncdec.EXPECT().Encode(gomock.Any()).Return(nil)
	m1.Ack()

	cc.Close()
	// Second ack will not trigger encode since the consumer is closed.
	m2.Ack()
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
	producer := proto.NewEncodeDecoder(conn, opts.EncodeDecoderOptions())

	c, err := l.Accept()
	require.NoError(t, err)

	mockEncdec := proto.NewMockEncodeDecoder(ctrl)

	cc := c.(*consumer)
	encdec := cc.encdec
	cc.encdec = mockEncdec

	err = producer.Encode(&testMsg1)
	require.NoError(t, err)

	err = producer.Encode(&testMsg2)
	require.NoError(t, err)

	mockEncdec.EXPECT().Decode(gomock.Any()).DoAndReturn(
		func(m proto.Unmarshaler) error {
			return encdec.Decode(m)
		},
	)

	m1, err := cc.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg1.Value, m1.Bytes())

	m1.Ack()
	require.Equal(t, 1, len(cc.ackPb.Metadata))
	mockEncdec.EXPECT().Encode(gomock.Any()).Return(nil)
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
	encdec := proto.NewEncodeDecoder(conn, opts.EncodeDecoderOptions())

	err = encdec.Encode(&testMsg)
	require.NoError(t, err)

	c, err := l.Accept()
	require.NoError(t, err)
	defer c.Close()

	m, err := c.Message()
	require.NoError(t, err)
	require.Equal(t, testMsg.Value, m.Bytes())

	m.Ack()
	var ack msgpb.Ack
	err = encdec.Decode(&ack)
	require.NoError(t, err)
	require.Equal(t, 1, len(ack.Metadata))
	require.Equal(t, testMsg.Metadata, ack.Metadata[0])
}

func testOptions() Options {
	opts := NewOptions()
	return opts.
		SetAckBufferSize(1).
		SetMessagePoolOptions(pool.NewObjectPoolOptions().SetSize(1)).
		SetConnectionWriteBufferSize(1)
}
