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
	"bufio"
	"net"
	"sync"

	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/protocol/proto"
)

type listener struct {
	net.Listener

	opts    Options
	msgPool *messagePool
}

// NewListener creates a consumer listener.
func NewListener(addr string, opts Options) (Listener, error) {
	if opts == nil {
		opts = NewOptions()
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mPool := newMessagePool(opts.MessagePoolOptions())
	mPool.Init()
	return &listener{
		Listener: lis,
		opts:     opts,
		msgPool:  mPool,
	}, nil
}

func (l *listener) Accept() (Consumer, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return newConsumer(conn, l.msgPool, l.opts), nil
}

type consumer struct {
	opts   Options
	mPool  *messagePool
	encdec proto.EncodeDecoder
	rw     *bufio.ReadWriter
	conn   net.Conn

	ackLock sync.Mutex
	ackPb   msgpb.Ack
	canAck  bool
}

func newConsumer(
	conn net.Conn,
	mPool *messagePool,
	opts Options,
) *consumer {
	r := bufio.NewReaderSize(conn, opts.ConnectionReadBufferSize())
	w := bufio.NewWriterSize(conn, opts.ConnectionWriteBufferSize())
	rw := bufio.NewReadWriter(r, w)
	return &consumer{
		opts:   opts,
		mPool:  mPool,
		encdec: proto.NewEncodeDecoder(rw, opts.EncodeDecoderOptions()),
		rw:     rw,
		conn:   conn,
		canAck: true,
	}
}

func (c *consumer) Message() (Message, error) {
	m := c.mPool.Get()
	m.reset(c)
	if err := c.encdec.Decode(m); err != nil {
		c.mPool.Put(m)
		return nil, err
	}
	return m, nil
}

// This function could be called concurrently if messages are being
// processed concurrently.
func (c *consumer) tryAck(m msgpb.Metadata) {
	c.ackLock.Lock()
	if !c.canAck {
		c.ackLock.Unlock()
		return
	}
	c.ackPb.Metadata = append(c.ackPb.Metadata, m)
	if len(c.ackPb.Metadata) < c.opts.AckBufferSize() {
		c.ackLock.Unlock()
		return
	}
	if err := c.encodeAckWithLock(); err != nil {
		c.conn.Close()
	}
	c.ackLock.Unlock()
}

func (c *consumer) encodeAckWithLock() error {
	err := c.encdec.Encode(&c.ackPb)
	c.ackPb.Metadata = c.ackPb.Metadata[:0]
	return err
}

func (c *consumer) Close() {
	c.ackLock.Lock()
	c.canAck = false
	if len(c.ackPb.Metadata) > 0 {
		c.encodeAckWithLock()
	}
	c.ackLock.Unlock()

	c.rw.Flush()
	c.conn.Close()
}

type message struct {
	msgpb.Message

	p *messagePool
	c *consumer
}

func newMessage(p *messagePool) *message {
	return &message{p: p}
}

func (m *message) Bytes() []byte {
	return m.Value
}

func (m *message) Ack() {
	m.c.tryAck(m.Metadata)
	if m.p != nil {
		m.p.Put(m)
	}
}

func (m *message) reset(c *consumer) {
	m.c = c
	resetProto(&m.Message)
}

func resetProto(m *msgpb.Message) {
	m.Metadata.Id = 0
	m.Metadata.Shard = 0
	m.Value = m.Value[:0]
}
