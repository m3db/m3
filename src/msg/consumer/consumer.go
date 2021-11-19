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
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/x/clock"
	xio "github.com/m3db/m3/src/x/io"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
)

type listener struct {
	net.Listener

	opts    Options
	msgPool *messagePool
	m       metrics
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
		m:        newConsumerMetrics(opts.InstrumentOptions().MetricsScope()),
	}, nil
}

func (l *listener) Accept() (Consumer, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	return newConsumer(conn, l.msgPool, l.opts, l.m, NewNoOpMessageProcessor()), nil
}

type metrics struct {
	messageReceived    tally.Counter
	messageDecodeError tally.Counter
	ackSent            tally.Counter
	ackEncodeError     tally.Counter
	ackWriteError      tally.Counter
	// the duration between the producer sending the message and the consumer reading the message.
	receiveLatency tally.Histogram
	// the duration between the consumer reading the message and sending an ack to the producer.
	handleLatency tally.Histogram
}

func newConsumerMetrics(scope tally.Scope) metrics {
	return metrics{
		messageReceived:    scope.Counter("message-received"),
		messageDecodeError: scope.Counter("message-decode-error"),
		ackSent:            scope.Counter("ack-sent"),
		ackEncodeError:     scope.Counter("ack-encode-error"),
		ackWriteError:      scope.Counter("ack-write-error"),
		receiveLatency: scope.Histogram("receive-latency",
			// 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms, 1.2s, 2.4s, 4.8s, 9.6s
			tally.MustMakeExponentialDurationBuckets(time.Millisecond*10, 2, 11)),
		handleLatency: scope.Histogram("handle-latency",
			// 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms, 1.2s, 2.4s, 4.8s, 9.6s
			tally.MustMakeExponentialDurationBuckets(time.Millisecond*10, 2, 11)),
	}
}

type consumer struct {
	sync.Mutex

	opts    Options
	mPool   *messagePool
	encoder proto.Encoder
	decoder proto.Decoder
	w       xio.ResettableWriter
	conn    net.Conn

	ackPb            msgpb.Ack
	closed           bool
	doneCh           chan struct{}
	wg               sync.WaitGroup
	m                metrics
	messageProcessor MessageProcessor
}

func newConsumer(
	conn net.Conn,
	mPool *messagePool,
	opts Options,
	m metrics,
	mp MessageProcessor,
) *consumer {
	var (
		wOpts = xio.ResettableWriterOptions{
			WriteBufferSize: opts.ConnectionWriteBufferSize(),
		}

		rwOpts   = opts.DecoderOptions().RWOptions()
		writerFn = rwOpts.ResettableWriterFn()
	)

	return &consumer{
		opts:    opts,
		mPool:   mPool,
		encoder: proto.NewEncoder(opts.EncoderOptions()),
		decoder: proto.NewDecoder(
			conn, opts.DecoderOptions(), opts.ConnectionReadBufferSize(),
		),
		w:                writerFn(newConnWithTimeout(conn, opts.ConnectionWriteTimeout(), time.Now), wOpts),
		conn:             conn,
		closed:           false,
		doneCh:           make(chan struct{}),
		m:                m,
		messageProcessor: mp,
	}
}

func (c *consumer) Init() {
	c.wg.Add(1)
	go func() {
		c.ackUntilClose()
		c.wg.Done()
	}()
}
func (c *consumer) process(m Message) {
	c.messageProcessor.Process(m)
}

func (c *consumer) Message() (Message, error) {
	m := c.mPool.Get()
	m.reset(c)
	if err := c.decoder.Decode(m); err != nil {
		c.mPool.Put(m)
		c.m.messageDecodeError.Inc(1)
		return nil, err
	}
	if m.Metadata.SentAtNanos > 0 {
		c.m.receiveLatency.RecordDuration(xtime.Since(xtime.UnixNano(m.Metadata.SentAtNanos)))
	}
	c.m.messageReceived.Inc(1)
	return m, nil
}

// This function could be called concurrently if messages are being
// processed concurrently.
func (c *consumer) tryAck(m msgpb.Metadata) {
	c.Lock()
	if c.closed {
		c.Unlock()
		return
	}
	c.ackPb.Metadata = append(c.ackPb.Metadata, m)
	ackLen := len(c.ackPb.Metadata)
	if ackLen < c.opts.AckBufferSize() {
		c.Unlock()
		return
	}
	c.trySendAcksWithLock(ackLen)
	c.Unlock()
}

func (c *consumer) ackUntilClose() {
	flushTicker := time.NewTicker(c.opts.AckFlushInterval())
	defer flushTicker.Stop()

	for {
		select {
		case <-flushTicker.C:
			c.tryAckAndFlush()
		case <-c.doneCh:
			c.tryAckAndFlush()
			return
		}
	}
}

func (c *consumer) tryAckAndFlush() {
	c.Lock()
	if ackLen := len(c.ackPb.Metadata); ackLen > 0 {
		c.trySendAcksWithLock(ackLen)
	}
	c.w.Flush()
	c.Unlock()
}

// if acks fail to send the client will retry sending the messages.
func (c *consumer) trySendAcksWithLock(ackLen int) {
	err := c.encoder.Encode(&c.ackPb)
	log := c.opts.InstrumentOptions().Logger()
	c.ackPb.Metadata = c.ackPb.Metadata[:0]
	if err != nil {
		c.m.ackEncodeError.Inc(1)
		log.Error("failed to encode ack. client will retry sending message.", zap.Error(err))
		return
	}
	_, err = c.w.Write(c.encoder.Bytes())
	if err != nil {
		c.m.ackWriteError.Inc(1)
		log.Error("failed to write ack. client will retry sending message.", zap.Error(err))
		c.tryCloseConn()
		return
	}
	if err := c.w.Flush(); err != nil {
		c.m.ackWriteError.Inc(1)
		log.Error("failed to flush ack. client will retry sending message.", zap.Error(err))
		c.tryCloseConn()
		return
	}
	c.m.ackSent.Inc(int64(ackLen))
}

func (c *consumer) tryCloseConn() {
	if err := c.conn.Close(); err != nil {
		c.opts.InstrumentOptions().Logger().Error("failed to close connection.", zap.Error(err))
	}
}

func (c *consumer) Close() {
	c.Lock()
	if c.closed {
		c.Unlock()
		return
	}
	c.closed = true
	c.Unlock()

	close(c.doneCh)
	c.wg.Wait()
	c.conn.Close()
}

type message struct {
	msgpb.Message

	mPool *messagePool
	c     *consumer
}

func newMessage(p *messagePool) *message {
	return &message{mPool: p}
}

func (m *message) Bytes() []byte {
	return m.Value
}

func (m *message) Ack() {
	m.c.tryAck(m.Metadata)
	if m.mPool != nil {
		m.mPool.Put(m)
	}
}

func (m *message) reset(c *consumer) {
	m.c = c
	resetProto(&m.Message)
}

func (m *message) ShardID() uint64 {
	return m.Metadata.Shard
}

func (m *message) SentAtNanos() uint64 {
	return m.Metadata.SentAtNanos
}

func resetProto(m *msgpb.Message) {
	m.Metadata.Id = 0
	m.Metadata.Shard = 0
	m.Value = m.Value[:0]
}

type connWithTimeout struct {
	net.Conn

	timeout time.Duration
	nowFn   clock.NowFn
}

func newConnWithTimeout(conn net.Conn, timeout time.Duration, nowFn clock.NowFn) connWithTimeout {
	return connWithTimeout{
		Conn:    conn,
		timeout: timeout,
		nowFn:   nowFn,
	}
}

func (conn connWithTimeout) Write(p []byte) (int, error) {
	if conn.timeout > 0 {
		conn.SetWriteDeadline(conn.nowFn().Add(conn.timeout))
	}
	return conn.Conn.Write(p)
}
