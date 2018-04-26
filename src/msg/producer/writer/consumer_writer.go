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
	"sync"

	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/protocol/proto"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"
)

type consumerWriter interface {
	// Write writes the marshaler, it should be thread safe.
	Write(m proto.Marshaler) error

	// Init initializes the consumer writer.
	Init()

	// Close closes the consumer writer.
	Close()
}

type consumerWriterMetrics struct {
	ackError    tally.Counter
	decodeError tally.Counter
	encodeError tally.Counter
}

func newConsumerWriterMetrics(scope tally.Scope) consumerWriterMetrics {
	return consumerWriterMetrics{
		ackError:    scope.Counter("ack-error"),
		decodeError: scope.Counter("decode-error"),
		encodeError: scope.Counter("encode-error"),
	}
}

type consumerWriterImpl struct {
	// encodeLock controls the access to the encode function.
	encodeLock sync.Mutex
	encdec     proto.EncodeDecoder
	addr       string
	router     ackRouter
	opts       Options
	retrier    retry.Retrier
	logger     log.Logger

	ack    msgpb.Ack
	c      *retryableConnection
	closed *atomic.Bool
	doneCh chan struct{}
	wg     sync.WaitGroup
	m      consumerWriterMetrics
}

func newConsumerWriter(
	addr string,
	router ackRouter,
	opts Options,
) consumerWriter {
	if opts == nil {
		opts = NewOptions()
	}
	conn := newRetryableConnection(addr, opts.ConnectionOptions())
	return &consumerWriterImpl{
		encdec:  proto.NewEncodeDecoder(conn, opts.EncodeDecoderOptions()),
		addr:    addr,
		router:  router,
		opts:    opts,
		retrier: retry.NewRetrier(opts.AckErrorRetryOptions()),
		logger:  opts.InstrumentOptions().Logger(),
		c:       conn,
		closed:  atomic.NewBool(false),
		doneCh:  make(chan struct{}),
		m:       newConsumerWriterMetrics(opts.InstrumentOptions().MetricsScope()),
	}
}

// Write should fail fast so that the write could be tried on other
// consumer writers that are sharing the data.
func (w *consumerWriterImpl) Write(m proto.Marshaler) error {
	w.encodeLock.Lock()
	err := w.encdec.Encode(m)
	w.encodeLock.Unlock()
	if err != nil {
		w.c.NotifyReset()
		w.m.encodeError.Inc(1)
	}
	return err
}

func (w *consumerWriterImpl) Init() {
	w.c.Init()

	w.wg.Add(1)
	go func() {
		w.readAcksUntilClose()
		w.wg.Done()
	}()
}

func (w *consumerWriterImpl) readAcksUntilClose() {
	for {
		select {
		case <-w.doneCh:
			return
		default:
			w.readAcksWithRetry()
		}
	}
}

func (w *consumerWriterImpl) readAcksWithRetry() {
	if err := w.retrier.AttemptWhile(
		w.readAckContinueFn,
		w.readAcks,
	); err != nil {
		w.logger.Errorf("could not read acks from %s, will retry later: %v", w.addr, err)
	}
}

func (w *consumerWriterImpl) readAckContinueFn(int) bool {
	return !w.closed.Load()
}

func (w *consumerWriterImpl) readAcks() error {
	// TODO: make util function for proto reset.
	// NB(cw) The proto needs to be cleaned up because the gogo protobuf
	// unmarshalling will append to the underlying slice.
	w.ack.Metadata = w.ack.Metadata[:0]
	err := w.encdec.Decode(&w.ack)
	if err != nil {
		// On io.EOF, don't reset the connection but return an error
		// so the retrier will back off and retry later.
		if err == io.EOF {
			return err
		}
		w.c.NotifyReset()
		w.m.decodeError.Inc(1)
		return err
	}
	for _, m := range w.ack.Metadata {
		if err := w.router.Ack(newMetadataFromProto(m)); err != nil {
			w.m.ackError.Inc(1)
			// This is fine, usually this means the ack has been acked.
			w.logger.Errorf("could not ack metadata, %v", err)
		}
	}
	return nil
}

func (w *consumerWriterImpl) Close() {
	if !w.closed.CAS(false, true) {
		return
	}
	close(w.doneCh)
	w.c.Close()
	w.encdec.Close()
	w.wg.Wait()
}
