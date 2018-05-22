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
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/protocol/proto"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"
)

const (
	defaultRetryForever = true
)

var (
	u uninitializedReadWriter

	errNotInitialized = errors.New("connection not initialized")
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
	ackError                tally.Counter
	decodeError             tally.Counter
	encodeError             tally.Counter
	resetTooSoon            tally.Counter
	resetSuccess            tally.Counter
	resetError              tally.Counter
	connectError            tally.Counter
	setKeepAliveError       tally.Counter
	setKeepAlivePeriodError tally.Counter
}

func newConsumerWriterMetrics(scope tally.Scope) consumerWriterMetrics {
	return consumerWriterMetrics{
		ackError:                scope.Counter("ack-error"),
		decodeError:             scope.Counter("decode-error"),
		encodeError:             scope.Counter("encode-error"),
		resetTooSoon:            scope.Counter("reset-too-soon"),
		resetSuccess:            scope.Counter("reset-success"),
		resetError:              scope.Counter("reset-error"),
		connectError:            scope.Counter("connect-error"),
		setKeepAliveError:       scope.Counter("set-keep-alive-error"),
		setKeepAlivePeriodError: scope.Counter("set-keep-alive-period-error"),
	}
}

type connectFn func(addr string) (io.ReadWriteCloser, error)

type consumerWriterImpl struct {
	encodeLock  sync.Mutex
	decodeLock  sync.Mutex
	encdec      proto.EncodeDecoder
	addr        string
	router      ackRouter
	opts        Options
	connOpts    ConnectionOptions
	ackRetrier  retry.Retrier
	connRetrier retry.Retrier
	logger      log.Logger

	conn           io.ReadWriteCloser
	bw             *bufio.Writer
	br             *bufio.Reader
	lastResetNanos int64
	resetCh        chan struct{}
	ack            msgpb.Ack
	closed         *atomic.Bool
	doneCh         chan struct{}
	wg             sync.WaitGroup
	m              consumerWriterMetrics

	nowFn     clock.NowFn
	connectFn connectFn
}

func newConsumerWriter(
	addr string,
	router ackRouter,
	opts Options,
) consumerWriter {
	if opts == nil {
		opts = NewOptions()
	}

	var (
		connOpts = opts.ConnectionOptions()
		bw       = bufio.NewWriterSize(u, connOpts.WriteBufferSize())
		br       = bufio.NewReaderSize(u, connOpts.ReadBufferSize())
	)
	w := &consumerWriterImpl{
		encdec:         proto.NewEncodeDecoder(bufio.NewReadWriter(br, bw), opts.EncodeDecoderOptions()),
		addr:           addr,
		router:         router,
		opts:           opts,
		connOpts:       connOpts,
		ackRetrier:     retry.NewRetrier(opts.AckErrorRetryOptions()),
		connRetrier:    retry.NewRetrier(connOpts.RetryOptions().SetForever(defaultRetryForever)),
		logger:         opts.InstrumentOptions().Logger(),
		conn:           u,
		bw:             bw,
		br:             br,
		lastResetNanos: 0,
		resetCh:        make(chan struct{}, 1),
		closed:         atomic.NewBool(false),
		doneCh:         make(chan struct{}),
		m:              newConsumerWriterMetrics(opts.InstrumentOptions().MetricsScope()),
		nowFn:          time.Now,
	}

	w.connectFn = w.connectOnce
	if err := w.resetWithConnectFn(w.connectFn); err != nil {
		w.notifyReset()
	}
	return w
}

// Write should fail fast so that the write could be tried on other
// consumer writers that are sharing the data.
func (w *consumerWriterImpl) Write(m proto.Marshaler) error {
	w.encodeLock.Lock()
	err := w.encdec.Encode(m)
	w.encodeLock.Unlock()
	if err != nil {
		w.notifyReset()
		w.m.encodeError.Inc(1)
	}
	return err
}

func (w *consumerWriterImpl) Init() {
	w.wg.Add(1)
	go func() {
		w.resetConnectionUntilClose()
		w.wg.Done()
	}()

	w.wg.Add(1)
	go func() {
		w.readAcksUntilClose()
		w.wg.Done()
	}()
}

func (w *consumerWriterImpl) resetConnectionUntilClose() {
	for {
		select {
		case <-w.resetCh:
			w.resetWithConnectFn(w.connectWithRetry)
		case <-w.doneCh:
			w.conn.Close()
			return
		}
	}
}

func (w *consumerWriterImpl) resetWithConnectFn(fn connectFn) error {
	// Avoid resetting too frequent.
	if w.nowFn().UnixNano() < w.lastResetNanos+int64(w.connOpts.ResetDelay()) {
		w.m.resetTooSoon.Inc(1)
		return nil
	}
	conn, err := fn(w.addr)
	if err != nil {
		w.m.resetError.Inc(1)
		w.logger.Errorf("could not connect to %s, %v", w.addr, err)
		return err
	}
	w.reset(conn)
	w.m.resetSuccess.Inc(1)
	w.logger.Infof("reconnected to %s", w.addr)
	return nil
}

func (w *consumerWriterImpl) readAcksUntilClose() {
	for {
		select {
		case <-w.doneCh:
			return
		default:
			w.ackRetrier.AttemptWhile(
				w.continueFn,
				w.readAcks,
			)
		}
	}
}

func (w *consumerWriterImpl) continueFn(int) bool {
	return !w.isClosed()
}

func (w *consumerWriterImpl) readAcks() error {
	// TODO: make util function for proto reset.
	// NB(cw) The proto needs to be cleaned up because the gogo protobuf
	// unmarshalling will append to the underlying slice.
	w.ack.Metadata = w.ack.Metadata[:0]
	w.decodeLock.Lock()
	err := w.encdec.Decode(&w.ack)
	w.decodeLock.Unlock()
	if err != nil {
		w.notifyReset()
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
	w.wg.Wait()
	w.encdec.Close()
}

func (w *consumerWriterImpl) notifyReset() {
	select {
	case w.resetCh <- struct{}{}:
	default:
	}
}

func (w *consumerWriterImpl) isClosed() bool {
	return w.closed.Load()
}

func (w *consumerWriterImpl) reset(conn io.ReadWriteCloser) {
	// Close the connection to wake up potential blocking encode/decode calls.
	w.conn.Close()

	// NB: Connection can only be reset between encode/decode calls.
	w.decodeLock.Lock()
	defer w.decodeLock.Unlock()

	w.encodeLock.Lock()
	defer w.encodeLock.Unlock()

	w.conn = conn
	w.bw.Reset(conn)
	w.br.Reset(conn)
	w.lastResetNanos = w.nowFn().UnixNano()
}

func (w *consumerWriterImpl) connectOnce(addr string) (io.ReadWriteCloser, error) {
	conn, err := net.DialTimeout("tcp", addr, w.connOpts.DialTimeout())
	if err != nil {
		w.m.connectError.Inc(1)
		return nil, err
	}
	tcpConn := conn.(*net.TCPConn)
	if err = tcpConn.SetKeepAlive(true); err != nil {
		w.m.setKeepAliveError.Inc(1)
	}
	keepAlivePeriod := w.connOpts.KeepAlivePeriod()
	if keepAlivePeriod <= 0 {
		return conn, nil
	}
	if err = tcpConn.SetKeepAlivePeriod(keepAlivePeriod); err != nil {
		w.m.setKeepAlivePeriodError.Inc(1)
	}
	return conn, nil
}

func (w *consumerWriterImpl) connectWithRetry(addr string) (io.ReadWriteCloser, error) {
	var (
		conn io.ReadWriteCloser
		err  error
	)
	fn := func() error {
		conn, err = w.connectFn(addr)
		return err
	}
	if attemptErr := w.connRetrier.AttemptWhile(
		w.continueFn,
		fn,
	); attemptErr != nil {
		return nil, fmt.Errorf("failed to connect to address %s, %v", addr, attemptErr)
	}
	return conn, nil
}

type uninitializedReadWriter struct{}

func (u uninitializedReadWriter) Read(p []byte) (int, error)  { return 0, errNotInitialized }
func (u uninitializedReadWriter) Write(p []byte) (int, error) { return 0, errNotInitialized }
func (u uninitializedReadWriter) Close() error                { return nil }
