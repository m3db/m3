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

	errInvalidConnection = errors.New("connection is invalid")
)

type consumerWriter interface {
	// Address returns the consumer address.
	Address() string

	// Write writes the marshaler, it should be thread safe.
	Write(m proto.Marshaler) error

	// Init initializes the consumer writer.
	Init()

	// Close closes the consumer writer.
	Close()
}

type consumerWriterMetrics struct {
	writeInvalidConn        tally.Counter
	readInvalidConn         tally.Counter
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
		writeInvalidConn:        scope.Counter("write-invalid-conn"),
		readInvalidConn:         scope.Counter("read-invalid-conn"),
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

	validConn      *atomic.Bool
	conn           io.ReadWriteCloser
	rw             *bufio.ReadWriter
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
	m consumerWriterMetrics,
) consumerWriter {
	if opts == nil {
		opts = NewOptions()
	}

	var (
		connOpts = opts.ConnectionOptions()
		rw       = bufio.NewReadWriter(
			bufio.NewReaderSize(u, connOpts.ReadBufferSize()),
			bufio.NewWriterSize(u, connOpts.WriteBufferSize()),
		)
	)
	w := &consumerWriterImpl{
		encdec:         proto.NewEncodeDecoder(rw, opts.EncodeDecoderOptions()),
		addr:           addr,
		router:         router,
		opts:           opts,
		connOpts:       connOpts,
		ackRetrier:     retry.NewRetrier(opts.AckErrorRetryOptions()),
		connRetrier:    retry.NewRetrier(connOpts.RetryOptions().SetForever(defaultRetryForever)),
		logger:         opts.InstrumentOptions().Logger(),
		validConn:      atomic.NewBool(false),
		conn:           u,
		rw:             rw,
		lastResetNanos: 0,
		resetCh:        make(chan struct{}, 1),
		closed:         atomic.NewBool(false),
		doneCh:         make(chan struct{}),
		m:              m,
		nowFn:          time.Now,
	}

	w.connectFn = w.connectOnce
	if err := w.resetWithConnectFn(w.connectFn); err != nil {
		w.notifyReset()
	}
	return w
}

func (w *consumerWriterImpl) Address() string {
	return w.addr
}

// Write should fail fast so that the write could be tried on other
// consumer writers that are sharing the message queue.
func (w *consumerWriterImpl) Write(m proto.Marshaler) error {
	if !w.validConn.Load() {
		w.m.writeInvalidConn.Inc(1)
		return errInvalidConnection
	}
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

	w.wg.Add(1)
	go func() {
		w.flushUntilClose()
		w.wg.Done()
	}()
}

func (w *consumerWriterImpl) flushUntilClose() {
	flushTicker := time.NewTicker(w.connOpts.FlushInterval())
	defer flushTicker.Stop()

	for {
		select {
		case <-flushTicker.C:
			w.encodeLock.Lock()
			w.rw.Flush()
			w.encodeLock.Unlock()
		case <-w.doneCh:
			return
		}
	}
}

func (w *consumerWriterImpl) resetConnectionUntilClose() {
	for {
		select {
		case <-w.resetCh:
			// Avoid resetting too frequent.
			if w.resetTooSoon() {
				w.m.resetTooSoon.Inc(1)
				continue
			}
			if err := w.resetWithConnectFn(w.connectWithRetry); err != nil {
				w.m.resetError.Inc(1)
				w.logger.Errorf("could not reconnect to %s, %v", w.addr, err)
				continue
			}
			w.m.resetSuccess.Inc(1)
			w.logger.Infof("reconnected to %s", w.addr)
		case <-w.doneCh:
			w.conn.Close()
			return
		}
	}
}

func (w *consumerWriterImpl) resetTooSoon() bool {
	return w.nowFn().UnixNano() < w.lastResetNanos+int64(w.connOpts.ResetDelay())
}

func (w *consumerWriterImpl) resetWithConnectFn(fn connectFn) error {
	w.validConn.Store(false)
	conn, err := fn(w.addr)
	if err != nil {
		return err
	}
	w.reset(conn)
	w.validConn.Store(true)
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
	if !w.validConn.Load() {
		w.m.readInvalidConn.Inc(1)
		return errInvalidConnection
	}
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
	w.rw.Reader.Reset(conn)
	w.rw.Writer.Reset(conn)
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
	return newReadWriterWithTimeout(conn, w.connOpts.WriteTimeout(), w.nowFn), nil
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
		return nil, attemptErr
	}
	return conn, nil
}

type readWriterWithTimeout struct {
	net.Conn

	timeout time.Duration
	nowFn   clock.NowFn
}

func newReadWriterWithTimeout(conn net.Conn, timeout time.Duration, nowFn clock.NowFn) readWriterWithTimeout {
	return readWriterWithTimeout{
		Conn:    conn,
		timeout: timeout,
		nowFn:   nowFn,
	}
}

func (conn readWriterWithTimeout) Write(p []byte) (int, error) {
	conn.SetWriteDeadline(conn.nowFn().Add(conn.timeout))
	return conn.Conn.Write(p)
}

type uninitializedReadWriter struct{}

func (u uninitializedReadWriter) Read(p []byte) (int, error)  { return 0, errInvalidConnection }
func (u uninitializedReadWriter) Write(p []byte) (int, error) { return 0, errInvalidConnection }
func (u uninitializedReadWriter) Close() error                { return nil }
