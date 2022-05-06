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
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/x/clock"
	xio "github.com/m3db/m3/src/x/io"
	"github.com/m3db/m3/src/x/retry"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	defaultRetryForever = true
)

var (
	errInvalidConnection = errors.New("connection is invalid")
	u                    uninitializedReadWriter
)

type consumerWriter interface {
	// Address returns the consumer address.
	Address() string

	// Write writes the bytes, it is thread safe per connection index.
	Write(connIndex int, b []byte) error

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

type connectAllFn func(addr string) ([]io.ReadWriteCloser, error)

type consumerWriterImpl struct {
	writeState consumerWriterImplWriteState

	addr        string
	router      ackRouter
	opts        Options
	connOpts    ConnectionOptions
	ackRetrier  retry.Retrier
	connRetrier retry.Retrier
	logger      *zap.Logger

	resetCh chan struct{}
	doneCh  chan struct{}
	wg      sync.WaitGroup
	m       consumerWriterMetrics

	nowFn     clock.NowFn
	connectFn connectFn
}

type consumerWriterImplWriteState struct {
	sync.RWMutex

	// conns keeps active connections.
	// Note: readers will take a reference to this slice with a lock
	// then loop through it and call decode on decoders, so not safe
	// to reuse.
	conns          []*connection
	lastResetNanos int64

	closed     bool
	validConns bool
}

type connection struct {
	writeLock sync.Mutex
	conn      io.ReadWriteCloser
	w         xio.ResettableWriter
	decoder   proto.Decoder
	ack       msgpb.Ack
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

	connOpts := opts.ConnectionOptions()
	w := &consumerWriterImpl{
		addr:        addr,
		router:      router,
		opts:        opts,
		connOpts:    connOpts,
		ackRetrier:  retry.NewRetrier(opts.AckErrorRetryOptions()),
		connRetrier: retry.NewRetrier(connOpts.RetryOptions().SetForever(defaultRetryForever)),
		logger:      opts.InstrumentOptions().Logger(),
		resetCh:     make(chan struct{}, 1),
		doneCh:      make(chan struct{}),
		m:           m,
		nowFn:       time.Now,
	}
	w.connectFn = w.connectNoRetry

	// Initialize no-op connections since it's valid even if connecting the
	// first time fails to continue to try to write to the writer.
	// Note: Also tests try to break a non-connected writer.
	conns := make([]io.ReadWriteCloser, 0, connOpts.NumConnections())
	for i := 0; i < connOpts.NumConnections(); i++ {
		conns = append(conns, u)
	}
	// NB(r): Reset at epoch since a connection failure should trigger
	// an immediate reset after first connection attempt (if write fails
	// since first connection is with no retry).
	w.reset(resetOptions{
		connections: conns,
		at:          time.Time{},
		validConns:  false,
	})

	// Try connecting without retry first attempt.
	connectAllNoRetry := w.newConnectFn(connectOptions{retry: false})
	if err := w.resetWithConnectFn(connectAllNoRetry); err != nil {
		w.notifyReset(err)
	}
	return w
}

func (w *consumerWriterImpl) Address() string {
	return w.addr
}

// Write should fail fast so that the write could be tried on other
// consumer writers that are sharing the message queue.
func (w *consumerWriterImpl) Write(connIndex int, b []byte) error {
	w.writeState.RLock()
	if !w.writeState.validConns || len(w.writeState.conns) == 0 {
		w.writeState.RUnlock()
		w.m.writeInvalidConn.Inc(1)
		return errInvalidConnection
	}
	if connIndex < 0 || connIndex >= len(w.writeState.conns) {
		w.writeState.RUnlock()
		return fmt.Errorf("connection index out of range: %d", connIndex)
	}

	writeConn := w.writeState.conns[connIndex]

	// Make sure only writer to this connection.
	writeConn.writeLock.Lock()
	_, err := writeConn.w.Write(b)
	writeConn.writeLock.Unlock()

	// Hold onto the write state lock until done, since
	// closing connections are done by acquiring the write state lock.
	w.writeState.RUnlock()

	if err != nil {
		w.notifyReset(err)
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

	for i := 0; i < w.connOpts.NumConnections(); i++ {
		idx := i
		w.wg.Add(1)
		go func() {
			w.readAcksUntilClose(idx)
			w.wg.Done()
		}()
	}

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
			w.writeState.RLock()
			for _, conn := range w.writeState.conns {
				conn.writeLock.Lock()
				if err := conn.w.Flush(); err != nil {
					w.notifyReset(err)
				}
				conn.writeLock.Unlock()
			}
			// Hold onto the write state lock until done, since
			// closing connections are done by acquiring the write state lock.
			w.writeState.RUnlock()
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
			// Connect with retry.
			connectAllWithRetry := w.newConnectFn(connectOptions{retry: true})
			if err := w.resetWithConnectFn(connectAllWithRetry); err != nil {
				w.m.resetError.Inc(1)
				w.logger.Error("could not reconnect", zap.String("address", w.addr), zap.Error(err))
				continue
			}
			w.m.resetSuccess.Inc(1)
			w.logger.Info("reconnected", zap.String("address", w.addr))
		case <-w.doneCh:
			w.writeState.Lock()
			for _, c := range w.writeState.conns {
				c.conn.Close()
			}
			w.writeState.Unlock()
			return
		}
	}
}

func (w *consumerWriterImpl) resetTooSoon() bool {
	w.writeState.RLock()
	defer w.writeState.RUnlock()
	return w.nowFn().UnixNano() < w.writeState.lastResetNanos+int64(w.connOpts.ResetDelay())
}

func (w *consumerWriterImpl) resetWithConnectFn(fn connectAllFn) error {
	w.writeState.Lock()
	w.writeState.validConns = false
	w.writeState.Unlock()
	conns, err := fn(w.addr)
	if err != nil {
		return err
	}
	w.reset(resetOptions{
		connections: conns,
		at:          w.nowFn(),
		validConns:  true,
	})
	return nil
}

func (w *consumerWriterImpl) readAcksUntilClose(idx int) {
	for {
		select {
		case <-w.doneCh:
			return
		default:
			w.ackRetrier.AttemptWhile(w.continueFn,
				func() error {
					return w.readAcks(idx)
				})
		}
	}
}

func (w *consumerWriterImpl) continueFn(int) bool {
	return !w.isClosed()
}

func (w *consumerWriterImpl) readAcks(idx int) error {
	w.writeState.RLock()
	validConns := w.writeState.validConns
	conn := w.writeState.conns[idx]
	w.writeState.RUnlock()
	if !validConns {
		w.m.readInvalidConn.Inc(1)
		return errInvalidConnection
	}

	// Read from decoder, safe to read from acquired decoder as not re-used.
	// NB(cw) The proto needs to be cleaned up because the gogo protobuf
	// unmarshalling will append to the underlying slice.
	conn.ack.Metadata = conn.ack.Metadata[:0]
	err := conn.decoder.Decode(&conn.ack)
	if err != nil {
		w.notifyReset(err)
		w.m.decodeError.Inc(1)
		return err
	}
	for _, m := range conn.ack.Metadata {
		if err := w.router.Ack(newMetadataFromProto(m)); err != nil {
			w.m.ackError.Inc(1)
			// This is fine, usually this means the ack has been acked.
			w.logger.Error("could not ack metadata", zap.Error(err))
		}
	}

	return nil
}

func (w *consumerWriterImpl) Close() {
	w.writeState.Lock()
	wasClosed := w.writeState.closed
	w.writeState.closed = true
	w.writeState.Unlock()

	if wasClosed {
		return
	}

	close(w.doneCh)

	w.wg.Wait()
}

func (w *consumerWriterImpl) notifyReset(err error) {
	select {
	case w.resetCh <- struct{}{}:
		if err != nil {
			w.logger.Error("connection error", zap.Error(err))
		}
	default:
	}
}

func (w *consumerWriterImpl) isClosed() bool {
	w.writeState.RLock()
	defer w.writeState.RUnlock()
	return w.writeState.closed
}

type resetOptions struct {
	connections []io.ReadWriteCloser
	at          time.Time
	validConns  bool
}

func (w *consumerWriterImpl) reset(opts resetOptions) {
	w.writeState.Lock()
	prevConns := w.writeState.conns
	defer func() {
		w.writeState.Unlock()
		// Close existing connections outside of locks.
		for _, c := range prevConns {
			c.conn.Close()
		}
	}()

	var (
		wOpts = xio.ResettableWriterOptions{
			WriteBufferSize: w.connOpts.WriteBufferSize(),
		}

		rwOpts   = w.opts.DecoderOptions().RWOptions()
		writerFn = rwOpts.ResettableWriterFn()
	)

	w.writeState.conns = make([]*connection, 0, len(opts.connections))
	for _, conn := range opts.connections {
		wr := writerFn(u, wOpts)
		wr.Reset(conn)

		decoder := proto.NewDecoder(conn, w.opts.DecoderOptions(), w.connOpts.ReadBufferSize())
		newConn := &connection{
			conn:    conn,
			w:       wr,
			decoder: decoder,
		}

		w.writeState.conns = append(w.writeState.conns, newConn)
	}

	w.writeState.lastResetNanos = opts.at.UnixNano()
	w.writeState.validConns = opts.validConns
}

func (w *consumerWriterImpl) connectNoRetry(addr string) (io.ReadWriteCloser, error) {
	// Upcast readWriterWithTimeout to the interface; this allows us to mock out the connectNoRetry function in tests.
	return w.connectNoRetryWithTimeout(addr)
}

func (w *consumerWriterImpl) connectNoRetryWithTimeout(addr string) (readWriterWithTimeout, error) {
	// N.B.: this is roughly equivalent to what net.DialTimeout does; shouldn't introduce performance regressions.
	ctx, cancel := context.WithTimeout(context.Background(), w.connOpts.DialTimeout())
	defer cancel()

	conn, err := w.dialContext(ctx, addr)
	if err != nil {
		w.m.connectError.Inc(1)
		return readWriterWithTimeout{}, err
	}
	tcpConn, ok := conn.(keepAlivable)
	if !ok {
		// If using a custom dialer which doesn't return *net.TCPConn, users are responsible for TCP keep alive options
		// themselves.
		return newReadWriterWithTimeout(conn, w.connOpts.WriteTimeout(), w.nowFn), nil
	}
	if err = tcpConn.SetKeepAlive(true); err != nil {
		w.m.setKeepAliveError.Inc(1)
	}
	keepAlivePeriod := w.connOpts.KeepAlivePeriod()
	if keepAlivePeriod <= 0 {
		return newReadWriterWithTimeout(conn, w.connOpts.WriteTimeout(), w.nowFn), nil
	}
	if err = tcpConn.SetKeepAlivePeriod(keepAlivePeriod); err != nil {
		w.m.setKeepAlivePeriodError.Inc(1)
	}
	return newReadWriterWithTimeout(conn, w.connOpts.WriteTimeout(), w.nowFn), nil
}

// Make sure net.TCPConn implements this; otherwise bad things will happen.
var _ keepAlivable = (*net.TCPConn)(nil)

type keepAlivable interface {
	SetKeepAlive(shouldKeepAlive bool) error
	SetKeepAlivePeriod(d time.Duration) error
}

func (w *consumerWriterImpl) dialContext(ctx context.Context, addr string) (net.Conn, error) {
	if dialer := w.connOpts.ContextDialer(); dialer != nil {
		return dialer(ctx, "tcp", addr)
	}
	var dialer net.Dialer
	return dialer.DialContext(ctx, "tcp", addr)
}

type connectOptions struct {
	retry bool
}

func (w *consumerWriterImpl) newConnectFn(opts connectOptions) connectAllFn {
	return func(addr string) ([]io.ReadWriteCloser, error) {
		var (
			numConns = w.connOpts.NumConnections()
			conns    = make([]io.ReadWriteCloser, 0, numConns)
		)
		for i := 0; i < numConns; i++ {
			var (
				conn io.ReadWriteCloser
				fn   = func() error {
					var connectErr error
					conn, connectErr = w.connectFn(addr)
					return connectErr
				}
				resultErr error
			)
			if !opts.retry {
				resultErr = fn()
			} else {
				resultErr = w.connRetrier.AttemptWhile(w.continueFn, fn)
			}
			if resultErr != nil {
				return nil, resultErr
			}

			conns = append(conns, conn)
		}
		return conns, nil
	}
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
	if conn.timeout > 0 {
		conn.SetWriteDeadline(conn.nowFn().Add(conn.timeout))
	}
	return conn.Conn.Write(p)
}

type uninitializedReadWriter struct{}

func (u uninitializedReadWriter) Read(p []byte) (int, error)  { return 0, errInvalidConnection }
func (u uninitializedReadWriter) Write(p []byte) (int, error) { return 0, errInvalidConnection }
func (u uninitializedReadWriter) Close() error                { return nil }
