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
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/x/clock"
	xio "github.com/m3db/m3/src/x/io"
	"github.com/m3db/m3/src/x/retry"
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

	// ForcedFlush forces a flush of the bytes in the buffer.
	ForcedFlush(connIndex int) error

	// Init initializes the consumer writer.
	Init()

	// Close closes the consumer writer.
	Close()

	// AvailableBuffer returns the available capacity in bytes in the send buffer.
	// Note that this only accounts for the capacity in the bufio layer and not
	// the bytes within the TCP sendbuffer.
	AvailableBuffer(connIndex int) int
}

type consumerWriterMetrics struct {
	writeInvalidConn             tally.Counter
	readInvalidConn              tally.Counter
	ackError                     tally.Counter
	decodeError                  tally.Counter
	encodeError                  tally.Counter
	resetTooSoon                 tally.Counter
	resetSuccess                 tally.Counter
	resetError                   tally.Counter
	connectError                 tally.Counter
	setKeepAliveError            tally.Counter
	setKeepAlivePeriodError      tally.Counter
	cwWriteTimeoutError          tally.Counter
	cwFlushTimeoutError          tally.Counter
	cwFlushLatency               tally.Histogram
	cwFlushLatencyWithLock       tally.Histogram
	cwForcedFlushTimeoutError    tally.Counter
	cwForcedFlushSkipped         tally.Counter
	cwForcedFlushLatency         tally.Histogram
	cwForcedFlushLatencyWithLock tally.Histogram
	cwWriteErrorLatency          tally.Histogram
	cwWriteErrorLatencyWithLock  tally.Histogram
	cwBufioWriterCastError       tally.Counter
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
		cwWriteTimeoutError:     scope.Counter("cw-write-timeout-error"),
		cwFlushTimeoutError:     scope.Counter("cw-flush-timeout-error"),
		cwFlushLatency: scope.Histogram("cw-flush-latency",
			tally.MustMakeExponentialDurationBuckets(time.Millisecond*10, 2, 15)),
		cwFlushLatencyWithLock: scope.Histogram("cw-flush-latency-with-lock",
			tally.MustMakeExponentialDurationBuckets(time.Millisecond*10, 2, 15)),
		cwForcedFlushTimeoutError: scope.Counter("cw-forced-flush-timeout-error"),
		cwForcedFlushSkipped:      scope.Counter("cw-forced-flush-skipped"),
		cwForcedFlushLatency: scope.Histogram("cw-forced-flush-latency",
			tally.MustMakeExponentialDurationBuckets(time.Millisecond*10, 2, 15)),
		cwForcedFlushLatencyWithLock: scope.Histogram("cw-forced-flush-latency-with-lock",
			tally.MustMakeExponentialDurationBuckets(time.Millisecond*10, 2, 15)),
		cwWriteErrorLatency: scope.Histogram("cw-write-error-latency",
			tally.MustMakeExponentialDurationBuckets(time.Millisecond*10, 2, 15)),
		cwWriteErrorLatencyWithLock: scope.Histogram("cw-write-error-latency-with-lock",
			tally.MustMakeExponentialDurationBuckets(time.Millisecond*10, 2, 15)),
		cwBufioWriterCastError: scope.Counter("cw-bufio-writer-cast-error"),
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

	closed     bool
	validConns bool

	// conns keeps active connections.
	// Note: readers will take a reference to this slice with a lock
	// then loop through it and call decode on decoders, so not safe
	// to reuse.
	conns          []*connection
	lastResetNanos int64
	forcedFlush    forcedFlushState
}

type forcedFlushState struct {
	mu         sync.Mutex
	cond       *sync.Cond
	inProgress bool
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

	logger := opts.InstrumentOptions().Logger()
	connOpts := opts.ConnectionOptions()
	w := &consumerWriterImpl{
		addr:        addr,
		router:      router,
		opts:        opts,
		connOpts:    connOpts,
		ackRetrier:  retry.NewRetrier(opts.AckErrorRetryOptions()),
		connRetrier: retry.NewRetrier(connOpts.RetryOptions().SetForever(defaultRetryForever)),
		logger:      logger,
		resetCh:     make(chan struct{}, 1),
		doneCh:      make(chan struct{}),
		m:           m,
		nowFn:       time.Now,
	}
	w.writeState.forcedFlush.cond = sync.NewCond(&w.writeState.forcedFlush.mu)
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
	logger.Info("consumer writer created", zap.String("address", addr))
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
	startWriteWithLockTs := w.nowFn()
	writeConn.writeLock.Lock()
	startWriteTs := w.nowFn()
	_, err := writeConn.w.Write(b)
	if err != nil {
		var netErr *net.OpError
		if errors.As(err, &netErr) && netErr.Timeout() {
			w.logger.Warn("consumer writer write timeout", zap.String("address", w.addr))
			w.m.cwWriteTimeoutError.Inc(1)
		}
	}
	endWriteTs := w.nowFn()
	writeConn.writeLock.Unlock()
	endWriteWithLockTs := w.nowFn()

	// Hold onto the write state lock until done, since
	// closing connections are done by acquiring the write state lock.
	w.writeState.RUnlock()

	if err != nil {
		w.notifyReset(err)
		w.m.encodeError.Inc(1)
		w.m.cwWriteErrorLatencyWithLock.RecordDuration(endWriteWithLockTs.Sub(startWriteWithLockTs))
		w.m.cwWriteErrorLatency.RecordDuration(endWriteTs.Sub(startWriteTs))
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

// ForcedFlush the data for a consumer writer.
func (w *consumerWriterImpl) ForcedFlush(connIndex int) error {
	waited := false
	w.writeState.forcedFlush.mu.Lock()
	for w.writeState.forcedFlush.inProgress {
		waited = true
		// Wait for the in progress flush to finish.
		w.writeState.forcedFlush.cond.Wait()
	}
	if waited {
		// we don't need to perform a flush as we just got done.
		// simply
		//   unblock other forcedflushers
		// 	 release the lock
		//   return success.
		w.writeState.forcedFlush.cond.Broadcast()
		w.writeState.forcedFlush.mu.Unlock()
		w.logger.Info("forced flush skipped", zap.String("address", w.addr))
		w.m.cwForcedFlushSkipped.Inc(1)
		return nil
	}

	// we need to perform a forced flush now.
	w.writeState.forcedFlush.inProgress = true
	w.writeState.forcedFlush.mu.Unlock()

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
	// Make sure this is the only writer to this connection.
	startFlushWithLockTs := w.nowFn()

	writeConn.writeLock.Lock()
	startFlushTs := w.nowFn()

	if err := writeConn.w.Flush(); err != nil {
		var netErr *net.OpError
		if errors.As(err, &netErr) && netErr.Timeout() {
			w.logger.Warn("consumer writer forced flush timeout", zap.String("address", w.addr))
			w.m.cwForcedFlushTimeoutError.Inc(1)
		}
		w.notifyReset(err)
	}
	endFlushTs := w.nowFn()

	writeConn.writeLock.Unlock()
	endFlushWithLockTs := w.nowFn()

	w.writeState.RUnlock()

	// Set flush in progress to false.
	w.writeState.forcedFlush.mu.Lock()
	w.writeState.forcedFlush.inProgress = false
	w.writeState.forcedFlush.cond.Broadcast()
	w.writeState.forcedFlush.mu.Unlock()

	w.m.cwForcedFlushLatency.RecordDuration(endFlushTs.Sub(startFlushTs))
	w.m.cwForcedFlushLatencyWithLock.RecordDuration(endFlushWithLockTs.Sub(startFlushWithLockTs))

	return nil
}

func (w *consumerWriterImpl) AvailableBuffer(connIndex int) int {
	w.writeState.RLock()
	defer w.writeState.RUnlock()
	if !w.writeState.validConns || len(w.writeState.conns) == 0 {
		return 0
	}

	if connIndex < 0 || connIndex >= len(w.writeState.conns) {
		return 0
	}

	conn := w.writeState.conns[connIndex]
	if conn.w == nil {
		return 0
	}

	buf, ok := conn.w.(*bufio.Writer)
	if !ok {
		w.m.cwBufioWriterCastError.Inc(1)
		return math.MaxInt
	}
	return buf.Available()
}

func (w *consumerWriterImpl) flushUntilClose() {
	flushTicker := time.NewTicker(w.connOpts.FlushInterval())
	defer flushTicker.Stop()

	for {
		select {
		case <-flushTicker.C:
			w.writeState.RLock()
			for _, conn := range w.writeState.conns {
				startFlushWithLockTs := w.nowFn()
				conn.writeLock.Lock()
				startFlushTs := w.nowFn()
				if err := conn.w.Flush(); err != nil {
					var netErr *net.OpError
					if errors.As(err, &netErr) && netErr.Timeout() {
						w.logger.Warn("consumer writer flush timeout", zap.String("address", w.addr))
						w.m.cwFlushTimeoutError.Inc(1)
					}
					w.notifyReset(err)
				}
				endFlushTs := w.nowFn()
				conn.writeLock.Unlock()
				endFlushWithLockTs := w.nowFn()

				w.m.cwFlushLatency.RecordDuration(endFlushTs.Sub(startFlushTs))
				w.m.cwFlushLatencyWithLock.RecordDuration(endFlushWithLockTs.Sub(startFlushWithLockTs))
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
	existingConns := []*connection{}
	w.writeState.Lock()
	if w.writeState.validConns {
		w.writeState.validConns = false
		existingConns = w.writeState.conns
	}
	w.writeState.Unlock()

	// Close the existing connections.
	for _, c := range existingConns {
		if err := w.closeConnection(c); err != nil {
			w.logger.Warn("close connection failed", zap.Error(err))
		}
	}

	// Now that the existing connections have been closed,
	// we can re-attempt connections.
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

func (w *consumerWriterImpl) closeConnection(c *connection) error {
	if w.connOpts.AbortOnServerClose() {
		// set linger off to abort the connection immediately.
		rw, ok := c.conn.(readWriterWithTimeout)
		if ok && rw.setLingerOff != nil {
			err := rw.setLingerOff()
			if err != nil {
				w.logger.Warn("could not set linger", zap.Error(err))
			}
		} else {
			w.logger.Warn("could not set linger, not a TCP connection")
		}
	}

	w.logger.Info("closing connection on server reset", zap.String("address", w.addr))
	if err := c.conn.Close(); err != nil {
		w.logger.Warn(
			"could not close connection",
			zap.Error(err),
			zap.String("address", w.addr),
		)
		return err
	}

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
	w.logger.Info("closing consumer writer", zap.String("address", w.addr))
	w.writeState.Lock()
	wasClosed := w.writeState.closed
	w.writeState.closed = true
	w.writeState.Unlock()

	if wasClosed {
		return
	}

	close(w.doneCh)

	w.wg.Wait()
	w.logger.Info("closed consumer writer", zap.String("address", w.addr))
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
	defer w.writeState.Unlock()

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

	var setLingerOffFn setLingerOffFn
	if conn, ok := conn.(*net.TCPConn); ok {
		setLingerOffFn = func() error {
			return conn.SetLinger(0)
		}
	}

	tcpConn, ok := conn.(keepAlivable)
	if !ok {
		// If using a custom dialer which doesn't return *net.TCPConn, users are responsible for TCP keep alive options
		// themselves.
		return newReadWriterWithTimeout(
			conn,
			w.connOpts.WriteTimeout(),
			setLingerOffFn,
			w.nowFn,
		), nil
	}
	if err = tcpConn.SetKeepAlive(true); err != nil {
		w.m.setKeepAliveError.Inc(1)
	}
	keepAlivePeriod := w.connOpts.KeepAlivePeriod()
	if keepAlivePeriod <= 0 {
		return newReadWriterWithTimeout(
			conn,
			w.connOpts.WriteTimeout(),
			setLingerOffFn,
			w.nowFn,
		), nil
	}
	if err = tcpConn.SetKeepAlivePeriod(keepAlivePeriod); err != nil {
		w.m.setKeepAlivePeriodError.Inc(1)
	}
	return newReadWriterWithTimeout(
		conn,
		w.connOpts.WriteTimeout(),
		setLingerOffFn,
		w.nowFn,
	), nil
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

type setLingerOffFn func() error

type readWriterWithTimeout struct {
	net.Conn

	setLingerOff setLingerOffFn
	timeout      time.Duration
	nowFn        clock.NowFn
}

func newReadWriterWithTimeout(
	conn net.Conn,
	timeout time.Duration,
	setLingerOff setLingerOffFn,
	nowFn clock.NowFn,
) readWriterWithTimeout {
	return readWriterWithTimeout{
		Conn:         conn,
		timeout:      timeout,
		setLingerOff: setLingerOff,
		nowFn:        nowFn,
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
