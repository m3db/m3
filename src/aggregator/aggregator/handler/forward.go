// Copyright (c) 2017 Uber Technologies, Inc.
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

package handler

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
)

const (
	tcpProtocol = "tcp"
)

var (
	errEmptyServerList = errors.New("empty server list")
	errHandlerClosed   = errors.New("handler is closed")
	errBufferQueueFull = errors.New("buffer queue is full")
)

type dropType int

const (
	dropCurrent dropType = iota + 1
	dropOldestInQueue
)

type tryConnectFn func(addr string) (*net.TCPConn, error)
type dialWithTimeoutFn func(protocol string, addr string, timeout time.Duration) (net.Conn, error)
type writeToConnFn func(conn *net.TCPConn, data []byte) (int, error)

type forwardHandlerMetrics struct {
	connectTimeouts  tally.Counter
	connectSuccesses tally.Counter
	enqueueErrors    tally.Counter
	enqueueSuccesses tally.Counter
	currentDropped   tally.Counter
	oldestdDropped   tally.Counter
	writeSuccesses   tally.Counter
	writeErrors      tally.Counter
	openConnections  tally.Gauge
	queueSize        tally.Gauge
}

func newForwardHandlerMetrics(scope tally.Scope) forwardHandlerMetrics {
	return forwardHandlerMetrics{
		connectTimeouts:  scope.Counter("connect-timeouts"),
		connectSuccesses: scope.Counter("connect-successes"),
		enqueueErrors:    scope.Counter("enqueue-errors"),
		enqueueSuccesses: scope.Counter("enqueue-successes"),
		currentDropped:   scope.Counter("current-dropped"),
		oldestdDropped:   scope.Counter("oldest-dropped"),
		writeSuccesses:   scope.Counter("write-successes"),
		writeErrors:      scope.Counter("write-errors"),
		openConnections:  scope.Gauge("open-connections"),
		queueSize:        scope.Gauge("queue-size"),
	}
}

type forwardHandler struct {
	sync.RWMutex

	servers                []string
	log                    xlog.Logger
	nowFn                  clock.NowFn
	connectTimeout         time.Duration
	connectionKeepAlive    bool
	connectionWriteTimeout time.Duration
	reconnectRetrier       xretry.Retrier
	reportInterval         time.Duration

	bufCh              chan msgpack.Buffer
	wg                 sync.WaitGroup
	closed             bool
	closedCh           chan struct{}
	metrics            forwardHandlerMetrics
	numOpenConnections int32
	tryConnectFn       tryConnectFn
	dialWithTimeoutFn  dialWithTimeoutFn
	writeToConnFn      writeToConnFn
}

// NewForwardHandler creates a new forwarding handler.
func NewForwardHandler(
	servers []string,
	opts ForwardHandlerOptions,
) (aggregator.Handler, error) {
	h, err := newForwardHandler(servers, opts)
	if err != nil {
		return nil, err
	}

	h.initConnections(servers)
	go h.reportMetrics()

	return h, nil
}

func (h *forwardHandler) Handle(buffer msgpack.Buffer) error {
	// NB(xichen): the buffer contains newly flushed data so it's preferrable to keep
	// it and drop the oldest buffer in queue in case the queue is full.
	return h.enqueue(buffer, dropOldestInQueue)
}

func (h *forwardHandler) Close() {
	h.Lock()
	if h.closed {
		h.Unlock()
		return
	}
	h.closed = true
	close(h.bufCh)
	close(h.closedCh)
	h.Unlock()

	// Wait for the queue to be drained.
	h.wg.Wait()
}

func (h *forwardHandler) initConnections(servers []string) {
	h.wg.Add(len(servers))
	for _, addr := range servers {
		conn, err := h.tryConnectFn(addr)
		if err != nil {
			h.log.WithFields(
				xlog.NewLogField("address", addr),
				xlog.NewLogErrField(err),
			).Error("error connecting to server")
		}
		go h.forwardToConn(addr, conn)
	}
}

func (h *forwardHandler) tryConnect(addr string) (*net.TCPConn, error) {
	conn, err := h.dialWithTimeoutFn(tcpProtocol, addr, h.connectTimeout)
	if err != nil {
		h.metrics.connectTimeouts.Inc(1)
		return nil, err
	}
	atomic.AddInt32(&h.numOpenConnections, 1)
	h.metrics.connectSuccesses.Inc(1)
	tcpConn := conn.(*net.TCPConn)
	tcpConn.SetKeepAlive(h.connectionKeepAlive)
	return tcpConn, nil
}

func (h *forwardHandler) enqueue(buffer msgpack.Buffer, dropType dropType) error {
	h.RLock()
	if h.closed {
		h.RUnlock()
		h.metrics.enqueueErrors.Inc(1)
		return errHandlerClosed
	}
	for {
		select {
		case h.bufCh <- buffer:
			h.RUnlock()
			h.metrics.enqueueSuccesses.Inc(1)
			return nil
		default:
			if dropType == dropCurrent {
				h.RUnlock()
				buffer.Close()
				h.metrics.enqueueErrors.Inc(1)
				h.metrics.currentDropped.Inc(1)
				return errBufferQueueFull
			}
		}

		// Drop oldest in queue to make room for new buffer.
		select {
		case buf := <-h.bufCh:
			buf.Close()
			h.metrics.oldestdDropped.Inc(1)
		default:
		}
	}
	panic("unreachable code")
	return nil
}

func (h *forwardHandler) forwardToConn(addr string, conn *net.TCPConn) {
	defer h.wg.Done()

	var err error
	continueFn := func(int) bool {
		h.RLock()
		closed := h.closed
		h.RUnlock()
		return !closed
	}
	connectFn := func() error {
		conn, err = h.tryConnectFn(addr)
		return err
	}

	for {
		// Retry establishing connection until either success or the handler is closed.
		for conn == nil {
			attemptErr := h.reconnectRetrier.AttemptWhile(continueFn, connectFn)
			if attemptErr == xretry.ErrWhileConditionFalse {
				return
			}
		}

		// Dequeue buffers and forward to server.
		for {
			buf, ok := <-h.bufCh
			if !ok {
				conn.Close()
				atomic.AddInt32(&h.numOpenConnections, -1)
				return
			}
			_, writeErr := h.writeToConnFn(conn, buf.Bytes())
			if writeErr == nil {
				h.metrics.writeSuccesses.Inc(1)
				buf.Close()
				continue
			}
			h.metrics.writeErrors.Inc(1)
			h.log.WithFields(
				xlog.NewLogField("address", addr),
				xlog.NewLogErrField(writeErr),
			).Error("error writing to server")

			// NB(xichen): the buffer contains the oldest flushed data in queue
			// so it's preferrable to drop it in case the queue is full.
			if enqueueErr := h.enqueue(buf, dropCurrent); enqueueErr != nil {
				h.log.WithFields(
					xlog.NewLogField("address", addr),
					xlog.NewLogErrField(enqueueErr),
				).Error("error enqueuing the buffer")
			}

			// Close and reset the connection.
			conn.Close()
			conn = nil
			atomic.AddInt32(&h.numOpenConnections, -1)
			break
		}
	}
}

func (h *forwardHandler) writeToConn(conn *net.TCPConn, data []byte) (int, error) {
	conn.SetWriteDeadline(h.nowFn().Add(h.connectionWriteTimeout))
	return conn.Write(data)
}

func (h *forwardHandler) reportMetrics() {
	t := time.NewTicker(h.reportInterval)
	for {
		select {
		case <-h.closedCh:
			t.Stop()
			return
		case <-t.C:
			h.metrics.openConnections.Update(float64(atomic.LoadInt32(&h.numOpenConnections)))
			h.metrics.queueSize.Update(float64(len(h.bufCh)))
		}
	}
}

func newForwardHandler(servers []string, opts ForwardHandlerOptions) (*forwardHandler, error) {
	if len(servers) == 0 {
		return nil, errEmptyServerList
	}

	instrumentOpts := opts.InstrumentOptions()
	h := &forwardHandler{
		servers:                servers,
		log:                    instrumentOpts.Logger(),
		nowFn:                  opts.ClockOptions().NowFn(),
		connectTimeout:         opts.ConnectTimeout(),
		connectionKeepAlive:    opts.ConnectionKeepAlive(),
		connectionWriteTimeout: opts.ConnectionWriteTimeout(),
		reconnectRetrier:       opts.ReconnectRetrier(),
		reportInterval:         instrumentOpts.ReportInterval(),
		bufCh:                  make(chan msgpack.Buffer, opts.QueueSize()),
		closedCh:               make(chan struct{}),
		metrics:                newForwardHandlerMetrics(instrumentOpts.MetricsScope()),
		dialWithTimeoutFn:      net.DialTimeout,
	}
	h.tryConnectFn = h.tryConnect
	h.writeToConnFn = h.writeToConn
	return h, nil
}
