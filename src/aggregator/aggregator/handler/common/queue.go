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

package common

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

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
	errQueueClosed     = errors.New("queue is closed")
	errQueueFull       = errors.New("queue is full")
)

type dropType int

const (
	dropCurrent dropType = iota + 1
	dropOldestInQueue
)

type tryConnectFn func(addr string) (*net.TCPConn, error)
type dialWithTimeoutFn func(protocol string, addr string, timeout time.Duration) (net.Conn, error)
type writeToConnFn func(conn *net.TCPConn, data []byte) (int, error)

type queueMetrics struct {
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

func newQueueMetrics(scope tally.Scope) queueMetrics {
	return queueMetrics{
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

// Queue is a queue for queuing encoded data and sending them to backend servers.
type Queue interface {
	// Enqueue enqueues a ref-counted buffer.
	Enqueue(buffer *RefCountedBuffer) error

	// Close closes the queue.
	Close()
}

type queue struct {
	sync.RWMutex

	servers                []string
	log                    log.Logger
	nowFn                  clock.NowFn
	connectTimeout         time.Duration
	connectionKeepAlive    bool
	connectionWriteTimeout time.Duration
	reconnectRetrier       retry.Retrier
	reportInterval         time.Duration

	bufCh              chan *RefCountedBuffer
	wg                 sync.WaitGroup
	closed             bool
	closedCh           chan struct{}
	metrics            queueMetrics
	numOpenConnections int32
	tryConnectFn       tryConnectFn
	dialWithTimeoutFn  dialWithTimeoutFn
	writeToConnFn      writeToConnFn
}

// NewQueue creates a new queue.
func NewQueue(
	servers []string,
	opts QueueOptions,
) (Queue, error) {
	q, err := newQueue(servers, opts)
	if err != nil {
		return nil, err
	}

	q.initConnections(servers)
	go q.reportMetrics()

	return q, nil
}

func (q *queue) Enqueue(buffer *RefCountedBuffer) error {
	// NB(xichen): the buffer contains newly flushed data so it's preferrable to keep
	// it and drop the oldest buffer in queue in case the queue is full.
	return q.enqueue(buffer, dropOldestInQueue)
}

func (q *queue) Close() {
	q.Lock()
	if q.closed {
		q.Unlock()
		return
	}
	close(q.bufCh)
	close(q.closedCh)
	q.closed = true
	q.Unlock()

	// Wait for the queue to be drained.
	q.wg.Wait()
}

func (q *queue) initConnections(servers []string) {
	q.wg.Add(len(servers))
	for _, addr := range servers {
		conn, err := q.tryConnectFn(addr)
		if err != nil {
			q.log.WithFields(
				log.NewField("address", addr),
				log.NewErrField(err),
			).Error("error connecting to server")
		}
		go q.forwardToConn(addr, conn)
	}
}

func (q *queue) tryConnect(addr string) (*net.TCPConn, error) {
	conn, err := q.dialWithTimeoutFn(tcpProtocol, addr, q.connectTimeout)
	if err != nil {
		q.metrics.connectTimeouts.Inc(1)
		return nil, err
	}
	atomic.AddInt32(&q.numOpenConnections, 1)
	q.metrics.connectSuccesses.Inc(1)
	tcpConn := conn.(*net.TCPConn)
	tcpConn.SetKeepAlive(q.connectionKeepAlive)
	return tcpConn, nil
}

func (q *queue) enqueue(buffer *RefCountedBuffer, dropType dropType) error {
	q.RLock()
	if q.closed {
		q.RUnlock()
		buffer.DecRef()
		q.metrics.enqueueErrors.Inc(1)
		return errQueueClosed
	}
	for {
		select {
		case q.bufCh <- buffer:
			q.RUnlock()
			q.metrics.enqueueSuccesses.Inc(1)
			return nil
		default:
			if dropType == dropCurrent {
				q.RUnlock()
				buffer.DecRef()
				q.metrics.enqueueErrors.Inc(1)
				q.metrics.currentDropped.Inc(1)
				return errQueueFull
			}
		}

		// Drop oldest in queue to make room for new buffer.
		select {
		case buf := <-q.bufCh:
			buf.DecRef()
			q.metrics.oldestdDropped.Inc(1)
		default:
		}
	}
}

func (q *queue) forwardToConn(addr string, conn *net.TCPConn) {
	defer q.wg.Done()

	var err error
	continueFn := func(int) bool {
		q.RLock()
		closed := q.closed
		q.RUnlock()
		return !closed
	}
	connectFn := func() error {
		conn, err = q.tryConnectFn(addr)
		return err
	}

	for {
		// Retry establishing connection until either success or the queue is closed.
		for conn == nil {
			attemptErr := q.reconnectRetrier.AttemptWhile(continueFn, connectFn)
			if attemptErr == retry.ErrWhileConditionFalse {
				return
			}
		}

		// Dequeue buffers and forward to server.
		for {
			buf, ok := <-q.bufCh
			if !ok {
				conn.Close()
				atomic.AddInt32(&q.numOpenConnections, -1)
				return
			}
			_, err := q.writeToConnFn(conn, buf.Buffer().Bytes())
			if err == nil {
				q.metrics.writeSuccesses.Inc(1)
				buf.DecRef()
				continue
			}
			q.metrics.writeErrors.Inc(1)
			q.log.WithFields(
				log.NewField("address", addr),
				log.NewErrField(err),
			).Error("error writing to server")

			// NB(xichen): the buffer contains the oldest flushed data in queue
			// so it's preferrable to drop it in case the queue is full.
			if err := q.enqueue(buf, dropCurrent); err != nil {
				q.log.WithFields(
					log.NewField("address", addr),
					log.NewErrField(err),
				).Error("error enqueuing the buffer")
			}

			// Close and reset the connection.
			conn.Close()
			conn = nil
			atomic.AddInt32(&q.numOpenConnections, -1)
			break
		}
	}
}

func (q *queue) writeToConn(conn *net.TCPConn, data []byte) (int, error) {
	conn.SetWriteDeadline(q.nowFn().Add(q.connectionWriteTimeout))
	return conn.Write(data)
}

func (q *queue) reportMetrics() {
	t := time.NewTicker(q.reportInterval)
	for {
		select {
		case <-q.closedCh:
			t.Stop()
			return
		case <-t.C:
			q.metrics.openConnections.Update(float64(atomic.LoadInt32(&q.numOpenConnections)))
			q.metrics.queueSize.Update(float64(len(q.bufCh)))
		}
	}
}

func newQueue(servers []string, opts QueueOptions) (*queue, error) {
	if len(servers) == 0 {
		return nil, errEmptyServerList
	}

	instrumentOpts := opts.InstrumentOptions()
	connectionOpts := opts.ConnectionOptions()
	q := &queue{
		servers:                servers,
		log:                    instrumentOpts.Logger(),
		nowFn:                  opts.ClockOptions().NowFn(),
		connectTimeout:         connectionOpts.ConnectTimeout(),
		connectionKeepAlive:    connectionOpts.ConnectionKeepAlive(),
		connectionWriteTimeout: connectionOpts.ConnectionWriteTimeout(),
		reconnectRetrier:       retry.NewRetrier(connectionOpts.ReconnectRetryOptions().SetForever(true)),
		reportInterval:         instrumentOpts.ReportInterval(),
		bufCh:                  make(chan *RefCountedBuffer, opts.QueueSize()),
		closedCh:               make(chan struct{}),
		metrics:                newQueueMetrics(instrumentOpts.MetricsScope()),
		dialWithTimeoutFn:      net.DialTimeout,
	}
	q.tryConnectFn = q.tryConnect
	q.writeToConnFn = q.writeToConn
	return q, nil
}
