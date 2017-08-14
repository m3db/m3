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

package msgpack

import (
	"errors"
	"sync"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/log"

	"github.com/uber-go/tally"
)

var (
	errInstanceQueueClosed = errors.New("instance queue is closed")
	errWriterQueueFull     = errors.New("writer queue is full")
)

// instanceQueue processes write requests for given instance.
type instanceQueue interface {
	// Enqueue enqueues a data buffer.
	Enqueue(buf msgpack.Buffer) error

	// Close closes the queue.
	Close() error
}

type writeFn func([]byte) error

type queue struct {
	sync.RWMutex

	log      xlog.Logger
	metrics  queueMetrics
	instance services.PlacementInstance
	conn     *connection
	bufCh    chan msgpack.Buffer
	closed   bool

	writeFn writeFn
}

func newInstanceQueue(instance services.PlacementInstance, opts ServerOptions) instanceQueue {
	conn := newConnection(instance.Endpoint(), opts.ConnectionOptions())
	iOpts := opts.InstrumentOptions()
	q := &queue{
		log:      iOpts.Logger(),
		metrics:  newQueueMetrics(iOpts.MetricsScope()),
		instance: instance,
		conn:     conn,
		bufCh:    make(chan msgpack.Buffer, opts.InstanceQueueSize()),
	}
	q.writeFn = q.conn.Write

	go q.drain()
	return q
}

func (q *queue) Enqueue(buf msgpack.Buffer) error {
	q.RLock()
	if q.closed {
		q.RUnlock()
		q.metrics.queueClosedErrors.Inc(1)
		return errInstanceQueueClosed
	}
	// NB(xichen): the buffer already batches multiple metric buf points
	// to maximize per packet utilization so there should be no need to perform
	// additional batching here.
	select {
	case q.bufCh <- buf:
	default:
		q.RUnlock()

		// Close the buffer so it's resources are freed.
		buf.Close()

		q.metrics.queueFullErrors.Inc(1)
		return errWriterQueueFull
	}
	q.RUnlock()
	return nil
}

func (q *queue) Close() error {
	q.Lock()
	defer q.Unlock()

	if q.closed {
		return errInstanceQueueClosed
	}
	q.closed = true
	close(q.bufCh)
	return nil
}

func (q *queue) drain() {
	for buf := range q.bufCh {
		if err := q.writeFn(buf.Bytes()); err != nil {
			q.log.WithFields(
				xlog.NewLogField("instance", q.instance.Endpoint()),
				xlog.NewLogErrField(err),
			).Error("write data error")
			q.metrics.connWriteErrors.Inc(1)
		}
		buf.Close()
	}
	q.conn.Close()
}

type queueMetrics struct {
	queueClosedErrors tally.Counter
	queueFullErrors   tally.Counter
	connWriteErrors   tally.Counter
}

func newQueueMetrics(s tally.Scope) queueMetrics {
	return queueMetrics{
		queueClosedErrors: s.Tagged(
			map[string]string{"error-type": "queue-closed", "action": "enqueue"},
		).Counter("errors"),
		queueFullErrors: s.Tagged(
			map[string]string{"error-type": "queue-full", "action": "enqueue"},
		).Counter("errors"),
		connWriteErrors: s.Tagged(
			map[string]string{"error-type": "conn-write", "action": "drain"},
		).Counter("errors"),
	}
}
