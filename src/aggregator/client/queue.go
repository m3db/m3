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

package client

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errInstanceQueueClosed = errors.New("instance queue is closed")
	errWriterQueueFull     = errors.New("writer queue is full")
)

// DropType determines which metrics should be dropped when the queue is full.
type DropType int

const (
	// DropOldest signifies that the oldest metrics in the queue should be dropped.
	DropOldest DropType = iota

	// DropCurrent signifies that the current metrics in the queue should be dropped.
	DropCurrent
)

var (
	validDropTypes = []DropType{
		DropOldest,
		DropCurrent,
	}
)

func (t DropType) String() string {
	switch t {
	case DropOldest:
		return "oldest"
	case DropCurrent:
		return "current"
	}
	return "unknown"
}

// UnmarshalYAML unmarshals a DropType into a valid type from string.
func (t *DropType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		*t = defaultDropType
		return nil
	}
	strs := make([]string, 0, len(validDropTypes))
	for _, valid := range validDropTypes {
		if str == valid.String() {
			*t = valid
			return nil
		}
		strs = append(strs, "'"+valid.String()+"'")
	}
	return fmt.Errorf(
		"invalid DropType '%s' valid types are: %s", str, strings.Join(strs, ", "),
	)
}

// instanceQueue processes write requests for given instance.
type instanceQueue interface {
	// Enqueue enqueues a data buffer.
	Enqueue(buf protobuf.Buffer) error

	// Size returns the number of items in the queue.
	Size() int

	// Close closes the queue, it blocks until the queue is drained.
	Close() error
}

type writeFn func([]byte) error

type queue struct {
	sync.RWMutex

	log                *zap.Logger
	metrics            queueMetrics
	dropType           DropType
	instance           placement.Instance
	conn               *connection
	bufCh              chan protobuf.Buffer
	doneCh             chan struct{}
	closed             bool
	buf                []byte
	maxBatchSize       int
	batchFlushDeadline time.Duration
	wg                 sync.WaitGroup

	writeFn writeFn
}

func newInstanceQueue(instance placement.Instance, opts Options) instanceQueue {
	var (
		instrumentOpts     = opts.InstrumentOptions()
		scope              = instrumentOpts.MetricsScope()
		connInstrumentOpts = instrumentOpts.SetMetricsScope(scope.SubScope("connection"))
		connOpts           = opts.ConnectionOptions().
					SetInstrumentOptions(connInstrumentOpts).
					SetRWOptions(opts.RWOptions())
		conn          = newConnection(instance.Endpoint(), connOpts)
		iOpts         = opts.InstrumentOptions()
		queueSize     = opts.InstanceQueueSize()
		maxBatchSize  = opts.MaxBatchSize()
		writeInterval = opts.BatchFlushDeadline()
	)
	q := &queue{
		dropType:           opts.QueueDropType(),
		log:                iOpts.Logger(),
		metrics:            newQueueMetrics(iOpts.MetricsScope(), queueSize),
		instance:           instance,
		conn:               conn,
		bufCh:              make(chan protobuf.Buffer, queueSize),
		doneCh:             make(chan struct{}),
		maxBatchSize:       maxBatchSize,
		batchFlushDeadline: writeInterval,
		buf:                make([]byte, 0, maxBatchSize),
	}
	q.writeFn = q.conn.Write

	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		q.drain()
	}()

	return q
}

func (q *queue) Enqueue(buf protobuf.Buffer) error {
	q.RLock()
	if q.closed {
		q.RUnlock()
		q.metrics.enqueueClosedErrors.Inc(1)
		return errInstanceQueueClosed
	}
	for {
		select {
		case q.bufCh <- buf:
			q.RUnlock()
			q.metrics.enqueueSuccesses.Inc(1)
			return nil

		default:
			if q.dropType == DropCurrent {
				q.RUnlock()

				// Close the buffer so it's resources are freed.
				buf.Close()
				q.metrics.enqueueCurrentDropped.Inc(1)
				return errWriterQueueFull
			}
		}

		select {
		case buf := <-q.bufCh:
			// Close the buffer so it's resources are freed.
			buf.Close()
			q.metrics.enqueueOldestDropped.Inc(1)
		default:
		}
	}
}

func (q *queue) Close() error {
	q.Lock()
	if q.closed {
		return errInstanceQueueClosed
	}
	q.closed = true
	q.Unlock()

	close(q.doneCh)
	q.wg.Wait()

	close(q.bufCh)
	return nil
}

func (q *queue) writeAndReset() {
	if len(q.buf) == 0 {
		return
	}
	if err := q.writeFn(q.buf); err != nil {
		q.log.Error("error writing data",
			zap.Int("buffer_size", len(q.buf)),
			zap.String("target_instance_id", q.instance.ID()),
			zap.String("target_instance", q.instance.Endpoint()),
			zap.Error(err),
		)
		q.metrics.connWriteErrors.Inc(1)
	} else {
		q.metrics.connWriteSuccesses.Inc(1)
	}
	q.buf = q.buf[:0]
}

func (q *queue) drain() {
	defer q.conn.Close()
	timer := time.NewTimer(q.batchFlushDeadline)
	lastDrain := time.Now()
	write := func() {
		q.writeAndReset()
		lastDrain = time.Now()
	}

	for {
		select {
		case qitem := <-q.bufCh:
			drained := false
			msg := qitem.Bytes()
			if len(q.buf)+len(msg) > q.maxBatchSize {
				write()
				drained = true
			}
			q.buf = append(q.buf, msg...)
			qitem.Close()

			if drained || (len(q.buf) < q.maxBatchSize &&
				time.Since(lastDrain) < q.batchFlushDeadline) {
				continue
			}

			write()
		case ts := <-timer.C:
			delta := ts.Sub(lastDrain)
			if delta < q.batchFlushDeadline {
				timer.Reset(q.batchFlushDeadline - delta)
				continue
			}
			write()
			timer.Reset(q.batchFlushDeadline)
		case <-q.doneCh:
			return
		}
	}
}

func (q *queue) Size() int {
	return len(q.bufCh)
}

type queueMetrics struct {
	enqueueSuccesses      tally.Counter
	enqueueOldestDropped  tally.Counter
	enqueueCurrentDropped tally.Counter
	enqueueClosedErrors   tally.Counter
	connWriteSuccesses    tally.Counter
	connWriteErrors       tally.Counter
}

func newQueueMetrics(s tally.Scope, queueSize int) queueMetrics {
	enqueueScope := s.Tagged(map[string]string{"action": "enqueue"})
	connWriteScope := s.Tagged(map[string]string{"action": "conn-write"})
	return queueMetrics{
		enqueueSuccesses: enqueueScope.Counter("successes"),
		enqueueOldestDropped: enqueueScope.Tagged(map[string]string{"drop-type": "oldest"}).
			Counter("dropped"),
		enqueueCurrentDropped: enqueueScope.Tagged(map[string]string{"drop-type": "current"}).
			Counter("dropped"),
		enqueueClosedErrors: enqueueScope.Tagged(map[string]string{"error-type": "queue-closed"}).
			Counter("errors"),
		connWriteSuccesses: connWriteScope.Counter("successes"),
		connWriteErrors:    connWriteScope.Counter("errors"),
	}
}
