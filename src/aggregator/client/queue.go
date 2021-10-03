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
	"io"
	"math"
	"strings"
	"sync"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	_queueMinWriteBufSize = 65536
	_queueMaxWriteBufSize = 8 * _queueMinWriteBufSize
)

var (
	errInstanceQueueClosed = errors.New("instance queue is closed")
	errWriterQueueFull     = errors.New("writer queue is full")
	errInvalidDropType     = errors.New("invalid queue drop type")

	_queueConnWriteBufPool = sync.Pool{New: func() interface{} {
		b := make([]byte, 0, _queueMinWriteBufSize)
		return &b
	}}
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

	// Flush flushes the queue, it blocks until the queue is drained.
	Flush()
}

type writeFn func([]byte) error

type queue struct {
	metrics  queueMetrics
	instance placement.Instance
	conn     *connection
	log      *zap.Logger
	writeFn  writeFn
	buf      qbuf
	dropType DropType
	closed   atomic.Bool
	mtx      sync.Mutex
}

func newInstanceQueue(instance placement.Instance, opts Options) instanceQueue {
	var (
		instrumentOpts     = opts.InstrumentOptions()
		scope              = instrumentOpts.MetricsScope()
		connInstrumentOpts = instrumentOpts.SetMetricsScope(scope.SubScope("connection"))
		connOpts           = opts.ConnectionOptions().
					SetInstrumentOptions(connInstrumentOpts).
					SetRWOptions(opts.RWOptions())
		conn      = newConnection(instance.Endpoint(), connOpts)
		iOpts     = opts.InstrumentOptions()
		queueSize = opts.InstanceQueueSize()
	)

	// Round up queue size to power of 2.
	// buf is a ring buffer of byte buffers, so it should definitely be many orders of magnitude
	// below max uint32.
	qsize := uint32(roundUpToPowerOfTwo(queueSize))

	q := &queue{
		dropType: opts.QueueDropType(),
		log:      iOpts.Logger(),
		metrics:  newQueueMetrics(iOpts.MetricsScope()),
		instance: instance,
		conn:     conn,
		buf: qbuf{
			b: make([]protobuf.Buffer, int(qsize)),
		},
	}
	q.writeFn = q.conn.Write

	return q
}

func (q *queue) Enqueue(buf protobuf.Buffer) error {
	if q.closed.Load() {
		q.metrics.enqueueClosedErrors.Inc(1)
		return errInstanceQueueClosed
	}

	if len(buf.Bytes()) == 0 {
		return nil
	}

	q.mtx.Lock()
	defer q.mtx.Unlock()

	if full := q.buf.full(); full {
		switch q.dropType {
		case DropCurrent:
			// Close the current buffer so it's resources are freed.
			buf.Close()
			q.metrics.enqueueCurrentDropped.Inc(1)
			return errWriterQueueFull
		case DropOldest:
			// Consume oldest buffer instead.
			oldest := q.buf.shift()
			oldest.Close()
			q.metrics.enqueueOldestDropped.Inc(1)
		default:
			return errInvalidDropType
		}
	}

	q.buf.push(buf)
	q.metrics.enqueueSuccesses.Inc(1)
	return nil
}

func (q *queue) Close() error {
	if !q.closed.CAS(false, true) {
		return errInstanceQueueClosed
	}

	return nil
}

func (q *queue) Flush() {
	var (
		buf = _queueConnWriteBufPool.Get().(*[]byte)
		n   int
		err error
	)

	for err == nil {
		// flush everything in batches, to make sure no single payload is too large,
		// to prevent a) allocs and b) timeouts due to big buffer IO taking too long.
		var processed int
		processed, err = q.flush(buf)
		n += processed
	}

	if err != nil && !errors.Is(err, io.EOF) {
		q.log.Error("error writing data",
			zap.String("target_instance_id", q.instance.ID()),
			zap.String("target_instance", q.instance.Endpoint()),
			zap.Int("bytes_processed", n),
			zap.Error(err),
		)
	}

	// Check buffer capacity, not length, to make sure we're not pooling slices that are too large.
	// Otherwise, it could result in multi-megabyte slices hanging around, in case we get a single massive write.
	if cap(*buf) <= _queueMaxWriteBufSize {
		*buf = (*buf)[:0]
		_queueConnWriteBufPool.Put(buf)
	}
}

func (q *queue) flush(tmpWriteBuf *[]byte) (int, error) {
	var n int

	q.mtx.Lock()

	if q.buf.size() == 0 {
		q.mtx.Unlock()
		return n, io.EOF
	}

	*tmpWriteBuf = (*tmpWriteBuf)[:0]
	for q.buf.size() > 0 {
		protoBuffer := q.buf.peek()
		bytes := protoBuffer.Bytes()

		if n > 0 && len(bytes)+len(*tmpWriteBuf) >= _queueMaxWriteBufSize {
			// only merge buffers that are smaller than _queueMaxWriteBufSize bytes
			break
		}
		_ = q.buf.shift()

		if len(bytes) == 0 {
			continue
		}

		*tmpWriteBuf = append(*tmpWriteBuf, bytes...)
		n += len(bytes)
		protoBuffer.Close()
	}

	// mutex is not held while doing IO
	q.mtx.Unlock()

	if n == 0 {
		return n, io.EOF
	}

	if err := q.writeFn(*tmpWriteBuf); err != nil {
		q.metrics.connWriteErrors.Inc(1)
		return n, err
	}

	q.metrics.connWriteSuccesses.Inc(1)

	return n, nil
}

func (q *queue) Size() int {
	return int(q.buf.size())
}

type queueMetrics struct {
	enqueueSuccesses      tally.Counter
	enqueueOldestDropped  tally.Counter
	enqueueCurrentDropped tally.Counter
	enqueueClosedErrors   tally.Counter
	connWriteSuccesses    tally.Counter
	connWriteErrors       tally.Counter
}

func newQueueMetrics(s tally.Scope) queueMetrics {
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

// qbuf is a specialized ring buffer for proto payloads
type qbuf struct {
	b []protobuf.Buffer
	// buffer cursors
	r uint32
	w uint32
}

func (q *qbuf) size() uint32 {
	return q.w - q.r
}

func (q *qbuf) full() bool {
	return q.size() == uint32(cap(q.b))
}

func (q *qbuf) mask(idx uint32) uint32 {
	return idx & (uint32(cap(q.b)) - 1)
}

func (q *qbuf) push(buf protobuf.Buffer) {
	q.w++
	idx := q.mask(q.w)
	q.b[idx].Close()
	q.b[idx] = buf
}

func (q *qbuf) shift() protobuf.Buffer {
	q.r++
	idx := q.mask(q.r)
	val := q.b[idx]
	q.b[idx] = protobuf.Buffer{}
	return val
}

func (q *qbuf) peek() protobuf.Buffer {
	idx := q.mask(q.r + 1)
	return q.b[idx]
}

func roundUpToPowerOfTwo(val int) int {
	return int(math.Pow(2, math.Ceil(math.Log2(float64(val)))))
}
