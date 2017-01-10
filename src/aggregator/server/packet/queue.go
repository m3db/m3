// Copyright (c) 2016 Uber Technologies, Inc.
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

package packet

import (
	"errors"
	"sync/atomic"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/uber-go/tally"
)

var (
	errQueueClosed = errors.New("queue is closed")
)

// Packet is a packet containing an unaggregated metric along with applicable versioned policies
type Packet struct {
	Metric   unaggregated.MetricUnion
	Policies policy.VersionedPolicies
}

type queueMetrics struct {
	enqueue   instrument.MethodMetrics
	dequeue   instrument.MethodMetrics
	discarded tally.Counter
}

func newQueueMetrics(scope tally.Scope, samplingRate float64) queueMetrics {
	return queueMetrics{
		enqueue:   instrument.NewMethodMetrics(scope, "enqueue", samplingRate),
		dequeue:   instrument.NewMethodMetrics(scope, "dequeue", samplingRate),
		discarded: scope.Counter("discarded"),
	}
}

// Queue is a packet queue. It is a fixed-size queue for incoming packets.
// If the queue is full when enqueuing packets, oldest packets will be
// dropped until there is more space in the queue
type Queue struct {
	nowFn   clock.NowFn
	queue   chan Packet
	metrics queueMetrics
	closed  int32
}

// NewQueue creates a new packet queue
func NewQueue(
	size int,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
) *Queue {
	scope := instrumentOpts.MetricsScope().SubScope("queue")
	samplingRate := instrumentOpts.MetricsSamplingRate()
	return &Queue{
		nowFn:   clockOpts.NowFn(),
		queue:   make(chan Packet, size),
		metrics: newQueueMetrics(scope, samplingRate),
	}
}

// Len returns the number of items in the queue
func (q *Queue) Len() int { return len(q.queue) }

// Enqueue pushes a packet into the queue
func (q *Queue) Enqueue(p Packet) error {
	callStart := q.nowFn()
	for atomic.LoadInt32(&q.closed) == 0 {
		select {
		case q.queue <- p:
			q.metrics.enqueue.ReportSuccess(q.nowFn().Sub(callStart))
			return nil
		default:
		}

		select {
		case <-q.queue:
			q.metrics.discarded.Inc(1)
		default:
		}
	}

	q.metrics.enqueue.ReportError(q.nowFn().Sub(callStart))
	return errQueueClosed
}

// Dequeue pops a packet from the queue
func (q *Queue) Dequeue() (Packet, error) {
	callStart := q.nowFn()
	p, ok := <-q.queue
	if !ok {
		q.metrics.dequeue.ReportError(q.nowFn().Sub(callStart))
		return Packet{}, errQueueClosed
	}
	q.metrics.dequeue.ReportSuccess(q.nowFn().Sub(callStart))
	return p, nil
}

// Close closes the queue
func (q *Queue) Close() {
	if !atomic.CompareAndSwapInt32(&q.closed, 0, 1) {
		return
	}
	close(q.queue)
}
