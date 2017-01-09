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

package server

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

type packet struct {
	metric   unaggregated.MetricUnion
	policies policy.VersionedPolicies
}

type packetQueueMetrics struct {
	enqueue   instrument.MethodMetrics
	dequeue   instrument.MethodMetrics
	discarded tally.Counter
}

func newPacketQueueMetrics(scope tally.Scope, samplingRate float64) packetQueueMetrics {
	return packetQueueMetrics{
		enqueue:   instrument.NewMethodMetrics(scope, "enqueue", samplingRate),
		dequeue:   instrument.NewMethodMetrics(scope, "dequeue", samplingRate),
		discarded: scope.Counter("discarded"),
	}
}

// NB(xichen): packet queue is a fixed-size queue for incoming packets.
// If the queue is full when enqueuing packets, oldest packets will be
// dropped until there is more space in the queue
type packetQueue struct {
	nowFn   clock.NowFn
	queue   chan packet
	metrics packetQueueMetrics
	closed  int32
}

func newPacketQueue(
	size int,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
) *packetQueue {
	scope := instrumentOpts.MetricsScope().SubScope("queue")
	samplingRate := instrumentOpts.MetricsSamplingRate()
	return &packetQueue{
		nowFn:   clockOpts.NowFn(),
		queue:   make(chan packet, size),
		metrics: newPacketQueueMetrics(scope, samplingRate),
	}
}

func (q *packetQueue) Len() int { return len(q.queue) }

func (q *packetQueue) Enqueue(p packet) error {
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

func (q *packetQueue) Dequeue() (packet, error) {
	callStart := q.nowFn()
	p, ok := <-q.queue
	if !ok {
		q.metrics.dequeue.ReportError(q.nowFn().Sub(callStart))
		return packet{}, errQueueClosed
	}
	q.metrics.dequeue.ReportSuccess(q.nowFn().Sub(callStart))
	return p, nil
}

func (q *packetQueue) Close() {
	if !atomic.CompareAndSwapInt32(&q.closed, 0, 1) {
		return
	}
	close(q.queue)
}
