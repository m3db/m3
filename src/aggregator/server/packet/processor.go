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
	"sync"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/sync"

	"github.com/uber-go/tally"
)

var (
	errInvalidMetricType = errors.New("invalid metric type")
)

type processorMetrics struct {
	drainErrors   tally.Counter
	processPacket instrument.MethodMetrics
}

func newProcessorMetrics(scope tally.Scope, samplingRate float64) processorMetrics {
	return processorMetrics{
		drainErrors:   scope.Counter("drain-errors"),
		processPacket: instrument.NewMethodMetrics(scope, "process", samplingRate),
	}
}

// Processor is a packet processor
type Processor struct {
	queue      *Queue
	aggregator aggregator.Aggregator
	workers    xsync.WorkerPool
	wgWorkers  sync.WaitGroup
	nowFn      clock.NowFn
	log        xlog.Logger
	metrics    processorMetrics
}

// NewProcessor creates a new processor
func NewProcessor(
	queue *Queue,
	aggregator aggregator.Aggregator,
	numWorkers int,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
) *Processor {
	scope := instrumentOpts.MetricsScope().SubScope("processor")
	samplingRate := instrumentOpts.MetricsSamplingRate()
	p := &Processor{
		queue:      queue,
		aggregator: aggregator,
		nowFn:      clockOpts.NowFn(),
		log:        instrumentOpts.Logger(),
		metrics:    newProcessorMetrics(scope, samplingRate),
	}

	// Start the workers to drain the queue
	p.wgWorkers.Add(numWorkers)
	p.workers = xsync.NewWorkerPool(numWorkers)
	p.workers.Init()
	for i := 0; i < numWorkers; i++ {
		p.workers.Go(p.drain)
	}

	return p
}

// Close closes the processor. It's safe to call close more than once.
// All but the first call are no-ops.
func (p *Processor) Close() {
	// Wait for all workers to finish dequeuing existing
	// packets in the queue
	p.wgWorkers.Wait()
}

func (p *Processor) drain() {
	defer p.wgWorkers.Done()

	for {
		packet, err := p.queue.Dequeue()
		if err == errQueueClosed {
			return
		}
		if err != nil {
			p.log.Errorf("packet drain error: %v", err)
			p.metrics.drainErrors.Inc(1)
			continue
		}
		if err = p.processPacket(packet); err != nil {
			p.log.WithFields(
				xlog.NewLogField("metric", packet.Metric),
				xlog.NewLogField("policies", packet.Policies),
				xlog.NewLogErrField(err),
			).Errorf("process packet error")
		}
	}
}

func (p *Processor) processPacket(packet Packet) error {
	callStart := p.nowFn()
	switch packet.Metric.Type {
	case unaggregated.CounterType, unaggregated.BatchTimerType, unaggregated.GaugeType:
		err := p.aggregator.AddMetricWithPolicies(packet.Metric, packet.Policies)
		p.metrics.processPacket.ReportSuccessOrError(err, p.nowFn().Sub(callStart))
		return err
	default:
		p.metrics.processPacket.ReportError(p.nowFn().Sub(callStart))
		return errInvalidMetricType
	}
}
