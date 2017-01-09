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

package processor

import (
	"errors"
	"sync"

	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/sync"
)

var (
	errProcessorQueueIsFull = errors.New("processor queue is full")
	errProcessorIsClosed    = errors.New("processor is closed")
)

// MetricWithPolicyFn handles aggregated metrics with corresponding policy.
// It must be thread-safe.
type MetricWithPolicyFn func(metric aggregated.Metric, policy policy.Policy) error

// AggregatedMetricProcessor processes aggregated metrics
type AggregatedMetricProcessor struct {
	sync.RWMutex

	closed    bool
	queue     chan msgpack.BufferedEncoder
	workers   xsync.WorkerPool
	wgWorkers sync.WaitGroup
	fn        MetricWithPolicyFn
	log       xlog.Logger
	iterOpts  msgpack.AggregatedIteratorOptions
}

// NewAggregatedMetricProcessor creates a new aggregated metric processor
func NewAggregatedMetricProcessor(opts Options) *AggregatedMetricProcessor {
	p := &AggregatedMetricProcessor{
		queue:    make(chan msgpack.BufferedEncoder, opts.QueueSize()),
		fn:       opts.MetricWithPolicyFn(),
		log:      opts.InstrumentOptions().Logger(),
		iterOpts: opts.IteratorOptions(),
	}
	numWorkers := opts.NumWorkers()
	p.wgWorkers.Add(numWorkers)
	p.workers = xsync.NewWorkerPool(numWorkers)
	p.workers.Init()
	for i := 0; i < numWorkers; i++ {
		p.workers.Go(p.drain)
	}
	return p
}

// Add adds a buffered encoder containing encoded metrics with policies
func (p *AggregatedMetricProcessor) Add(encoder msgpack.BufferedEncoder) error {
	p.RLock()
	defer p.RUnlock()

	if p.closed {
		return errProcessorIsClosed
	}
	select {
	case p.queue <- encoder:
		return nil
	default:
		return errProcessorQueueIsFull
	}
}

// Close closes the processor
func (p *AggregatedMetricProcessor) Close() {
	p.Lock()
	if p.closed {
		p.Unlock()
		return
	}
	p.closed = true
	p.Unlock()

	close(p.queue)
	p.wgWorkers.Wait()
}

func (p *AggregatedMetricProcessor) drain() {
	defer p.wgWorkers.Done()

	iter := msgpack.NewAggregatedIterator(nil, p.iterOpts)
	defer iter.Close()

	for {
		encoder, ok := <-p.queue
		if !ok {
			return
		}
		iter.Reset(encoder.Buffer)
		for iter.Next() {
			rawMetric, policy := iter.Value()
			metric, err := rawMetric.Metric()
			if err != nil {
				p.log.WithFields(
					xlog.NewLogField("rawMetric:", rawMetric.Bytes()),
					xlog.NewLogErrField(err),
				).Errorf("raw metric to metric conversion error")
				continue
			}
			if err := p.fn(metric, policy); err != nil {
				p.log.WithFields(
					xlog.NewLogField("metric", metric),
					xlog.NewLogField("policy", policy),
					xlog.NewLogErrField(err),
				).Errorf("handling metric with policy error")
			}
		}
		if err := iter.Err(); err != nil {
			p.log.Errorf("draining iterator error: %v", err)
		}
	}
}
