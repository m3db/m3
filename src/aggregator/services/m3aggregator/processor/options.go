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
	"math"
	"runtime"

	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultQueueSize = 4096
)

var (
	defaultNumWorkers = int(math.Max(float64(runtime.NumCPU()/4), 1.0))
)

// Options provides a set of processor options
type Options interface {
	// SetInstrumentOptions sets the instrument options
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options
	InstrumentOptions() instrument.Options

	// SetIteratorOptions sets the iterator options
	SetIteratorOptions(value msgpack.AggregatedIteratorOptions) Options

	// IteratorOptions returns the iterator options
	IteratorOptions() msgpack.AggregatedIteratorOptions

	// SetQueueSize sets the processor queue size
	SetQueueSize(value int) Options

	// QueueSize returns the processor queue size
	QueueSize() int

	// SetNumWorkers sets the number of workers
	SetNumWorkers(value int) Options

	// NumWorkers returns the number of workers
	NumWorkers() int

	// SetMetricWithPolicyFn sets the metric with policy function
	SetMetricWithPolicyFn(value MetricWithPolicyFn) Options

	// MetricWithPolicyFn returns the metric with policy function
	MetricWithPolicyFn() MetricWithPolicyFn
}

type options struct {
	instrumentOpts     instrument.Options
	iterOpts           msgpack.AggregatedIteratorOptions
	queueSize          int
	numWorkers         int
	metricWithPolicyFn MetricWithPolicyFn
}

// NewOptions creates a new set of processor options
func NewOptions() Options {
	return &options{
		instrumentOpts: instrument.NewOptions(),
		iterOpts:       msgpack.NewAggregatedIteratorOptions(),
		queueSize:      defaultQueueSize,
		numWorkers:     defaultNumWorkers,
	}
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetIteratorOptions(value msgpack.AggregatedIteratorOptions) Options {
	opts := *o
	opts.iterOpts = value
	return &opts
}

func (o *options) IteratorOptions() msgpack.AggregatedIteratorOptions {
	return o.iterOpts
}

func (o *options) SetQueueSize(value int) Options {
	opts := *o
	opts.queueSize = value
	return &opts
}

func (o *options) QueueSize() int {
	return o.queueSize
}

func (o *options) SetNumWorkers(value int) Options {
	opts := *o
	opts.numWorkers = value
	return &opts
}

func (o *options) NumWorkers() int {
	return o.numWorkers
}

func (o *options) SetMetricWithPolicyFn(value MetricWithPolicyFn) Options {
	opts := *o
	opts.metricWithPolicyFn = value
	return &opts
}

func (o *options) MetricWithPolicyFn() MetricWithPolicyFn {
	return o.metricWithPolicyFn
}
