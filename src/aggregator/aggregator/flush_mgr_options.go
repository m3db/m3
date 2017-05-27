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

package aggregator

import (
	"math"
	"runtime"
	"time"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultCheckEvery    = time.Second
	defaultJitterEnabled = true
)

var (
	defaultWorkerPoolSize = int(math.Max(float64(runtime.NumCPU()/8), 1.0))
)

// FlushManagerOptions provide a set of options for the flush manager.
type FlushManagerOptions interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) FlushManagerOptions

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) FlushManagerOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetCheckEvery sets the check period.
	SetCheckEvery(value time.Duration) FlushManagerOptions

	// CheckEvery returns the check period.
	CheckEvery() time.Duration

	// SetJitterEnabled sets whether jittering is enabled.
	SetJitterEnabled(value bool) FlushManagerOptions

	// JitterEnabled returns whether jittering is enabled.
	JitterEnabled() bool

	// SetWorkerPoolSize sets the worker pool size.
	SetWorkerPoolSize(value int) FlushManagerOptions

	// WorkerPoolSize returns worker pool size.
	WorkerPoolSize() int
}

type flushManagerOptions struct {
	clockOpts      clock.Options
	instrumentOpts instrument.Options
	checkEvery     time.Duration
	jitterEnabled  bool
	workerPoolSize int
}

// NewFlushManagerOptions create a new set of flush manager options.
func NewFlushManagerOptions() FlushManagerOptions {
	return &flushManagerOptions{
		clockOpts:      clock.NewOptions(),
		instrumentOpts: instrument.NewOptions(),
		checkEvery:     defaultCheckEvery,
		jitterEnabled:  defaultJitterEnabled,
		workerPoolSize: defaultWorkerPoolSize,
	}
}

func (o *flushManagerOptions) SetClockOptions(value clock.Options) FlushManagerOptions {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *flushManagerOptions) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *flushManagerOptions) SetInstrumentOptions(value instrument.Options) FlushManagerOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *flushManagerOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *flushManagerOptions) SetCheckEvery(value time.Duration) FlushManagerOptions {
	opts := *o
	opts.checkEvery = value
	return &opts
}

func (o *flushManagerOptions) CheckEvery() time.Duration {
	return o.checkEvery
}

func (o *flushManagerOptions) SetJitterEnabled(value bool) FlushManagerOptions {
	opts := *o
	opts.jitterEnabled = value
	return &opts
}

func (o *flushManagerOptions) JitterEnabled() bool {
	return o.jitterEnabled
}

func (o *flushManagerOptions) SetWorkerPoolSize(value int) FlushManagerOptions {
	opts := *o
	opts.workerPoolSize = value
	return &opts
}

func (o *flushManagerOptions) WorkerPoolSize() int {
	return o.workerPoolSize
}
