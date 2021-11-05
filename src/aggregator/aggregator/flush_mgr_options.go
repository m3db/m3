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

	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/sync"
)

const (
	defaultCheckEvery            = time.Second
	defaultJitterEnabled         = true
	defaultMaxBufferSize         = 5 * time.Minute
	defaultForcedFlushWindowSize = 10 * time.Second
)

var defaultWorkerPoolSize = int(math.Max(float64(runtime.GOMAXPROCS(0)/8), 1.0))

// FlushJitterFn determines the jitter based on the flush interval.
type FlushJitterFn func(flushInterval time.Duration) time.Duration

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

	// SetMaxJitterFn sets the max flush jittering function.
	SetMaxJitterFn(value FlushJitterFn) FlushManagerOptions

	// MaxJitterFn returns the max flush jittering function.
	MaxJitterFn() FlushJitterFn

	// SetWorkerPool sets the worker pool.
	SetWorkerPool(value sync.WorkerPool) FlushManagerOptions

	// WorkerPool returns the worker pool.
	WorkerPool() sync.WorkerPool

	// SetPlacementManager sets the placement manager.
	SetPlacementManager(value PlacementManager) FlushManagerOptions

	// PlacementManager returns the placement manager.
	PlacementManager() PlacementManager

	// SetElectionManager sets the election manager.
	SetElectionManager(value ElectionManager) FlushManagerOptions

	// ElectionManager returns the election manager.
	ElectionManager() ElectionManager

	// SetFlushTimesManager sets the flush times manager.
	SetFlushTimesManager(value FlushTimesManager) FlushManagerOptions

	// FlushTimesManager returns the flush times manager.
	FlushTimesManager() FlushTimesManager

	// SetFlushTimesPersistEvery sets how frequently the flush times are stored in kv.
	SetFlushTimesPersistEvery(value time.Duration) FlushManagerOptions

	// FlushTimesPersistEvery returns how frequently the flush times are stored in kv.
	FlushTimesPersistEvery() time.Duration

	// SetMaxBufferSize sets the maximum duration data are buffered for without getting
	// flushed or discarded to handle transient KV issues or for backing out of active
	// topology changes.
	SetMaxBufferSize(value time.Duration) FlushManagerOptions

	// MaxBufferSize sets the maximum duration data are buffered for without getting
	// flushed or discarded to handle transient KV issues or for backing out of active
	// topology changes.
	MaxBufferSize() time.Duration

	// SetForcedFlushWindowSize sets the window size for a forced flush.
	SetForcedFlushWindowSize(value time.Duration) FlushManagerOptions

	// ForcedFlushWindowSize returns the window size for a forced flush.
	ForcedFlushWindowSize() time.Duration

	// SetBufferForPastTimedMetric sets the size of the buffer for timed metrics in the past.
	SetBufferForPastTimedMetric(value time.Duration) FlushManagerOptions

	// BufferForPastTimedMetric returns the size of the buffer for timed metrics in the past.
	BufferForPastTimedMetric() time.Duration
}

type flushManagerOptions struct {
	clockOpts              clock.Options
	instrumentOpts         instrument.Options
	checkEvery             time.Duration
	jitterEnabled          bool
	maxJitterFn            FlushJitterFn
	workerPool             sync.WorkerPool
	placementManager       PlacementManager
	electionManager        ElectionManager
	flushTimesManager      FlushTimesManager
	flushTimesPersistEvery time.Duration
	maxBufferSize          time.Duration
	forcedFlushWindowSize  time.Duration

	bufferForPastTimedMetric time.Duration
}

// NewFlushManagerOptions create a new set of flush manager options.
func NewFlushManagerOptions() FlushManagerOptions {
	workerPool := sync.NewWorkerPool(defaultWorkerPoolSize)
	workerPool.Init()
	return &flushManagerOptions{
		clockOpts:             clock.NewOptions(),
		instrumentOpts:        instrument.NewOptions(),
		checkEvery:            defaultCheckEvery,
		jitterEnabled:         defaultJitterEnabled,
		workerPool:            workerPool,
		maxBufferSize:         defaultMaxBufferSize,
		forcedFlushWindowSize: defaultForcedFlushWindowSize,

		bufferForPastTimedMetric: defaultTimedMetricBuffer,
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

func (o *flushManagerOptions) SetMaxJitterFn(value FlushJitterFn) FlushManagerOptions {
	opts := *o
	opts.maxJitterFn = value
	return &opts
}

func (o *flushManagerOptions) MaxJitterFn() FlushJitterFn {
	return o.maxJitterFn
}

func (o *flushManagerOptions) SetWorkerPool(value sync.WorkerPool) FlushManagerOptions {
	opts := *o
	opts.workerPool = value
	return &opts
}

func (o *flushManagerOptions) WorkerPool() sync.WorkerPool {
	return o.workerPool
}

func (o *flushManagerOptions) SetPlacementManager(value PlacementManager) FlushManagerOptions {
	opts := *o
	opts.placementManager = value
	return &opts
}

func (o *flushManagerOptions) PlacementManager() PlacementManager {
	return o.placementManager
}

func (o *flushManagerOptions) SetElectionManager(value ElectionManager) FlushManagerOptions {
	opts := *o
	opts.electionManager = value
	return &opts
}

func (o *flushManagerOptions) ElectionManager() ElectionManager {
	return o.electionManager
}

func (o *flushManagerOptions) SetFlushTimesManager(value FlushTimesManager) FlushManagerOptions {
	opts := *o
	opts.flushTimesManager = value
	return &opts
}

func (o *flushManagerOptions) FlushTimesManager() FlushTimesManager {
	return o.flushTimesManager
}

func (o *flushManagerOptions) SetFlushTimesPersistEvery(value time.Duration) FlushManagerOptions {
	opts := *o
	opts.flushTimesPersistEvery = value
	return &opts
}

func (o *flushManagerOptions) FlushTimesPersistEvery() time.Duration {
	return o.flushTimesPersistEvery
}

func (o *flushManagerOptions) SetMaxBufferSize(value time.Duration) FlushManagerOptions {
	opts := *o
	opts.maxBufferSize = value
	return &opts
}

func (o *flushManagerOptions) MaxBufferSize() time.Duration {
	return o.maxBufferSize
}

func (o *flushManagerOptions) SetForcedFlushWindowSize(value time.Duration) FlushManagerOptions {
	opts := *o
	opts.forcedFlushWindowSize = value
	return &opts
}

func (o *flushManagerOptions) ForcedFlushWindowSize() time.Duration {
	return o.forcedFlushWindowSize
}

func (o *flushManagerOptions) SetBufferForPastTimedMetric(value time.Duration) FlushManagerOptions {
	opts := *o
	opts.bufferForPastTimedMetric = value
	return &opts
}

func (o *flushManagerOptions) BufferForPastTimedMetric() time.Duration {
	return o.bufferForPastTimedMetric
}
