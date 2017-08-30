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

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/sync"
)

const (
	defaultCheckEvery    = time.Second
	defaultJitterEnabled = true
)

var (
	defaultWorkerPoolSize         = int(math.Max(float64(runtime.NumCPU()/8), 1.0))
	defaultFlushTimesKeyFormat    = "/shardset/%d/flush"
	defaultFlushTimesPersistEvery = 10 * time.Second
	defaultMaxNoFlushDuration     = 15 * time.Minute
	defaultForcedFlushWindowSize  = 10 * time.Second
)

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
	SetWorkerPool(value xsync.WorkerPool) FlushManagerOptions

	// WorkerPool returns the worker pool.
	WorkerPool() xsync.WorkerPool

	// SetElectionManager sets the election manager.
	SetElectionManager(value ElectionManager) FlushManagerOptions

	// ElectionManager returns the election manager.
	ElectionManager() ElectionManager

	// SetFlushTimesKeyFmt sets the flush times key format.
	SetFlushTimesKeyFmt(value string) FlushManagerOptions

	// FlushTimesKeyFmt returns the flush times key format.
	FlushTimesKeyFmt() string

	// SetFlushTimesStore sets the flush times store.
	SetFlushTimesStore(value kv.Store) FlushManagerOptions

	// FlushTimesStore returns the flush times store.
	FlushTimesStore() kv.Store

	// SetFlushTimesPersistEvery sets how frequently the flush times are stored in kv.
	SetFlushTimesPersistEvery(value time.Duration) FlushManagerOptions

	// FlushTimesPersistEvery returns how frequently the flush times are stored in kv.
	FlushTimesPersistEvery() time.Duration

	// SetFlushTimesPersistRetrier sets the retrier for persisting flush times.
	SetFlushTimesPersistRetrier(value xretry.Retrier) FlushManagerOptions

	// FlushTimesPersistRetrier returns the retrier for persisting flush times.
	FlushTimesPersistRetrier() xretry.Retrier

	// SetMaxNoFlushDuration sets the maximum duration with no flushes.
	SetMaxNoFlushDuration(value time.Duration) FlushManagerOptions

	// MaxNoFlushDuration returns the maximum duration with no flushes.
	MaxNoFlushDuration() time.Duration

	// SetForcedFlushWindowSize sets the window size for a forced flush.
	SetForcedFlushWindowSize(value time.Duration) FlushManagerOptions

	// ForcedFlushWindowSize returns the window size for a forced flush.
	ForcedFlushWindowSize() time.Duration

	// SetInstanceID sets the instance id.
	SetInstanceID(value string) FlushManagerOptions

	// InstanceID returns the instance id.
	InstanceID() string

	// SetStagedPlacementWatcher sets the staged placement watcher.
	SetStagedPlacementWatcher(value services.StagedPlacementWatcher) FlushManagerOptions

	// StagedPlacementWatcher returns the staged placement watcher.
	StagedPlacementWatcher() services.StagedPlacementWatcher
}

type flushManagerOptions struct {
	clockOpts                clock.Options
	instrumentOpts           instrument.Options
	checkEvery               time.Duration
	jitterEnabled            bool
	maxJitterFn              FlushJitterFn
	workerPool               xsync.WorkerPool
	electionManager          ElectionManager
	flushTimesKeyFmt         string
	flushTimesStore          kv.Store
	flushTimesPersistEvery   time.Duration
	flushTimesPersistRetrier xretry.Retrier
	maxNoFlushDuration       time.Duration
	forcedFlushWindowSize    time.Duration
	instanceID               string
	placementWatcher         services.StagedPlacementWatcher
}

// NewFlushManagerOptions create a new set of flush manager options.
func NewFlushManagerOptions() FlushManagerOptions {
	workerPool := xsync.NewWorkerPool(defaultWorkerPoolSize)
	workerPool.Init()
	return &flushManagerOptions{
		clockOpts:                clock.NewOptions(),
		instrumentOpts:           instrument.NewOptions(),
		checkEvery:               defaultCheckEvery,
		jitterEnabled:            defaultJitterEnabled,
		workerPool:               workerPool,
		flushTimesKeyFmt:         defaultFlushTimesKeyFormat,
		flushTimesPersistEvery:   defaultFlushTimesPersistEvery,
		flushTimesPersistRetrier: xretry.NewRetrier(xretry.NewOptions()),
		maxNoFlushDuration:       defaultMaxNoFlushDuration,
		instanceID:               defaultInstanceID,
		forcedFlushWindowSize:    defaultForcedFlushWindowSize,
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

func (o *flushManagerOptions) SetWorkerPool(value xsync.WorkerPool) FlushManagerOptions {
	opts := *o
	opts.workerPool = value
	return &opts
}

func (o *flushManagerOptions) WorkerPool() xsync.WorkerPool {
	return o.workerPool
}

func (o *flushManagerOptions) SetElectionManager(value ElectionManager) FlushManagerOptions {
	opts := *o
	opts.electionManager = value
	return &opts
}

func (o *flushManagerOptions) ElectionManager() ElectionManager {
	return o.electionManager
}

func (o *flushManagerOptions) SetFlushTimesKeyFmt(value string) FlushManagerOptions {
	opts := *o
	opts.flushTimesKeyFmt = value
	return &opts
}

func (o *flushManagerOptions) FlushTimesKeyFmt() string {
	return o.flushTimesKeyFmt
}

func (o *flushManagerOptions) SetFlushTimesStore(value kv.Store) FlushManagerOptions {
	opts := *o
	opts.flushTimesStore = value
	return &opts
}

func (o *flushManagerOptions) FlushTimesStore() kv.Store {
	return o.flushTimesStore
}

func (o *flushManagerOptions) SetFlushTimesPersistEvery(value time.Duration) FlushManagerOptions {
	opts := *o
	opts.flushTimesPersistEvery = value
	return &opts
}

func (o *flushManagerOptions) FlushTimesPersistEvery() time.Duration {
	return o.flushTimesPersistEvery
}

func (o *flushManagerOptions) SetFlushTimesPersistRetrier(value xretry.Retrier) FlushManagerOptions {
	opts := *o
	opts.flushTimesPersistRetrier = value
	return &opts
}

func (o *flushManagerOptions) FlushTimesPersistRetrier() xretry.Retrier {
	return o.flushTimesPersistRetrier
}

func (o *flushManagerOptions) SetMaxNoFlushDuration(value time.Duration) FlushManagerOptions {
	opts := *o
	opts.maxNoFlushDuration = value
	return &opts
}

func (o *flushManagerOptions) MaxNoFlushDuration() time.Duration {
	return o.maxNoFlushDuration
}

func (o *flushManagerOptions) SetForcedFlushWindowSize(value time.Duration) FlushManagerOptions {
	opts := *o
	opts.forcedFlushWindowSize = value
	return &opts
}

func (o *flushManagerOptions) ForcedFlushWindowSize() time.Duration {
	return o.forcedFlushWindowSize
}

func (o *flushManagerOptions) SetInstanceID(value string) FlushManagerOptions {
	opts := *o
	opts.instanceID = value
	return &opts
}

func (o *flushManagerOptions) InstanceID() string {
	return o.instanceID
}

func (o *flushManagerOptions) SetStagedPlacementWatcher(value services.StagedPlacementWatcher) FlushManagerOptions {
	opts := *o
	opts.placementWatcher = value
	return &opts
}

func (o *flushManagerOptions) StagedPlacementWatcher() services.StagedPlacementWatcher {
	return o.placementWatcher
}
