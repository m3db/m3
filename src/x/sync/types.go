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

package sync

import (
	gocontext "context"
	"time"

	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/instrument"
)

// Work is a unit of item to be worked on.
type Work func()

// PooledWorkerPool provides a pool for goroutines, but unlike WorkerPool,
// the actual goroutines themselves are re-used. This can be useful from a
// performance perspective in scenarios where the allocation and growth of
// the new goroutine and its stack is a bottleneck. Specifically, if the
// work function being performed has a very deep call-stack, calls to
// runtime.morestack can dominate the workload. Re-using existing goroutines
// allows the stack to be grown once, and then re-used for many invocations.
//
// In order to prevent abnormally large goroutine stacks from persisting over
// the life-cycle of an application, the PooledWorkerPool will randomly kill
// existing goroutines and spawn a new one.
//
// The PooledWorkerPool also implements sharding of its underlying worker channels
// to prevent excessive lock contention.
type PooledWorkerPool interface {
	// Init initializes the pool.
	Init()

	// Go assign the Work to be executed by a Goroutine. Whether or not
	// it waits for an existing Goroutine to become available or not
	// is determined by the GrowOnDemand() option. If GrowOnDemand is not
	// set then the call to Go() will block until a goroutine is available.
	// If GrowOnDemand() is set then it will expand the pool of goroutines to
	// accommodate the work. The newly allocated goroutine will temporarily
	// participate in the pool in an effort to amortize its allocation cost, but
	// will eventually be killed. This allows the pool to dynamically respond to
	// workloads without causing excessive memory pressure. The pool will grow in
	// size when the workload exceeds its capacity and shrink back down to its
	// original size if/when the burst subsides.
	Go(work Work)

	// GoWithTimeout waits up to the given timeout for a worker to become
	// available, returning true if a worker becomes available, or false
	// otherwise.
	GoWithTimeout(work Work, timeout time.Duration) bool

	// GoWithContext waits until a worker is available or the provided ctx is
	// canceled.
	GoWithContext(ctx gocontext.Context, work Work) bool

	// FastContextCheck returns a wrapper worker pool that only checks the context deadline every batchSize calls.
	// This is useful for tight looping code that wants to amortize the cost of the ctx deadline check over batchSize
	// iterations.
	// This should only be used for code that can guarantee the wait time for a worker is low since if the ctx is not
	// checked the calling goroutine blocks waiting for a worker.
	FastContextCheck(batchSize int) PooledWorkerPool
}

// NewPooledWorkerOptions is a set of new instrument worker pool options.
type NewPooledWorkerOptions struct {
	InstrumentOptions instrument.Options
}

// NewPooledWorkerFn returns a pooled worker pool that Init must be called on.
type NewPooledWorkerFn func(opts NewPooledWorkerOptions) (PooledWorkerPool, error)

// WorkerPool provides a pool for goroutines.
type WorkerPool interface {
	// Init initializes the pool.
	Init()

	// Go waits until the next worker becomes available and executes it.
	Go(work Work)

	// GoInstrument instruments Go with timing information.
	GoInstrument(work Work) ScheduleResult

	// GoIfAvailable performs the work inside a worker if one is available and
	// returns true, or false otherwise.
	GoIfAvailable(work Work) bool

	// GoWithTimeout waits up to the given timeout for a worker to become
	// available, returning true if a worker becomes available, or false
	// otherwise.
	GoWithTimeout(work Work, timeout time.Duration) bool

	// GoWithTimeoutInstrument instruments GoWithTimeout with timing information.
	GoWithTimeoutInstrument(work Work, timeout time.Duration) ScheduleResult

	// GoWithContext waits until a worker is available or the provided ctx is canceled.
	GoWithContext(ctx context.Context, work Work) ScheduleResult

	// FastContextCheck returns a wrapper worker pool that only checks the context deadline every batchSize calls.
	// This is useful for tight looping code that wants to amortize the cost of the ctx deadline check over batchSize
	// iterations.
	// This should only be used for code that can guarantee the wait time for a worker is low since if the ctx is not
	// checked the calling goroutine blocks waiting for a worker.
	FastContextCheck(batchSize int) WorkerPool
}

// ScheduleResult is the result of scheduling a goroutine in the worker pool.
type ScheduleResult struct {
	// Available is true if the goroutine was scheduled in the worker pool. False if the request timed out before a
	// worker became available.
	Available bool
	// WaitTime is how long the goroutine had to wait before receiving a worker from the pool or timing out.
	WaitTime time.Duration
}

// PooledWorkerPoolOptions is the options for a PooledWorkerPool.
type PooledWorkerPoolOptions interface {
	// SetGrowOnDemand sets whether the GrowOnDemand feature is enabled.
	SetGrowOnDemand(value bool) PooledWorkerPoolOptions

	// GrowOnDemand returns whether the GrowOnDemand feature is enabled.
	GrowOnDemand() bool

	// SetNumShards sets the number of worker channel shards.
	SetNumShards(value int64) PooledWorkerPoolOptions

	// NumShards returns the number of worker channel shards.
	NumShards() int64

	// SetKillWorkerProbability sets the probability to kill a worker.
	SetKillWorkerProbability(value float64) PooledWorkerPoolOptions

	// KillWorkerProbability returns the probability to kill a worker.
	KillWorkerProbability() float64

	// SetNowFn sets the now function.
	SetNowFn(value NowFn) PooledWorkerPoolOptions

	// NowFn returns the now function.
	NowFn() NowFn

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) PooledWorkerPoolOptions

	// InstrumentOptions returns the now function.
	InstrumentOptions() instrument.Options
}
