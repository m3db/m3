// Copyright (c) 2017 Uber Technologies, Inc.
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
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/sync"

	"github.com/uber-go/tally"
)

var (
	errFlushManagerClosed = errors.New("flush manager is closed")
)

// PeriodicFlusher flushes periodically.
type PeriodicFlusher interface {
	// FlushInterval returns the periodic flush interval.
	FlushInterval() time.Duration

	// Flush performs a flush.
	Flush()
}

// FlushManager manages and coordinates flushing activities across many
// periodic flushers with different flush intervals with controlled concurrency
// for flushes to minimize spikes in CPU load and reduce p99 flush latencies.
type FlushManager interface {
	// Register registers a metric list with the flush manager.
	Register(flusher PeriodicFlusher) error

	// Close closes the flush manager.
	Close()
}

type flushManagerMetrics struct {
	queueSize tally.Gauge
}

func newFlushManagerMetrics(scope tally.Scope) flushManagerMetrics {
	return flushManagerMetrics{
		queueSize: scope.Gauge("queue-size"),
	}
}

type flushManager struct {
	sync.Mutex

	nowFn         clock.NowFn
	checkEvery    time.Duration
	jitterEnabled bool
	workers       xsync.WorkerPool
	scope         tally.Scope

	closed     bool
	rand       *rand.Rand
	buckets    []*flushBucket
	flushTimes flushMetadataHeap
	sleepFn    sleepFn
	wgFlush    sync.WaitGroup
	metrics    flushManagerMetrics
}

// NewFlushManager creates a new flush manager.
func NewFlushManager(opts FlushManagerOptions) FlushManager {
	if opts == nil {
		opts = NewFlushManagerOptions()
	}
	nowFn := opts.ClockOptions().NowFn()
	scope := opts.InstrumentOptions().MetricsScope()
	rand := rand.New(rand.NewSource(nowFn().UnixNano()))
	workers := xsync.NewWorkerPool(opts.WorkerPoolSize())
	workers.Init()

	mgr := &flushManager{
		nowFn:         nowFn,
		checkEvery:    opts.CheckEvery(),
		jitterEnabled: opts.JitterEnabled(),
		workers:       workers,
		scope:         scope,
		rand:          rand,
		sleepFn:       time.Sleep,
		metrics:       newFlushManagerMetrics(scope),
	}
	if mgr.checkEvery > 0 {
		mgr.wgFlush.Add(1)
		go mgr.flush()
	}
	return mgr
}

func (mgr *flushManager) Register(flusher PeriodicFlusher) error {
	mgr.Lock()
	bucket, err := mgr.bucketForWithLock(flusher)
	if err == nil {
		bucket.Add(flusher)
		mgr.Unlock()
		return nil
	}
	mgr.Unlock()
	return err
}

func (mgr *flushManager) bucketForWithLock(l PeriodicFlusher) (*flushBucket, error) {
	if mgr.closed {
		return nil, errFlushManagerClosed
	}
	flushInterval := l.FlushInterval()
	for _, bucket := range mgr.buckets {
		if bucket.interval == flushInterval {
			return bucket, nil
		}
	}
	bucketScope := mgr.scope.SubScope("bucket").Tagged(map[string]string{
		"interval": flushInterval.String(),
	})
	bucket := newBucket(flushInterval, bucketScope)
	mgr.buckets = append(mgr.buckets, bucket)

	// NB(xichen): this is a new bucket so we need to update the flush times
	// heap for the flush goroutine to pick it up.
	nowNanos := mgr.nowNanos()
	nextFlushNanos := nowNanos + int64(flushInterval)
	if mgr.jitterEnabled {
		jitterNanos := int64(mgr.rand.Int63n(int64(flushInterval)))
		nextFlushNanos = nowNanos + jitterNanos
	}
	newFlushMetadata := flushMetadata{
		timeNanos: nextFlushNanos,
		bucketIdx: len(mgr.buckets) - 1,
	}
	mgr.flushTimes.Push(newFlushMetadata)

	return bucket, nil
}

func (mgr *flushManager) Close() {
	mgr.Lock()
	if mgr.closed {
		mgr.Unlock()
		return
	}
	mgr.closed = true
	mgr.Unlock()

	mgr.wgFlush.Wait()
}

// NB(xichen): apparently timer.Reset() is more difficult to use than I originally
// anticipated. For now I'm simply waking up every second to check for updates. Maybe
// when I have more time I'll spend a few hours to get timer.Reset() right and switch
// to a timer.Start + timer.Stop + timer.Reset + timer.After based approach.
func (mgr *flushManager) flush() {
	defer mgr.wgFlush.Done()

	for {
		var (
			shouldFlush = false
			duration    tally.Timer
			flushers    []PeriodicFlusher
			waitFor     = mgr.checkEvery
		)
		mgr.Lock()
		if mgr.closed {
			mgr.Unlock()
			break
		}
		numFlushTimes := mgr.flushTimes.Len()
		mgr.metrics.queueSize.Update(float64(numFlushTimes))
		if numFlushTimes > 0 {
			earliestFlush := mgr.flushTimes.Min()
			if nowNanos := mgr.nowNanos(); nowNanos >= earliestFlush.timeNanos {
				// NB(xichen): make a shallow copy of the flushers inside the lock
				// and use the snapshot for flushing below.
				shouldFlush = true
				bucketIdx := earliestFlush.bucketIdx
				duration = mgr.buckets[bucketIdx].duration
				flushers = mgr.buckets[bucketIdx].flushers
				nextFlushMetadata := flushMetadata{
					timeNanos: earliestFlush.timeNanos + int64(mgr.buckets[bucketIdx].interval),
					bucketIdx: bucketIdx,
				}
				mgr.flushTimes.Pop()
				mgr.flushTimes.Push(nextFlushMetadata)
			} else {
				// NB(xichen): don't oversleep if the next flush is about to happen.
				timeToNextFlush := time.Duration(earliestFlush.timeNanos - nowNanos)
				if timeToNextFlush < waitFor {
					waitFor = timeToNextFlush
				}
			}
		}
		mgr.Unlock()

		if !shouldFlush {
			mgr.sleepFn(waitFor)
		} else {
			var wgWorkers sync.WaitGroup
			start := mgr.nowFn()
			for _, flusher := range flushers {
				flusher := flusher
				wgWorkers.Add(1)
				mgr.workers.Go(func() {
					flusher.Flush()
					wgWorkers.Done()
				})
			}
			wgWorkers.Wait()
			duration.Record(mgr.nowFn().Sub(start))
		}
	}

	// Perform a final flush across all flushers before returning.
	var wgWorkers sync.WaitGroup
	for _, bucket := range mgr.buckets {
		for _, flusher := range bucket.flushers {
			flusher := flusher
			wgWorkers.Add(1)
			mgr.workers.Go(func() {
				flusher.Flush()
				wgWorkers.Done()
			})
		}
	}
	wgWorkers.Wait()
}

func (mgr *flushManager) nowNanos() int64 { return mgr.nowFn().UnixNano() }

// flushBucket contains all the registered lists for a given flush interval.
type flushBucket struct {
	interval time.Duration
	flushers []PeriodicFlusher
	duration tally.Timer
}

func newBucket(interval time.Duration, scope tally.Scope) *flushBucket {
	return &flushBucket{
		interval: interval,
		duration: scope.Timer("duration"),
	}
}

func (b *flushBucket) Add(flusher PeriodicFlusher) {
	b.flushers = append(b.flushers, flusher)
}

// flushMetadata contains metadata information for a flush.
type flushMetadata struct {
	timeNanos int64
	bucketIdx int
}

// flushMetadataHeap is a min heap for flush metadata where the metadata with the
// earliest flush time will be at the top of the heap. Unlike the generic heap in
// the container/heap package, pushing data to or popping data off of the heap doesn't
// require conversion between flush metadata and interface{}, therefore avoiding the
// memory and GC overhead due to the additional allocations.
type flushMetadataHeap []flushMetadata

// Len returns the number of values in the heap.
func (h flushMetadataHeap) Len() int { return len(h) }

// Min returns the metadata with the earliest flush time from the heap.
func (h flushMetadataHeap) Min() flushMetadata { return h[0] }

// Push pushes a flush metadata onto the heap.
func (h *flushMetadataHeap) Push(value flushMetadata) {
	*h = append(*h, value)
	h.shiftUp(h.Len() - 1)
}

// Pop pops the metadata with the earliest flush time from the heap.
func (h *flushMetadataHeap) Pop() flushMetadata {
	var (
		old = *h
		n   = old.Len()
		val = old[0]
	)

	old[0], old[n-1] = old[n-1], old[0]
	h.heapify(0, n-1)
	*h = (*h)[0 : n-1]
	return val
}

func (h flushMetadataHeap) shiftUp(i int) {
	for {
		parent := (i - 1) / 2
		if parent == i || h[parent].timeNanos <= h[i].timeNanos {
			break
		}
		h[parent], h[i] = h[i], h[parent]
		i = parent
	}
}

func (h flushMetadataHeap) heapify(i, n int) {
	for {
		left := i*2 + 1
		right := left + 1
		smallest := i
		if left < n && h[left].timeNanos < h[smallest].timeNanos {
			smallest = left
		}
		if right < n && h[right].timeNanos < h[smallest].timeNanos {
			smallest = right
		}
		if smallest == i {
			return
		}
		h[i], h[smallest] = h[smallest], h[i]
		i = smallest
	}
}
