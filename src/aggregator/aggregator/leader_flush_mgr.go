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
	"fmt"
	"math/rand"
	"sync"
	"time"

	schema "github.com/m3db/m3aggregator/generated/proto/flush"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/sync"
	"github.com/m3db/m3x/watch"

	"github.com/uber-go/tally"
)

type leaderFlushManagerMetrics struct {
	queueSize         tally.Gauge
	flushTimesPersist instrument.MethodMetrics
}

func newLeaderFlushManagerMetrics(scope tally.Scope) leaderFlushManagerMetrics {
	return leaderFlushManagerMetrics{
		queueSize:         scope.Gauge("queue-size"),
		flushTimesPersist: instrument.NewMethodMetrics(scope, "flush-times-persist", 1.0),
	}
}

type randFn func(int64) int64

type leaderFlushManager struct {
	sync.RWMutex

	nowFn                    clock.NowFn
	checkEvery               time.Duration
	jitterEnabled            bool
	maxJitterFn              FlushJitterFn
	workers                  xsync.WorkerPool
	flushTimesKeyFmt         string
	flushTimesStore          kv.Store
	flushTimesPersistEvery   time.Duration
	flushTimesPersistRetrier xretry.Retrier
	logger                   xlog.Logger
	scope                    tally.Scope

	doneCh              <-chan struct{}
	rand                *rand.Rand
	randFn              randFn
	flushTimes          flushMetadataHeap
	flushTimesKey       string
	lastPersistAtNanos  int64
	flushedSincePersist bool
	persistWatchable    xwatch.Watchable
	flushTask           *leaderFlushTask
	metrics             leaderFlushManagerMetrics
}

func newLeaderFlushManager(
	doneCh <-chan struct{},
	opts FlushManagerOptions,
) roleBasedFlushManager {
	nowFn := opts.ClockOptions().NowFn()
	rand := rand.New(rand.NewSource(nowFn().UnixNano()))
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()
	mgr := &leaderFlushManager{
		nowFn:                    nowFn,
		checkEvery:               opts.CheckEvery(),
		jitterEnabled:            opts.JitterEnabled(),
		maxJitterFn:              opts.MaxJitterFn(),
		workers:                  opts.WorkerPool(),
		flushTimesKeyFmt:         opts.FlushTimesKeyFmt(),
		flushTimesStore:          opts.FlushTimesStore(),
		flushTimesPersistEvery:   opts.FlushTimesPersistEvery(),
		flushTimesPersistRetrier: opts.FlushTimesPersistRetrier(),
		logger:             instrumentOpts.Logger(),
		scope:              scope,
		doneCh:             doneCh,
		rand:               rand,
		randFn:             rand.Int63n,
		lastPersistAtNanos: nowFn().UnixNano(),
		persistWatchable:   xwatch.NewWatchable(),
		metrics:            newLeaderFlushManagerMetrics(scope),
	}
	mgr.flushTask = &leaderFlushTask{mgr: mgr}
	return mgr
}

func (mgr *leaderFlushManager) Open(shardSetID uint32) error {
	mgr.Lock()
	mgr.flushTimesKey = fmt.Sprintf(mgr.flushTimesKeyFmt, shardSetID)
	mgr.Unlock()

	_, flushTimesWatch, err := mgr.persistWatchable.Watch()
	if err != nil {
		return err
	}
	go mgr.persistFlushTimes(flushTimesWatch)

	return nil
}

// Init initializes the leader flush manager by enqueuing all
// the flushers in the buckets.
func (mgr *leaderFlushManager) Init(buckets []*flushBucket) {
	mgr.Lock()
	mgr.flushTimes.Reset()
	for bucketIdx, bucket := range buckets {
		mgr.enqueueBucketWithLock(bucketIdx, bucket)
	}
	mgr.Unlock()
}

func (mgr *leaderFlushManager) Prepare(buckets []*flushBucket) (flushTask, time.Duration) {
	var (
		shouldFlush = false
		duration    tally.Timer
		flushers    []PeriodicFlusher
		waitFor     = mgr.checkEvery
	)
	mgr.Lock()
	defer mgr.Unlock()

	numFlushTimes := mgr.flushTimes.Len()
	mgr.metrics.queueSize.Update(float64(numFlushTimes))
	nowNanos := mgr.nowNanos()
	if numFlushTimes > 0 {
		earliestFlush := mgr.flushTimes.Min()
		if nowNanos >= earliestFlush.timeNanos {
			// NB(xichen): make a shallow copy of the flushers inside the lock
			// and use the snapshot for flushing below.
			shouldFlush = true
			waitFor = 0
			bucketIdx := earliestFlush.bucketIdx
			duration = buckets[bucketIdx].duration
			flushers = buckets[bucketIdx].flushers
			nextFlushMetadata := flushMetadata{
				timeNanos: earliestFlush.timeNanos + int64(buckets[bucketIdx].interval),
				bucketIdx: bucketIdx,
			}
			mgr.flushTimes.Pop()
			mgr.flushTimes.Push(nextFlushMetadata)
			mgr.flushedSincePersist = true
		} else {
			// NB(xichen): don't oversleep if the next flush is about to happen.
			timeToNextFlush := time.Duration(earliestFlush.timeNanos - nowNanos)
			if timeToNextFlush < waitFor {
				waitFor = timeToNextFlush
			}
		}
	}

	var (
		flushTimes               schema.ShardSetFlushTimes
		durationSinceLastPersist = time.Duration(nowNanos - mgr.lastPersistAtNanos)
	)
	if mgr.flushedSincePersist && durationSinceLastPersist >= mgr.flushTimesPersistEvery {
		mgr.lastPersistAtNanos = nowNanos
		mgr.flushedSincePersist = false
		flushTimes = mgr.prepareFlushTimesWithLock(buckets)
		mgr.persistWatchable.Update(&flushTimes)
	}

	if !shouldFlush {
		return nil, waitFor
	}
	mgr.flushTask.duration = duration
	mgr.flushTask.flushers = flushers
	return mgr.flushTask, waitFor
}

// NB(xichen): if the current instance is a leader, we need to update the flush
// times heap for the flush goroutine to pick it up.
func (mgr *leaderFlushManager) OnBucketAdded(
	bucketIdx int,
	bucket *flushBucket,
) {
	mgr.Lock()
	mgr.enqueueBucketWithLock(bucketIdx, bucket)
	mgr.Unlock()
}

// NB(xichen): leader flush manager can always lead.
func (mgr *leaderFlushManager) CanLead() bool { return true }

func (mgr *leaderFlushManager) enqueueBucketWithLock(
	bucketIdx int,
	bucket *flushBucket,
) {
	flushInterval := bucket.interval
	nextFlushNanos := mgr.computeNextFlushNanos(flushInterval)
	newFlushMetadata := flushMetadata{
		timeNanos: nextFlushNanos,
		bucketIdx: bucketIdx,
	}
	mgr.flushTimes.Push(newFlushMetadata)
}

func (mgr *leaderFlushManager) computeNextFlushNanos(flushInterval time.Duration) int64 {
	now := mgr.nowFn()
	nextFlushNanos := now.UnixNano() + int64(flushInterval)
	if mgr.jitterEnabled {
		alignedNow := now.Truncate(flushInterval)
		if alignedNow.Before(now) {
			alignedNow = alignedNow.Add(flushInterval)
		}
		maxJitter := mgr.maxJitterFn(flushInterval)
		jitterNanos := mgr.randFn(int64(maxJitter))
		nextFlushNanos = alignedNow.UnixNano() + jitterNanos
	}
	return nextFlushNanos
}

func (mgr *leaderFlushManager) prepareFlushTimesWithLock(
	buckets []*flushBucket,
) schema.ShardSetFlushTimes {
	proto := schema.ShardSetFlushTimes{
		ByShard: make(map[uint32]*schema.ShardFlushTimes, defaultInitialFlushTimesCapacity),
	}
	for _, bucket := range buckets {
		for _, flusher := range bucket.flushers {
			shard := flusher.Shard()
			shardFlushTimes, exists := proto.ByShard[shard]
			if !exists {
				shardFlushTimes = &schema.ShardFlushTimes{
					ByResolution: make(map[int64]int64, 2),
				}
				proto.ByShard[shard] = shardFlushTimes
			}
			resolution := flusher.Resolution()
			lastFlushedNanos := flusher.LastFlushedNanos()
			shardFlushTimes.ByResolution[int64(resolution)] = lastFlushedNanos
		}
	}
	return proto
}

func (mgr *leaderFlushManager) persistFlushTimes(flushTimesWatch xwatch.Watch) {
	for {
		select {
		case <-mgr.doneCh:
			return
		case <-flushTimesWatch.C():
			flushTimes := flushTimesWatch.Get().(*schema.ShardSetFlushTimes)
			persistStart := mgr.nowFn()
			persistErr := mgr.flushTimesPersistRetrier.Attempt(func() error {
				_, err := mgr.flushTimesStore.Set(mgr.flushTimesKey, flushTimes)
				return err
			})
			mgr.metrics.flushTimesPersist.ReportSuccessOrError(persistErr, mgr.nowFn().Sub(persistStart))
		}
	}
}

func (mgr *leaderFlushManager) nowNanos() int64 { return mgr.nowFn().UnixNano() }

type leaderFlushTask struct {
	mgr      *leaderFlushManager
	duration tally.Timer
	flushers []PeriodicFlusher
}

func (t *leaderFlushTask) Run() {
	mgr := t.mgr

	var wgWorkers sync.WaitGroup
	start := mgr.nowFn()
	for _, flusher := range t.flushers {
		flusher := flusher
		wgWorkers.Add(1)
		mgr.workers.Go(func() {
			flusher.Flush()
			wgWorkers.Done()
		})
	}
	wgWorkers.Wait()
	t.duration.Record(mgr.nowFn().Sub(start))
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

// Reset resets the heap.
func (h *flushMetadataHeap) Reset() { *h = (*h)[:0] }

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
