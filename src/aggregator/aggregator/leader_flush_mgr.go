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
	"sync"
	"time"

	schema "github.com/m3db/m3/src/aggregator/generated/proto/flush"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/x/clock"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	defaultInitialFlushCapacity = 32
)

type leaderFlusherMetrics struct {
	updateFlushTimes tally.Counter
}

func newLeaderFlusherMetrics(scope tally.Scope) leaderFlusherMetrics {
	return leaderFlusherMetrics{
		updateFlushTimes: scope.Counter("update-flush-times"),
	}
}

type leaderFlushManagerMetrics struct {
	queueSize             tally.Gauge
	getShardsError        tally.Counter
	flushTimesUpdateError tally.Counter
	standard              leaderFlusherMetrics
	forwarded             leaderFlusherMetrics
	timed                 leaderFlusherMetrics
}

func newLeaderFlushManagerMetrics(scope tally.Scope) leaderFlushManagerMetrics {
	standardScope := scope.Tagged(map[string]string{"flusher-type": "standard"})
	forwardedScope := scope.Tagged(map[string]string{"flusher-type": "forwarded"})
	timedScope := scope.Tagged(map[string]string{"flusher-type": "timed"})
	return leaderFlushManagerMetrics{
		queueSize:             scope.Gauge("queue-size"),
		getShardsError:        scope.Counter("get-shards-error"),
		flushTimesUpdateError: scope.Counter("flush-times-update-error"),
		standard:              newLeaderFlusherMetrics(standardScope),
		forwarded:             newLeaderFlusherMetrics(forwardedScope),
		timed:                 newLeaderFlusherMetrics(timedScope),
	}
}

type randFn func(int64) int64

type leaderFlushManager struct {
	sync.RWMutex

	nowFn                  clock.NowFn
	checkEvery             time.Duration
	workers                xsync.WorkerPool
	placementManager       PlacementManager
	flushTimesManager      FlushTimesManager
	flushTimesPersistEvery time.Duration
	maxBufferSize          time.Duration
	logger                 *zap.Logger
	scope                  tally.Scope

	doneCh              <-chan struct{}
	flushTimes          flushMetadataHeap
	flushedByShard      map[uint32]*schema.ShardFlushTimes
	lastPersistAtNanos  int64
	flushedSincePersist bool
	flushTask           *leaderFlushTask
	metrics             leaderFlushManagerMetrics
}

func newLeaderFlushManager(
	doneCh <-chan struct{},
	opts FlushManagerOptions,
) roleBasedFlushManager {
	nowFn := opts.ClockOptions().NowFn()
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()
	mgr := &leaderFlushManager{
		nowFn:                  nowFn,
		checkEvery:             opts.CheckEvery(),
		workers:                opts.WorkerPool(),
		placementManager:       opts.PlacementManager(),
		flushTimesManager:      opts.FlushTimesManager(),
		flushTimesPersistEvery: opts.FlushTimesPersistEvery(),
		maxBufferSize:          opts.MaxBufferSize(),
		logger:                 instrumentOpts.Logger(),
		scope:                  scope,
		doneCh:                 doneCh,
		flushedByShard:         make(map[uint32]*schema.ShardFlushTimes, defaultInitialFlushCapacity),
		lastPersistAtNanos:     nowFn().UnixNano(),
		metrics:                newLeaderFlushManagerMetrics(scope),
	}
	mgr.flushTask = &leaderFlushTask{
		mgr:      mgr,
		flushers: make([]flushingMetricList, 0, defaultInitialFlushCapacity),
	}
	return mgr
}

func (mgr *leaderFlushManager) Open() {}

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
			shouldFlush = true
			waitFor = 0
			bucketIdx := earliestFlush.bucketIdx
			buckets[bucketIdx].flushLag.RecordDuration(time.Duration(nowNanos - earliestFlush.timeNanos))
			// NB(xichen): make a shallow copy of the flushers inside the lock
			// and use the snapshot for flushing below because the flushers slice
			// inside the bucket may be modified during task execution when new
			// flushers are registered or old flushers are unregistered.
			mgr.flushTask.duration = buckets[bucketIdx].duration
			mgr.flushTask.jitter = buckets[bucketIdx].offset
			mgr.flushTask.flushers = append(mgr.flushTask.flushers[:0], buckets[bucketIdx].flushers...)
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

	// NB: if mgr.flushTimesPersistEvery is zero, always prepare a flush times
	// update task.
	durationSinceLastPersist := time.Duration(nowNanos - mgr.lastPersistAtNanos)
	if durationSinceLastPersist >= mgr.flushTimesPersistEvery && mgr.flushedSincePersist {
		if shouldFlush {
			mgr.flushTask.onComplete = func() {
				mgr.Lock()
				mgr.updateFlushTimesStoreWithLock(buckets)
				mgr.Unlock()
			}
		} else {
			mgr.updateFlushTimesStoreWithLock(buckets)
		}
	}

	if !shouldFlush {
		return nil, waitFor
	}

	return mgr.flushTask, waitFor
}

func (mgr *leaderFlushManager) updateFlushTimesStoreWithLock(
	buckets []*flushBucket,
) {
	if !mgr.flushedSincePersist {
		return
	}

	shards, err := mgr.placementManager.Shards()
	if err != nil {
		mgr.RUnlock()
		mgr.metrics.getShardsError.Inc(1)
		mgr.logger.Error("unable to determine shards owned by this instance", zap.Error(err))
		return
	}

	allShards := shards.All()

	mgr.flushedSincePersist = false
	flushTimes := mgr.prepareFlushTimesWithLock(buckets, allShards)
	if err := mgr.flushTimesManager.StoreSync(flushTimes); err != nil {
		mgr.metrics.flushTimesUpdateError.Inc(1)
		mgr.logger.Error("unable to store flush times", zap.Error(err))
	}

	mgr.lastPersistAtNanos = mgr.nowNanos()
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

// Reset the next flush timestamp of the bucket to be t0 = truncate(now, flushInterval)
// + flushOffset. As such, the newly added flusher will be flushed at t0 (or immediately
// if t0 < now) at the latest, at which point its last flushed time will be properly
// initialized. This helps to speed up deployment in certain cases and allow the follower
// to properly discard metrics, both of which rely on the last flushed times of the flushers.
func (mgr *leaderFlushManager) OnFlusherAdded(
	bucketIdx int,
	bucket *flushBucket,
	flusher flushingMetricList,
) {
	mgr.Lock()
	defer mgr.Unlock()

	for i := 0; i < len(mgr.flushTimes); i++ {
		if mgr.flushTimes[i].bucketIdx == bucketIdx {
			nextFlushNanos := mgr.computeNextFlushNanos(bucket.interval, bucket.offset)
			if nextFlushNanos == mgr.flushTimes[i].timeNanos {
				return
			}
			mgr.flushTimes[i].timeNanos = nextFlushNanos
			mgr.flushTimes.Fix(i)
			return
		}
	}
}

// NB(xichen): leader flush manager can always lead.
func (mgr *leaderFlushManager) CanLead() bool { return true }

func (mgr *leaderFlushManager) Close() {}

func (mgr *leaderFlushManager) enqueueBucketWithLock(
	bucketIdx int,
	bucket *flushBucket,
) {
	nextFlushNanos := mgr.computeNextFlushNanos(bucket.interval, bucket.offset)
	newFlushMetadata := flushMetadata{
		timeNanos: nextFlushNanos,
		bucketIdx: bucketIdx,
	}
	mgr.flushTimes.Push(newFlushMetadata)
}

func (mgr *leaderFlushManager) computeNextFlushNanos(
	flushInterval, flushOffset time.Duration,
) int64 {
	now := mgr.nowFn()
	alignedNowNanos := now.Truncate(flushInterval).UnixNano()
	nextFlushNanos := alignedNowNanos + flushOffset.Nanoseconds()
	return nextFlushNanos
}

func (mgr *leaderFlushManager) prepareFlushTimesWithLock(
	buckets []*flushBucket,
	shards []shard.Shard,
) *schema.ShardSetFlushTimes {
	// Update internal flush times to the latest flush times of all the flushers in the buckets.
	mgr.updateFlushTimesWithLock(buckets, shards)

	// Make a copy of the updated flush times for asynchronous persistence.
	cloned := cloneFlushTimesByShard(mgr.flushedByShard)

	return &schema.ShardSetFlushTimes{ByShard: cloned}
}

func (mgr *leaderFlushManager) updateFlushTimesWithLock(
	buckets []*flushBucket,
	shards []shard.Shard,
) {
	for _, shardFlushTimes := range mgr.flushedByShard {
		shardFlushTimes.Tombstoned = true
	}
	for _, bucket := range buckets {
		bucketID := bucket.bucketID
		switch bucketID.listType {
		case standardMetricListType:
			mgr.updateStandardFlushTimesWithLock(
				bucketID.standard.resolution,
				bucket.flushers,
				getStandardFlushTimesByResolutionFn,
				mgr.metrics.standard,
				shards,
			)
		case forwardedMetricListType:
			mgr.updateForwardedFlushTimesWithLock(bucketID.forwarded, bucket.flushers, shards)
		case timedMetricListType:
			mgr.updateStandardFlushTimesWithLock(
				bucketID.timed.resolution,
				bucket.flushers,
				getTimedFlushTimesByResolutionFn,
				mgr.metrics.timed,
				shards,
			)
		default:
			panic("should never get here")
		}
	}
}

func (mgr *leaderFlushManager) updateStandardFlushTimesWithLock(
	resolution time.Duration,
	flushers []flushingMetricList,
	getFlushTimesByResolutionFn getFlushTimesByResolutionFn,
	metrics leaderFlusherMetrics,
	shards []shard.Shard,
) {
	for _, flusher := range flushers {
		flushTimes := mgr.getOrCreateFlushTimesWithLock(flusher.Shard())
		flushTimesByResolution := getFlushTimesByResolutionFn(flushTimes)
		flushTimesByResolution[int64(resolution)] = flusher.LastFlushedNanos()
		flushTimes.Tombstoned = false
	}

	// Assign flush times to redirected shards.
	// NB: we only support a single layer of shard redirection.
	for _, shard := range shards {
		redirectToShardID := shard.RedirectToShardID()
		if redirectToShardID == nil {
			continue
		}

		redirectToFlushTimes, exists := mgr.flushedByShard[*redirectToShardID]
		if !exists {
			continue
		}

		redirectToFlushTimesByResolution := getFlushTimesByResolutionFn(redirectToFlushTimes)

		if redirectToFlushTime, ok := redirectToFlushTimesByResolution[int64(resolution)]; ok {
			flushTimes := mgr.getOrCreateFlushTimesWithLock(shard.ID())
			flushTimesByResolution := getFlushTimesByResolutionFn(flushTimes)
			flushTimesByResolution[int64(resolution)] = redirectToFlushTime
			flushTimes.Tombstoned = flushTimes.Tombstoned && redirectToFlushTimes.Tombstoned
		}
	}

	metrics.updateFlushTimes.Inc(int64(len(flushers)))
}

func (mgr *leaderFlushManager) updateForwardedFlushTimesWithLock(
	listID forwardedMetricListID,
	flushers []flushingMetricList,
	shards []shard.Shard,
) {
	var (
		resolution        = int64(listID.resolution)
		numForwardedTimes = int32(listID.numForwardedTimes)
	)
	for _, flusher := range flushers {
		flushTimes := mgr.getOrCreateFlushTimesWithLock(flusher.Shard())
		forwardedFlushTimes := mgr.getOrCreateForwarderFlushTimesForResolutionWithLock(flushTimes, resolution)
		forwardedFlushTimes.ByNumForwardedTimes[numForwardedTimes] = flusher.LastFlushedNanos()
		flushTimes.Tombstoned = false
	}

	// Assign flush times to redirected shards.
	// NB: we only support a single layer of shard redirection.
	for _, shard := range shards {
		redirectToShardID := shard.RedirectToShardID()
		if redirectToShardID == nil {
			continue
		}

		redirectToFlushTimes, exists := mgr.flushedByShard[*redirectToShardID]
		if !exists {
			continue
		}
		redirectToForwardedFlushTimes, exists := redirectToFlushTimes.ForwardedByResolution[resolution]
		if !exists {
			continue
		}

		if redirectToFlushTime, ok := redirectToForwardedFlushTimes.ByNumForwardedTimes[numForwardedTimes]; ok {
			flushTimes := mgr.getOrCreateFlushTimesWithLock(shard.ID())
			forwardedFlushTimes := mgr.getOrCreateForwarderFlushTimesForResolutionWithLock(flushTimes, resolution)
			forwardedFlushTimes.ByNumForwardedTimes[numForwardedTimes] = redirectToFlushTime
			flushTimes.Tombstoned = flushTimes.Tombstoned && redirectToFlushTimes.Tombstoned
		}
	}

	mgr.metrics.forwarded.updateFlushTimes.Inc(int64(len(flushers)))
}

func (mgr *leaderFlushManager) getOrCreateFlushTimesWithLock(shardID uint32) *schema.ShardFlushTimes {
	flushTimes, exists := mgr.flushedByShard[shardID]
	if !exists {
		flushTimes = newShardFlushTimes()
		mgr.flushedByShard[shardID] = flushTimes
	}
	return flushTimes
}

func (mgr *leaderFlushManager) getOrCreateForwarderFlushTimesForResolutionWithLock(
	flushTimes *schema.ShardFlushTimes,
	resolution int64,
) *schema.ForwardedFlushTimesForResolution {
	forwardedFlushTimes, exists := flushTimes.ForwardedByResolution[resolution]
	if !exists {
		forwardedFlushTimes = newForwardedFlushTimesForResolution()
		flushTimes.ForwardedByResolution[resolution] = forwardedFlushTimes
	}
	return forwardedFlushTimes
}

func (mgr *leaderFlushManager) nowNanos() int64 { return mgr.nowFn().UnixNano() }

func newShardFlushTimes() *schema.ShardFlushTimes {
	return &schema.ShardFlushTimes{
		StandardByResolution:  make(map[int64]int64),
		ForwardedByResolution: make(map[int64]*schema.ForwardedFlushTimesForResolution),
		TimedByResolution:     make(map[int64]int64),
	}
}

func newForwardedFlushTimesForResolution() *schema.ForwardedFlushTimesForResolution {
	return &schema.ForwardedFlushTimesForResolution{
		ByNumForwardedTimes: make(map[int32]int64),
	}
}

func cloneFlushTimesByShard(
	proto map[uint32]*schema.ShardFlushTimes,
) map[uint32]*schema.ShardFlushTimes {
	clonedFlushTimesByShard := make(map[uint32]*schema.ShardFlushTimes, len(proto))
	for k, v := range proto {
		clonedFlushTimesByShard[k] = cloneShardFlushTimes(v)
	}
	return clonedFlushTimesByShard
}

func cloneShardFlushTimes(proto *schema.ShardFlushTimes) *schema.ShardFlushTimes {
	clonedShardFlushTimes := &schema.ShardFlushTimes{
		StandardByResolution:  make(map[int64]int64, len(proto.StandardByResolution)),
		ForwardedByResolution: make(map[int64]*schema.ForwardedFlushTimesForResolution, len(proto.ForwardedByResolution)),
		TimedByResolution:     make(map[int64]int64, len(proto.TimedByResolution)),
		Tombstoned:            proto.Tombstoned,
	}

	for k, v := range proto.StandardByResolution {
		clonedShardFlushTimes.StandardByResolution[k] = v
	}
	for k, v := range proto.ForwardedByResolution {
		clonedShardFlushTimes.ForwardedByResolution[k] = cloneForwardedFlushTimesForResolution(v)
	}
	for k, v := range proto.TimedByResolution {
		clonedShardFlushTimes.TimedByResolution[k] = v
	}
	return clonedShardFlushTimes
}

func cloneForwardedFlushTimesForResolution(
	proto *schema.ForwardedFlushTimesForResolution,
) *schema.ForwardedFlushTimesForResolution {
	cloned := &schema.ForwardedFlushTimesForResolution{
		ByNumForwardedTimes: make(map[int32]int64, len(proto.ByNumForwardedTimes)),
	}
	for k, v := range proto.ByNumForwardedTimes {
		cloned.ByNumForwardedTimes[k] = v
	}
	return cloned
}

type leaderFlushTask struct {
	mgr        *leaderFlushManager
	duration   tally.Timer
	jitter     time.Duration
	flushers   []flushingMetricList
	onComplete func()
}

func (t *leaderFlushTask) Run() {
	mgr := t.mgr
	shards, err := mgr.placementManager.Shards()
	if err != nil {
		mgr.logger.Error("unable to determine shards owned by this instance", zap.Error(err))
		return
	}

	var (
		wgWorkers sync.WaitGroup
		start     = mgr.nowFn()
		jitter    = t.jitter
	)
	for _, flusher := range t.flushers {
		// By default traffic is cut off from a shard, unless the shard is in the list of
		// shards owned by the instance, in which case the cutover time and the cutoff time
		// are set to the corresponding cutover and cutoff times of the shard.
		var cutoverNanos, cutoffNanos int64
		shardID := flusher.Shard()
		if shard, found := shards.Shard(shardID); found {
			cutoverNanos = shard.CutoverNanos()
			cutoffNanos = shard.CutoffNanos()
		}

		// We intentionally buffer data for some time after the shard is cut off to ensure
		// the leaving instance has good data after the shard transfer happens during a
		// topology change in case we need to back out of the change and move the shard
		// back to the instance.
		req := flushRequest{
			CutoverNanos:      cutoverNanos,
			CutoffNanos:       cutoffNanos,
			BufferAfterCutoff: mgr.maxBufferSize,
			Jitter:            jitter,
		}
		flusher := flusher
		wgWorkers.Add(1)
		mgr.workers.Go(func() {
			flusher.Flush(req)
			wgWorkers.Done()
		})
	}
	wgWorkers.Wait()
	t.duration.Record(mgr.nowFn().Sub(start))

	if t.onComplete != nil {
		t.onComplete()
	}
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
	h.up(h.Len() - 1)
}

// Pop pops the metadata with the earliest flush time from the heap.
func (h *flushMetadataHeap) Pop() flushMetadata {
	var (
		old = *h
		n   = old.Len()
		val = old[0]
	)

	old[0], old[n-1] = old[n-1], old[0]
	h.down(0, n-1)
	*h = (*h)[0 : n-1]
	return val
}

// Fix re-establishes the ordering after the element at index i has
// changed its value.
func (h *flushMetadataHeap) Fix(i int) {
	if !h.down(i, h.Len()) {
		h.up(i)
	}
}

func (h flushMetadataHeap) up(i int) {
	for {
		parent := (i - 1) / 2
		if parent == i || h[parent].timeNanos <= h[i].timeNanos {
			break
		}
		h[parent], h[i] = h[i], h[parent]
		i = parent
	}
}

// down heapifies the element at index i0 by attempting to shift it downwards, returning
// true if the element has been successfully moved downwards, and false otherwise.
func (h flushMetadataHeap) down(i0, n int) bool {
	i := i0
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
			break
		}
		h[i], h[smallest] = h[smallest], h[i]
		i = smallest
	}
	return i > i0
}
