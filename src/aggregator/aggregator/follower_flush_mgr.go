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

	schema "github.com/m3db/m3aggregator/generated/proto/flush"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	xsync "github.com/m3db/m3x/sync"
	"github.com/m3db/m3x/watch"

	"github.com/uber-go/tally"
)

type followerFlushManagerMetrics struct {
	watchCreateErrors    tally.Counter
	shardNotFound        tally.Counter
	resolutionNotFound   tally.Counter
	kvUpdateFlush        tally.Counter
	forcedFlush          tally.Counter
	notCampaigning       tally.Counter
	flushWindowsNotEnded tally.Counter
}

func newFollowerFlushManagerMetrics(scope tally.Scope) followerFlushManagerMetrics {
	return followerFlushManagerMetrics{
		watchCreateErrors:    scope.Counter("watch-create-errors"),
		shardNotFound:        scope.Counter("shard-not-found"),
		resolutionNotFound:   scope.Counter("resolution-not-found"),
		kvUpdateFlush:        scope.Counter("kv-update-flush"),
		forcedFlush:          scope.Counter("forced-flush"),
		notCampaigning:       scope.Counter("not-campaigning"),
		flushWindowsNotEnded: scope.Counter("flush-windows-not-ended"),
	}
}

type followerFlushManager struct {
	sync.RWMutex
	sync.WaitGroup

	nowFn                 clock.NowFn
	checkEvery            time.Duration
	workers               xsync.WorkerPool
	placementManager      PlacementManager
	electionManager       ElectionManager
	flushTimesManager     FlushTimesManager
	maxBufferSize         time.Duration
	forcedFlushWindowSize time.Duration
	logger                log.Logger
	scope                 tally.Scope

	doneCh          <-chan struct{}
	received        *schema.ShardSetFlushTimes
	processed       *schema.ShardSetFlushTimes
	flushTimesState flushTimesState
	flushMode       followerFlushMode
	lastFlushed     time.Time
	openedAt        time.Time
	flushTask       *followerFlushTask
	sleepFn         sleepFn
	metrics         followerFlushManagerMetrics
}

func newFollowerFlushManager(
	doneCh <-chan struct{},
	opts FlushManagerOptions,
) roleBasedFlushManager {
	nowFn := opts.ClockOptions().NowFn()
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()
	mgr := &followerFlushManager{
		nowFn:                 nowFn,
		checkEvery:            opts.CheckEvery(),
		workers:               opts.WorkerPool(),
		placementManager:      opts.PlacementManager(),
		electionManager:       opts.ElectionManager(),
		flushTimesManager:     opts.FlushTimesManager(),
		maxBufferSize:         opts.MaxBufferSize(),
		forcedFlushWindowSize: opts.ForcedFlushWindowSize(),
		logger:                instrumentOpts.Logger(),
		scope:                 scope,
		doneCh:                doneCh,
		flushTimesState:       flushTimesUninitialized,
		flushMode:             unknownFollowerFlush,
		lastFlushed:           nowFn(),
		sleepFn:               time.Sleep,
		metrics:               newFollowerFlushManagerMetrics(scope),
	}
	mgr.flushTask = &followerFlushTask{mgr: mgr}
	return mgr
}

func (mgr *followerFlushManager) Open() {
	mgr.Lock()
	defer mgr.Unlock()

	mgr.openedAt = mgr.nowFn()
	mgr.Add(1)
	go mgr.watchFlushTimes()
}

// NB(xichen): no actions needed for initializing the follower flush manager.
func (mgr *followerFlushManager) Init([]*flushBucket) {}

func (mgr *followerFlushManager) Prepare(buckets []*flushBucket) (flushTask, time.Duration) {
	// NB(xichen): a flush is triggered in the following scenarios:
	// * The flush times persisted in kv have been updated since last flush, or
	// * Sufficient time (a.k.a. maxBufferSize) has elapsed since last flush.
	mgr.Lock()
	defer mgr.Unlock()

	var (
		now                = mgr.nowFn()
		needsFlush         bool
		flushersByInterval []flushersGroup
	)
	if mgr.flushTimesState == flushTimesUpdated {
		mgr.flushTimesState = flushTimesProcessed
		mgr.processed = mgr.received
		mgr.flushMode = kvUpdateFollowerFlush
		flushersByInterval = mgr.flushersFromKVUpdateWithLock(buckets)
		needsFlush = true
		mgr.metrics.kvUpdateFlush.Inc(1)
	} else {
		durationSinceLastFlush := now.Sub(mgr.lastFlushed)
		if mgr.flushMode != forcedFollowerFlush && durationSinceLastFlush >= mgr.maxBufferSize {
			mgr.flushMode = forcedFollowerFlush
		}
		if mgr.flushMode == forcedFollowerFlush && durationSinceLastFlush >= mgr.forcedFlushWindowSize {
			flushBeforeNanos := now.Add(-mgr.maxBufferSize).UnixNano()
			flushersByInterval = mgr.flushersFromForcedFlush(buckets, flushBeforeNanos)
			needsFlush = true
			mgr.metrics.forcedFlush.Inc(1)
		}
	}

	if !needsFlush {
		return nil, mgr.checkEvery
	}
	mgr.lastFlushed = now
	mgr.flushTask.flushersByInterval = flushersByInterval
	return mgr.flushTask, 0
}

// NB(xichen): the follower flush manager flushes data based on the flush times
// stored in kv and does not need to take extra actions when a new bucket is added.
func (mgr *followerFlushManager) OnBucketAdded(int, *flushBucket) {}

// The follower flush manager may only lead if and only if all the following conditions
// are met:
// * The instance is campaigning.
// * All the aggregation windows since the flush manager is opened have ended.
func (mgr *followerFlushManager) CanLead() bool {
	mgr.RLock()
	defer mgr.RUnlock()

	if !mgr.electionManager.IsCampaigning() {
		mgr.metrics.notCampaigning.Inc(1)
		return false
	}
	if mgr.processed == nil {
		return false
	}
	for _, shardFlushTimes := range mgr.processed.ByShard {
		for windowNanos, lastFlushedNanos := range shardFlushTimes.ByResolution {
			windowSize := time.Duration(windowNanos)
			windowEndAt := mgr.openedAt.Truncate(windowSize)
			if windowEndAt.Before(mgr.openedAt) {
				windowEndAt = windowEndAt.Add(windowSize)
			}
			if lastFlushedNanos < windowEndAt.UnixNano() {
				mgr.metrics.flushWindowsNotEnded.Inc(1)
				return false
			}
		}
	}
	return true
}

func (mgr *followerFlushManager) Close() { mgr.Wait() }

func (mgr *followerFlushManager) flushersFromKVUpdateWithLock(buckets []*flushBucket) []flushersGroup {
	flushersByInterval := make([]flushersGroup, len(buckets))
	for i, bucket := range buckets {
		flushersByInterval[i].interval = bucket.interval
		flushersByInterval[i].duration = bucket.duration
		flushersByInterval[i].flushers = make([]flusherWithTime, 0, defaultInitialFlushTimesCapacity)
		for _, flusher := range bucket.flushers {
			shard := flusher.Shard()
			shardFlushTimes, exists := mgr.received.ByShard[shard]
			if !exists {
				mgr.metrics.shardNotFound.Inc(1)
				mgr.logger.WithFields(
					log.NewField("shard", shard),
				).Warn("shard not found in flush times")
				continue
			}
			resolution := flusher.Resolution()
			lastFlushedAtNanos, exists := shardFlushTimes.ByResolution[int64(resolution)]
			if !exists {
				mgr.metrics.resolutionNotFound.Inc(1)
				mgr.logger.WithFields(
					log.NewField("shard", shard),
					log.NewField("resolution", resolution.String()),
				).Warn("resolution not found in flush times")
				continue
			}
			newFlushTarget := flusherWithTime{
				flusher:          flusher,
				flushBeforeNanos: lastFlushedAtNanos,
			}
			flushersByInterval[i].flushers = append(flushersByInterval[i].flushers, newFlushTarget)
		}
	}
	return flushersByInterval
}

func (mgr *followerFlushManager) flushersFromForcedFlush(
	buckets []*flushBucket,
	flushBeforeNanos int64,
) []flushersGroup {
	flushersByInterval := make([]flushersGroup, len(buckets))
	for i, bucket := range buckets {
		flushersByInterval[i].interval = bucket.interval
		flushersByInterval[i].duration = bucket.duration
		flushersByInterval[i].flushers = make([]flusherWithTime, 0, defaultInitialFlushTimesCapacity)
		for _, flusher := range bucket.flushers {
			newFlushTarget := flusherWithTime{
				flusher:          flusher,
				flushBeforeNanos: flushBeforeNanos,
			}
			flushersByInterval[i].flushers = append(flushersByInterval[i].flushers, newFlushTarget)
		}
	}
	return flushersByInterval
}

func (mgr *followerFlushManager) watchFlushTimes() {
	defer mgr.Done()

	var (
		throttlePeriod  = time.Second
		flushTimesWatch watch.Watch
		err             error
	)

	for {
		if flushTimesWatch == nil {
			flushTimesWatch, err = mgr.flushTimesManager.Watch()
			if err != nil {
				mgr.metrics.watchCreateErrors.Inc(1)
				mgr.sleepFn(throttlePeriod)
				continue
			}
		}

		select {
		case <-flushTimesWatch.C():
			mgr.Lock()
			mgr.received = flushTimesWatch.Get().(*schema.ShardSetFlushTimes)
			mgr.flushTimesState = flushTimesUpdated
			mgr.Unlock()
		case <-mgr.doneCh:
			return
		}
	}
}

type followerFlushTask struct {
	mgr                *followerFlushManager
	flushersByInterval []flushersGroup
}

func (t *followerFlushTask) Run() {
	var (
		mgr       = t.mgr
		wgWorkers sync.WaitGroup
	)
	for _, group := range t.flushersByInterval {
		start := mgr.nowFn()
		for _, flusherWithTime := range group.flushers {
			flusherWithTime := flusherWithTime
			wgWorkers.Add(1)
			mgr.workers.Go(func() {
				flusherWithTime.flusher.DiscardBefore(flusherWithTime.flushBeforeNanos)
				wgWorkers.Done()
			})
		}
		wgWorkers.Wait()
		group.duration.Record(mgr.nowFn().Sub(start))
	}
}

type flushTimesState int

const (
	flushTimesUninitialized flushTimesState = iota
	flushTimesUpdated
	flushTimesProcessed
)

type followerFlushMode int

const (
	unknownFollowerFlush followerFlushMode = iota
	kvUpdateFollowerFlush
	forcedFollowerFlush
)

type flusherWithTime struct {
	flusher          PeriodicFlusher
	flushBeforeNanos int64
}

type flushersGroup struct {
	interval time.Duration
	duration tally.Timer
	flushers []flusherWithTime
}
