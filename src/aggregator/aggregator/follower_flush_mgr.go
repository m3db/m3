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
	"sync"
	"time"

	schema "github.com/m3db/m3aggregator/generated/proto/flush"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	xsync "github.com/m3db/m3x/sync"

	"github.com/uber-go/tally"
)

const (
	defaultInitialFlushTimesCapacity = 16
)

type followerFlushManagerMetrics struct {
	watchCreateErrors      tally.Counter
	unmarshalErrors        tally.Counter
	shardNotFound          tally.Counter
	resolutionNotFound     tally.Counter
	kvUpdateFlush          tally.Counter
	forcedFlush            tally.Counter
	flushTimesNotProcessed tally.Counter
	flushWindowsNotEnded   tally.Counter
}

func newFollowerFlushManagerMetrics(scope tally.Scope) followerFlushManagerMetrics {
	return followerFlushManagerMetrics{
		watchCreateErrors:      scope.Counter("watch-create-errors"),
		unmarshalErrors:        scope.Counter("unmarshal-errors"),
		shardNotFound:          scope.Counter("shard-not-found"),
		resolutionNotFound:     scope.Counter("resolution-not-found"),
		kvUpdateFlush:          scope.Counter("kv-update-flush"),
		forcedFlush:            scope.Counter("forced-flush"),
		flushTimesNotProcessed: scope.Counter("flush-times-not-processed"),
		flushWindowsNotEnded:   scope.Counter("flush-windows-not-ended"),
	}
}

type followerFlushManager struct {
	sync.RWMutex

	nowFn                 clock.NowFn
	checkEvery            time.Duration
	workers               xsync.WorkerPool
	flushTimesKeyFmt      string
	flushTimesStore       kv.Store
	maxNoFlushDuration    time.Duration
	forcedFlushWindowSize time.Duration
	logger                log.Logger
	scope                 tally.Scope

	doneCh          <-chan struct{}
	proto           *schema.ShardSetFlushTimes
	flushTimesKey   string
	flushTimesState flushTimesState
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
		flushTimesKeyFmt:      opts.FlushTimesKeyFmt(),
		flushTimesStore:       opts.FlushTimesStore(),
		maxNoFlushDuration:    opts.MaxNoFlushDuration(),
		forcedFlushWindowSize: opts.ForcedFlushWindowSize(),
		logger:                instrumentOpts.Logger(),
		scope:                 scope,
		doneCh:                doneCh,
		proto:                 &schema.ShardSetFlushTimes{},
		flushTimesState:       flushTimesUninitialized,
		lastFlushed:           nowFn(),
		sleepFn:               time.Sleep,
		metrics:               newFollowerFlushManagerMetrics(scope),
	}
	mgr.flushTask = &followerFlushTask{mgr: mgr}
	return mgr
}

func (mgr *followerFlushManager) Open(shardSetID uint32) error {
	mgr.Lock()
	defer mgr.Unlock()

	mgr.flushTimesKey = fmt.Sprintf(mgr.flushTimesKeyFmt, shardSetID)
	mgr.openedAt = mgr.nowFn()
	go mgr.watchFlushTimes()
	return nil
}

// NB(xichen): no actions needed for initializing the follower flush manager.
func (mgr *followerFlushManager) Init([]*flushBucket) {}

func (mgr *followerFlushManager) Prepare(buckets []*flushBucket) (flushTask, time.Duration) {
	var (
		shouldFlush        = false
		flushersByInterval []flushersGroup
	)
	// NB(xichen): a flush is triggered in the following scenarios:
	// * The flush times persisted in kv have been updated since last flush.
	// * Sufficient time (a.k.a. maxNoFlushDuration) has elapsed since last flush.
	now := mgr.nowFn()
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.flushTimesState == flushTimesUpdated {
		mgr.lastFlushed = now
		mgr.flushTimesState = flushTimesProcessed
		flushersByInterval = mgr.flushersFromKVUpdateWithLock(buckets)
		shouldFlush = true
		mgr.metrics.kvUpdateFlush.Inc(1)
	} else {
		durationSinceLastFlush := now.Sub(mgr.lastFlushed)
		if durationSinceLastFlush > mgr.maxNoFlushDuration {
			mgr.lastFlushed = now
			shouldFlush = true
			mgr.metrics.forcedFlush.Inc(1)
			flushBeforeNanos := now.Add(-mgr.maxNoFlushDuration).Add(mgr.forcedFlushWindowSize).UnixNano()
			flushersByInterval = mgr.flushersFromForcedFlush(buckets, flushBeforeNanos)
		}
	}

	if !shouldFlush {
		return nil, mgr.checkEvery
	}
	mgr.flushTask.flushersByInterval = flushersByInterval
	return mgr.flushTask, 0
}

// NB(xichen): the follower flush manager flushes data based on the flush times
// stored in kv and does not need to take extra actions when a new bucket is added.
func (mgr *followerFlushManager) OnBucketAdded(int, *flushBucket) {}

// The follower flush manager may only lead if and only if all the following conditions
// are met:
// * The flush times persisted in kv have been read at least once.
// * There are no outstanding/unprocessed kv flush times update.
// * All the aggregation windows since the flush manager is opened have ended.
func (mgr *followerFlushManager) CanLead() bool {
	mgr.RLock()
	defer mgr.RUnlock()

	if mgr.flushTimesState != flushTimesProcessed {
		mgr.metrics.flushTimesNotProcessed.Inc(1)
		return false
	}

	for _, shardFlushTimes := range mgr.proto.ByShard {
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

func (mgr *followerFlushManager) flushersFromKVUpdateWithLock(buckets []*flushBucket) []flushersGroup {
	flushersByInterval := make([]flushersGroup, len(buckets))
	for i, bucket := range buckets {
		flushersByInterval[i].interval = bucket.interval
		flushersByInterval[i].duration = bucket.duration
		flushersByInterval[i].flushers = make([]flusherWithTime, 0, defaultInitialFlushTimesCapacity)
		for _, flusher := range bucket.flushers {
			shard := flusher.Shard()
			shardFlushTimes, exists := mgr.proto.ByShard[shard]
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
	var (
		throttlePeriod  = time.Second
		flushTimesWatch kv.ValueWatch
		err             error
	)

	for {
		if flushTimesWatch == nil {
			flushTimesWatch, err = mgr.flushTimesStore.Watch(mgr.flushTimesKey)
			if err != nil {
				mgr.metrics.watchCreateErrors.Inc(1)
				mgr.logger.WithFields(
					log.NewField("flushTimesKey", mgr.flushTimesKey),
					log.NewErrField(err),
				).Error("error creating flush times watch")
				mgr.sleepFn(throttlePeriod)
				continue
			}
		}

		select {
		case <-flushTimesWatch.C():
		case <-mgr.doneCh:
			return
		}

		var (
			value = flushTimesWatch.Get()
			proto schema.ShardSetFlushTimes
		)
		if err = value.Unmarshal(&proto); err != nil {
			mgr.metrics.unmarshalErrors.Inc(1)
			mgr.logger.WithFields(
				log.NewField("flushTimesKey", mgr.flushTimesKey),
				log.NewErrField(err),
			).Error("flush times unmarshal error")
			continue
		}
		mgr.Lock()
		mgr.proto = &proto
		mgr.flushTimesState = flushTimesUpdated
		mgr.Unlock()
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

type flusherWithTime struct {
	flusher          PeriodicFlusher
	flushBeforeNanos int64
}

type flushersGroup struct {
	interval time.Duration
	duration tally.Timer
	flushers []flusherWithTime
}
