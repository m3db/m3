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

package storage

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	resetShardInsertsEvery = 3 * time.Minute
)

var (
	errShardInsertQueueNotOpen             = errors.New("shard insert queue is not open")
	errShardInsertQueueAlreadyOpenOrClosed = errors.New("shard insert queue already open or is closed")
	errNewSeriesInsertRateLimitExceeded    = errors.New("shard insert of new series exceeds rate limit")
)

type dbShardInsertQueueState int

const (
	dbShardInsertQueueStateNotOpen dbShardInsertQueueState = iota
	dbShardInsertQueueStateOpen
	dbShardInsertQueueStateClosed
)

type dbShardInsertQueue struct {
	sync.RWMutex

	state              dbShardInsertQueueState
	nowFn              clock.NowFn
	insertEntryBatchFn dbShardInsertEntryBatchFn
	sleepFn            func(time.Duration)
	coreFn             xsync.CoreFn

	// rate limits, protected by mutex
	insertBatchBackoff   time.Duration
	insertPerSecondLimit *atomic.Uint64

	insertPerSecondLimitWindowNanos  *atomic.Uint64
	insertPerSecondLimitWindowValues *atomic.Uint64

	currBatch    *dbShardInsertBatch
	notifyInsert chan struct{}
	closeCh      chan struct{}

	metrics dbShardInsertQueueMetrics
	logger  *zap.Logger
}

type dbShardInsertQueueMetrics struct {
	insertsNoPendingWrite tally.Counter
	insertsPendingWrite   tally.Counter
	insertsBatchErrors    tally.Counter
}

func newDatabaseShardInsertQueueMetrics(
	scope tally.Scope,
) dbShardInsertQueueMetrics {
	insertName := "inserts"
	insertPendingWriteTagName := "pending-write"
	return dbShardInsertQueueMetrics{
		insertsNoPendingWrite: scope.Tagged(map[string]string{
			insertPendingWriteTagName: "no",
		}).Counter(insertName),
		insertsPendingWrite: scope.Tagged(map[string]string{
			insertPendingWriteTagName: "yes",
		}).Counter(insertName),
		insertsBatchErrors: scope.Counter("inserts-batch.errors"),
	}
}

type dbShardInsertEntryBatchFn func(inserts []dbShardInsert) error

// newDatabaseShardInsertQueue creates a new shard insert queue. The shard
// insert queue is used to batch inserts into the shard series map without
// sacrificing delays to insert the series.
//
// This is important as during floods of new IDs we want to avoid acquiring
// the lock to insert each individual series and insert as many as possible
// all together acquiring the lock once.
//
// It was experimented also sleeping for a very short duration, i.e. 1ms,
// during the insert loop and it actually added so much latency even just
// 1ms that it hurt it more than just acquiring the lock for each series.
//
// The batching as it is without any sleep and just relying on a notification
// trigger and hot looping when being flooded improved by a factor of roughly
// 4x during floods of new series.
func newDatabaseShardInsertQueue(
	insertEntryBatchFn dbShardInsertEntryBatchFn,
	nowFn clock.NowFn,
	coreFn xsync.CoreFn,
	scope tally.Scope,
	logger *zap.Logger,
) *dbShardInsertQueue {
	scope = scope.SubScope("insert-queue")
	currBatch := newDbShardInsertBatch(nowFn, scope)
	return &dbShardInsertQueue{
		nowFn:              nowFn,
		insertEntryBatchFn: insertEntryBatchFn,
		sleepFn:            time.Sleep,
		coreFn:             coreFn,
		currBatch:          currBatch,
		// NB(r): Use 2 * num cores so that each CPU insert queue which
		// is 1 per num CPU core can always enqueue a notification without
		// it being lost.
		notifyInsert:                     make(chan struct{}, 2*xsync.NumCores()),
		closeCh:                          make(chan struct{}, 1),
		insertPerSecondLimit:             atomic.NewUint64(0),
		insertPerSecondLimitWindowNanos:  atomic.NewUint64(0),
		insertPerSecondLimitWindowValues: atomic.NewUint64(0),
		metrics:                          newDatabaseShardInsertQueueMetrics(scope),
		logger:                           logger,
	}
}

func (q *dbShardInsertQueue) SetRuntimeOptions(value runtime.Options) {
	q.Lock()
	q.insertBatchBackoff = value.WriteNewSeriesBackoffDuration()
	q.Unlock()

	// Use atomics so no locks outside of per CPU core lock used.
	v := uint64(value.WriteNewSeriesLimitPerShardPerSecond())
	q.insertPerSecondLimit.Store(v)
}

func (q *dbShardInsertQueue) insertLoop() {
	defer func() {
		close(q.closeCh)
	}()

	var (
		lastInsert          time.Time
		allInserts          []dbShardInsert
		allInsertsLastReset time.Time
	)
	batch := newDbShardInsertBatch(q.nowFn, tally.NoopScope)
	for range q.notifyInsert {
		// Check if inserting too fast
		elapsedSinceLastInsert := q.nowFn().Sub(lastInsert)

		// Rotate batches
		var (
			state   dbShardInsertQueueState
			backoff time.Duration
		)
		q.Lock()
		state = q.state
		if elapsedSinceLastInsert < q.insertBatchBackoff {
			// Need to backoff before rotate and insert
			backoff = q.insertBatchBackoff - elapsedSinceLastInsert
		}
		q.Unlock()

		if backoff > 0 {
			q.sleepFn(backoff)
		}

		batchWg := q.currBatch.Rotate(batch)

		// NB(r): Either reset (to avoid spikey allocations sticking around
		// forever) or reuse existing slice.
		now := q.nowFn()
		if now.Sub(allInsertsLastReset) > resetShardInsertsEvery {
			allInserts = nil
			allInsertsLastReset = now
		} else {
			allInserts = allInserts[:0]
		}
		// Batch together for single insertion.
		for _, batchByCPUCore := range batch.insertsByCPUCore {
			batchByCPUCore.Lock()
			allInserts = append(allInserts, batchByCPUCore.inserts...)
			batchByCPUCore.Unlock()
		}

		err := q.insertEntryBatchFn(allInserts)
		if err != nil {
			q.metrics.insertsBatchErrors.Inc(1)
			q.logger.Error("shard insert queue batch insert failed",
				zap.Error(err))
		}

		batchWg.Done()

		// Memset optimization to clear inserts holding refs to objects.
		var insertZeroValue dbShardInsert
		for i := range allInserts {
			allInserts[i] = insertZeroValue
		}

		lastInsert = q.nowFn()

		if state != dbShardInsertQueueStateOpen {
			return // Break if the queue closed
		}
	}
}

func (q *dbShardInsertQueue) Start() error {
	q.Lock()
	defer q.Unlock()

	if q.state != dbShardInsertQueueStateNotOpen {
		return errShardInsertQueueAlreadyOpenOrClosed
	}

	q.state = dbShardInsertQueueStateOpen
	go q.insertLoop()
	return nil
}

func (q *dbShardInsertQueue) Stop() error {
	q.Lock()

	if q.state != dbShardInsertQueueStateOpen {
		q.Unlock()
		return errShardInsertQueueNotOpen
	}

	q.state = dbShardInsertQueueStateClosed
	q.Unlock()

	// Final flush.
	select {
	case q.notifyInsert <- struct{}{}:
	default:
		// Loop busy, already ready to consume notification.
	}

	// wait till other go routine is done
	<-q.closeCh

	return nil
}

func (q *dbShardInsertQueue) Insert(insert dbShardInsert) (*sync.WaitGroup, error) {
	if !insert.opts.skipRateLimit {
		if limit := q.insertPerSecondLimit.Load(); limit > 0 {
			windowNanos := uint64(q.nowFn().Truncate(time.Second).UnixNano())
			currLimitWindowNanos := q.insertPerSecondLimitWindowNanos.Load()
			if currLimitWindowNanos != windowNanos {
				// Rolled into a new window.
				if q.insertPerSecondLimitWindowNanos.CAS(currLimitWindowNanos, windowNanos) {
					// If managed to set it to the new window, reset the counter
					// otherwise another goroutine got to it first and
					// will zero the counter.
					q.insertPerSecondLimitWindowValues.Store(0)
				}
			}
			if q.insertPerSecondLimitWindowValues.Inc() > uint64(limit) {
				return nil, errNewSeriesInsertRateLimitExceeded
			}
		}
	}

	inserts := q.currBatch.insertsByCPUCore[q.coreFn()]
	inserts.Lock()
	// Track if first insert, if so then we need to notify insert loop,
	// otherwise we already have a pending notification.
	firstInsert := len(inserts.inserts) == 0
	inserts.inserts = append(inserts.inserts, insert)
	wg := inserts.wg
	inserts.Unlock()

	// Notify insert loop, only required if first to insert for this
	// this CPU core.
	if firstInsert {
		select {
		case q.notifyInsert <- struct{}{}:
		default:
			// Loop busy, already ready to consume notification.
		}
	}

	if insert.opts.hasPendingWrite {
		q.metrics.insertsPendingWrite.Inc(1)
	} else {
		q.metrics.insertsNoPendingWrite.Inc(1)
	}

	return wg, nil
}

type dbShardInsertBatch struct {
	nowFn clock.NowFn
	wg    *sync.WaitGroup
	// Note: since inserts by CPU core is allocated when
	// nsIndexInsertBatch is constructed and then never modified
	// it is safe to concurently read (but not modify obviously).
	insertsByCPUCore []*dbShardInsertsByCPUCore
	lastReset        time.Time
}

type dbShardInsertsByCPUCore struct {
	sync.Mutex

	wg      *sync.WaitGroup
	inserts []dbShardInsert
	metrics dbShardInsertsByCPUCoreMetrics
}

type dbShardInsert struct {
	entry *Entry
	opts  dbShardInsertAsyncOptions
}

type dbShardInsertAsyncOptions struct {
	skipRateLimit bool

	pendingWrite          dbShardPendingWrite
	pendingRetrievedBlock dbShardPendingRetrievedBlock
	pendingIndex          dbShardPendingIndex

	hasPendingWrite          bool
	hasPendingRetrievedBlock bool
	hasPendingIndexing       bool

	// NB(prateek): `releaseEntryRef` indicates if the
	// entry provided along with the dbShardInsertAsyncOptions
	// already has it's ref count incremented and it will be decremented after insert.
	// It's used to correctly manage the lifecycle of the entry across the
	// shard -> shard Queue -> shard boundaries.
	releaseEntryRef bool
}

type dbShardPendingWrite struct {
	timestamp  xtime.UnixNano
	value      float64
	unit       xtime.Unit
	annotation checked.Bytes
	opts       series.WriteOptions
}

type dbShardPendingIndex struct {
	timestamp  xtime.UnixNano
	enqueuedAt time.Time
}

type dbShardPendingRetrievedBlock struct {
	id      ident.ID
	tags    ident.TagIterator
	start   xtime.UnixNano
	segment ts.Segment
	nsCtx   namespace.Context
}

func newDbShardInsertBatch(
	nowFn clock.NowFn,
	scope tally.Scope,
) *dbShardInsertBatch {
	b := &dbShardInsertBatch{
		nowFn: nowFn,
		wg:    &sync.WaitGroup{},
	}
	numCores := xsync.NumCores()
	for i := 0; i < numCores; i++ {
		b.insertsByCPUCore = append(b.insertsByCPUCore, &dbShardInsertsByCPUCore{
			wg:      b.wg,
			metrics: newDBShardInsertsByCPUCoreMetrics(i, scope),
		})
	}
	b.Rotate(nil)
	return b
}

type dbShardInsertsByCPUCoreMetrics struct {
	rotateInserts tally.Counter
}

func newDBShardInsertsByCPUCoreMetrics(
	cpuIndex int,
	scope tally.Scope,
) dbShardInsertsByCPUCoreMetrics {
	scope = scope.Tagged(map[string]string{
		"cpu-index": strconv.Itoa(cpuIndex),
	})

	return dbShardInsertsByCPUCoreMetrics{
		rotateInserts: scope.Counter("rotate-inserts"),
	}
}

func (b *dbShardInsertBatch) Rotate(target *dbShardInsertBatch) *sync.WaitGroup {
	prevWg := b.wg

	// We always expect to be waiting for an index.
	b.wg = &sync.WaitGroup{}
	b.wg.Add(1)

	reset := false
	now := b.nowFn()
	if now.Sub(b.lastReset) > resetShardInsertsEvery {
		// NB(r): Sometimes this can grow very high, so we reset it
		// relatively frequently.
		reset = true
		b.lastReset = now
	}

	// Rotate to target if we need to.
	for idx, inserts := range b.insertsByCPUCore {
		if target == nil {
			// No target to rotate with.
			inserts.Lock()
			// Reset
			inserts.inserts = inserts.inserts[:0]
			// Use new wait group.
			inserts.wg = b.wg
			inserts.Unlock()
			continue
		}

		// First prepare the target to take the current batch's inserts.
		targetInserts := target.insertsByCPUCore[idx]
		targetInserts.Lock()

		// Reset the target inserts since we'll take ref to them in a second.
		var prevTargetInserts []dbShardInsert
		if !reset {
			// Only reuse if not resetting the allocation.
			// memset optimization.
			var zeroDbShardInsert dbShardInsert
			for i := range targetInserts.inserts {
				targetInserts.inserts[i] = zeroDbShardInsert
			}
			prevTargetInserts = targetInserts.inserts[:0]
		}

		// Lock the current batch inserts now ready to rotate to the target.
		inserts.Lock()

		// Update current slice refs to take target's inserts.
		targetInserts.inserts = inserts.inserts
		targetInserts.wg = inserts.wg

		// Reuse the target's old slices.
		inserts.inserts = prevTargetInserts

		// Use new wait group.
		inserts.wg = b.wg

		// Unlock as early as possible for writes to keep enqueuing.
		inserts.Unlock()

		numTargetInserts := len(targetInserts.inserts)

		// Now can unlock target inserts too.
		targetInserts.Unlock()

		if n := numTargetInserts; n > 0 {
			inserts.metrics.rotateInserts.Inc(int64(n))
		}
	}

	return prevWg
}
