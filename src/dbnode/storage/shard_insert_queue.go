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
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/storage/series/lookup"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
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

	// rate limits, protected by mutex
	insertBatchBackoff   time.Duration
	insertPerSecondLimit int

	insertPerSecondLimitWindowNanos  int64
	insertPerSecondLimitWindowValues int

	currBatch    *dbShardInsertBatch
	notifyInsert chan struct{}
	closeCh      chan struct{}

	metrics dbShardInsertQueueMetrics
}

type dbShardInsertQueueMetrics struct {
	insertsNoPendingWrite tally.Counter
	insertsPendingWrite   tally.Counter
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
	}
}

type dbShardInsertBatch struct {
	wg      *sync.WaitGroup
	inserts []dbShardInsert
}

type dbShardInsertAsyncOptions struct {
	pendingWrite          dbShardPendingWrite
	pendingRetrievedBlock dbShardPendingRetrievedBlock
	pendingIndex          dbShardPendingIndex

	hasPendingWrite          bool
	hasPendingRetrievedBlock bool
	hasPendingIndexing       bool

	// NB(prateek): `entryRefCountIncremented` indicates if the
	// entry provided along with the dbShardInsertAsyncOptions
	// already has it's ref count incremented. It's used to
	// correctly manage the lifecycle of the entry across the
	// shard -> shard Queue -> shard boundaries.
	entryRefCountIncremented bool
}

type dbShardInsert struct {
	entry *lookup.Entry
	opts  dbShardInsertAsyncOptions
}

var dbShardInsertZeroed = dbShardInsert{}

type dbShardPendingWrite struct {
	timestamp  time.Time
	value      float64
	unit       xtime.Unit
	annotation []byte
	wopts      series.WriteOptions
}

type dbShardPendingIndex struct {
	timestamp  time.Time
	enqueuedAt time.Time
}

type dbShardPendingRetrievedBlock struct {
	id      ident.ID
	tags    ident.TagIterator
	start   time.Time
	segment ts.Segment
}

func (b *dbShardInsertBatch) reset() {
	b.wg = &sync.WaitGroup{}
	// We always expect to be waiting for an insert
	b.wg.Add(1)
	for i := range b.inserts {
		b.inserts[i] = dbShardInsertZeroed
	}
	b.inserts = b.inserts[:0]
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
	scope tally.Scope,
) *dbShardInsertQueue {
	currBatch := &dbShardInsertBatch{}
	currBatch.reset()
	subscope := scope.SubScope("insert-queue")
	return &dbShardInsertQueue{
		nowFn:              nowFn,
		insertEntryBatchFn: insertEntryBatchFn,
		sleepFn:            time.Sleep,
		currBatch:          currBatch,
		notifyInsert:       make(chan struct{}, 1),
		closeCh:            make(chan struct{}, 1),
		metrics:            newDatabaseShardInsertQueueMetrics(subscope),
	}
}

func (q *dbShardInsertQueue) SetRuntimeOptions(value runtime.Options) {
	q.Lock()
	q.insertBatchBackoff = value.WriteNewSeriesBackoffDuration()
	q.insertPerSecondLimit = value.WriteNewSeriesLimitPerShardPerSecond()
	q.Unlock()
}

func (q *dbShardInsertQueue) insertLoop() {
	defer func() {
		close(q.closeCh)
	}()

	var lastInsert time.Time
	freeBatch := &dbShardInsertBatch{}
	freeBatch.reset()
	for range q.notifyInsert {
		// Check if inserting too fast
		elapsedSinceLastInsert := q.nowFn().Sub(lastInsert)

		// Rotate batches
		var (
			state   dbShardInsertQueueState
			backoff time.Duration
			batch   *dbShardInsertBatch
		)
		q.Lock()
		state = q.state
		if elapsedSinceLastInsert < q.insertBatchBackoff {
			// Need to backoff before rotate and insert
			backoff = q.insertBatchBackoff - elapsedSinceLastInsert
		} else {
			// No backoff required, rotate and go
			batch = q.currBatch
			q.currBatch = freeBatch
		}
		q.Unlock()

		if backoff > 0 {
			q.sleepFn(backoff)
			q.Lock()
			// Rotate after backoff
			batch = q.currBatch
			q.currBatch = freeBatch
			q.Unlock()
		}

		if len(batch.inserts) > 0 {
			q.insertEntryBatchFn(batch.inserts)
		}
		batch.wg.Done()

		// Set the free batch
		batch.reset()
		freeBatch = batch

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

	// Final flush
	select {
	case q.notifyInsert <- struct{}{}:
	default:
		// Loop busy, already ready to consume notification
	}

	// wait till other go routine is done
	<-q.closeCh

	return nil
}

func (q *dbShardInsertQueue) Insert(insert dbShardInsert) (*sync.WaitGroup, error) {
	windowNanos := q.nowFn().Truncate(time.Second).UnixNano()

	q.Lock()
	if q.state != dbShardInsertQueueStateOpen {
		q.Unlock()
		return nil, errShardInsertQueueNotOpen
	}
	if limit := q.insertPerSecondLimit; limit > 0 {
		if q.insertPerSecondLimitWindowNanos != windowNanos {
			// Rolled into to a new window
			q.insertPerSecondLimitWindowNanos = windowNanos
			q.insertPerSecondLimitWindowValues = 0
		}
		q.insertPerSecondLimitWindowValues++
		if q.insertPerSecondLimitWindowValues > limit {
			q.Unlock()
			return nil, errNewSeriesInsertRateLimitExceeded
		}
	}
	q.currBatch.inserts = append(q.currBatch.inserts, insert)
	wg := q.currBatch.wg
	q.Unlock()

	// Notify insert loop
	select {
	case q.notifyInsert <- struct{}{}:
	default:
		// Loop busy, already ready to consume notification
	}

	if insert.opts.hasPendingWrite {
		q.metrics.insertsPendingWrite.Inc(1)
	} else {
		q.metrics.insertsNoPendingWrite.Inc(1)
	}

	return wg, nil
}
