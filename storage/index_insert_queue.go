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

package storage

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3ninx/doc"

	"github.com/uber-go/tally"
)

var (
	errIndexInsertQueueNotOpen             = errors.New("index insert queue is not open")
	errIndexInsertQueueAlreadyOpenOrClosed = errors.New("index insert queue already open or is closed")
	errNewSeriesIndexRateLimitExceeded     = errors.New("indexing new series eclipses rate limit")
)

type dbIndexInsertQueueState int

const (
	dbIndexInsertQueueStateNotOpen dbIndexInsertQueueState = iota
	dbIndexInsertQueueStateOpen
	dbIndexInsertQueueStateClosed
)

var (
	defaultIndexBatchBackoff   = time.Second
	defaultIndexPerSecondLimit = 10000
)

type dbIndexInsertQueue struct {
	sync.RWMutex
	state dbIndexInsertQueueState
	// rate limits
	indexBatchBackoff               time.Duration
	indexPerSecondLimit             int
	indexPerSecondLimitWindowNanos  int64
	indexPerSecondLimitWindowValues int
	// active batch pending execution
	currBatch *dbIndexInsertBatch

	indexBatchFn dbIndexInsertBatchFn

	nowFn        clock.NowFn
	sleepFn      func(time.Duration)
	notifyInsert chan struct{}

	metrics dbIndexInsertQueueMetrics
}

type newDatabaseIndexInsertQueueFn func(
	dbIndexInsertBatchFn, clock.NowFn, tally.Scope) databaseIndexInsertQueue

// FOLLOWUP(prateek): subsequent PR to wire up rate limiting to runtime.Options
// nolint: deadcode
func newDatabaseIndexInsertQueue(
	indexBatchFn dbIndexInsertBatchFn,
	nowFn clock.NowFn,
	scope tally.Scope,
) databaseIndexInsertQueue {
	currBatch := &dbIndexInsertBatch{}
	currBatch.Reset()
	return &dbIndexInsertQueue{
		currBatch:           currBatch,
		indexBatchBackoff:   defaultIndexBatchBackoff,
		indexPerSecondLimit: defaultIndexPerSecondLimit,
		indexBatchFn:        indexBatchFn,
		nowFn:               nowFn,
		sleepFn:             time.Sleep,
		notifyInsert:        make(chan struct{}, 1),
		metrics:             newDatabaseIndexInsertQueueMetrics(scope),
	}
}

func (q *dbIndexInsertQueue) insertLoop() {
	var lastInsert time.Time
	freeBatch := &dbIndexInsertBatch{}
	freeBatch.Reset()
	for range q.notifyInsert {
		// Check if inserting too fast
		elapsedSinceLastInsert := q.nowFn().Sub(lastInsert)

		// Rotate batches
		var (
			state   dbIndexInsertQueueState
			backoff time.Duration
			batch   *dbIndexInsertBatch
		)
		q.Lock()
		state = q.state
		if elapsedSinceLastInsert < q.indexBatchBackoff {
			// Need to backoff before rotate and insert
			backoff = q.indexBatchBackoff - elapsedSinceLastInsert
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
			q.indexBatchFn(batch.inserts)
		}
		batch.wg.Done()

		// Set the free batch
		batch.Reset()
		freeBatch = batch

		lastInsert = q.nowFn()

		if state != dbIndexInsertQueueStateOpen {
			return // Break if the queue closed
		}
	}
}

func (q *dbIndexInsertQueue) Insert(
	d doc.Document,
	fns indexInsertLifecycleHooks,
) (*sync.WaitGroup, error) {
	windowNanos := q.nowFn().Truncate(time.Second).UnixNano()

	q.Lock()
	if q.state != dbIndexInsertQueueStateOpen {
		q.Unlock()
		return nil, errIndexInsertQueueNotOpen
	}
	if limit := q.indexPerSecondLimit; limit > 0 {
		if q.indexPerSecondLimitWindowNanos != windowNanos {
			// Rolled into to a new window
			q.indexPerSecondLimitWindowNanos = windowNanos
			q.indexPerSecondLimitWindowValues = 0
		}
		q.indexPerSecondLimitWindowValues++
		if q.indexPerSecondLimitWindowValues > limit {
			q.Unlock()
			return nil, errNewSeriesIndexRateLimitExceeded
		}
	}
	q.currBatch.inserts = append(q.currBatch.inserts, dbIndexInsert{
		doc: d,
		fns: fns,
	})
	wg := q.currBatch.wg
	q.Unlock()

	// Notify insert loop
	select {
	case q.notifyInsert <- struct{}{}:
	default:
		// Loop busy, already ready to consume notification
	}

	q.metrics.numPending.Inc(1)
	return wg, nil
}

func (q *dbIndexInsertQueue) Start() error {
	q.Lock()
	defer q.Unlock()

	if q.state != dbIndexInsertQueueStateNotOpen {
		return errIndexInsertQueueAlreadyOpenOrClosed
	}

	q.state = dbIndexInsertQueueStateOpen
	go q.insertLoop()
	return nil
}

func (q *dbIndexInsertQueue) Stop() error {
	q.Lock()
	defer q.Unlock()

	if q.state != dbIndexInsertQueueStateOpen {
		return errIndexInsertQueueNotOpen
	}

	q.state = dbIndexInsertQueueStateClosed

	// Final flush
	select {
	case q.notifyInsert <- struct{}{}:
	default:
		// Loop busy, already ready to consume notification
	}

	return nil
}

type dbIndexInsertBatchFn func(inserts []dbIndexInsert) error

type dbIndexInsert struct {
	doc doc.Document
	fns indexInsertLifecycleHooks
}

type dbIndexInsertBatch struct {
	wg      *sync.WaitGroup
	inserts []dbIndexInsert
}

var dbIndexInsertZeroed dbIndexInsert

func (b *dbIndexInsertBatch) Reset() {
	b.wg = &sync.WaitGroup{}
	// We always expect to be waiting for an index
	b.wg.Add(1)
	for i := range b.inserts {
		b.inserts[i] = dbIndexInsertZeroed
	}
	b.inserts = b.inserts[:0]
}

type dbIndexInsertQueueMetrics struct {
	numPending tally.Counter
}

func newDatabaseIndexInsertQueueMetrics(
	scope tally.Scope,
) dbIndexInsertQueueMetrics {
	subScope := scope.SubScope("index-queue")
	return dbIndexInsertQueueMetrics{
		numPending: subScope.Counter("num-pending"),
	}
}
