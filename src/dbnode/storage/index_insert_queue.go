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

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/storage/index"

	"github.com/uber-go/tally"
)

var (
	errIndexInsertQueueNotOpen             = errors.New("index insert queue is not open")
	errIndexInsertQueueAlreadyOpenOrClosed = errors.New("index insert queue already open or is closed")
	errNewSeriesIndexRateLimitExceeded     = errors.New("indexing new series exceeds rate limit")
)

type nsIndexInsertQueueState int

const (
	nsIndexInsertQueueStateNotOpen nsIndexInsertQueueState = iota
	nsIndexInsertQueueStateOpen
	nsIndexInsertQueueStateClosed
)

var (
	// TODO(prateek): runtime options for this stuff
	defaultIndexBatchBackoff   = time.Millisecond
	defaultIndexPerSecondLimit = 1000000
)

type nsIndexInsertQueue struct {
	sync.RWMutex
	state nsIndexInsertQueueState

	// rate limits
	indexBatchBackoff               time.Duration
	indexPerSecondLimit             int
	indexPerSecondLimitWindowNanos  int64
	indexPerSecondLimitWindowValues int

	// active batch pending execution
	currBatch *nsIndexInsertBatch

	indexBatchFn nsIndexInsertBatchFn
	nowFn        clock.NowFn
	sleepFn      func(time.Duration)
	notifyInsert chan struct{}
	closeCh      chan struct{}

	metrics nsIndexInsertQueueMetrics
}

type newNamespaceIndexInsertQueueFn func(
	nsIndexInsertBatchFn, clock.NowFn, tally.Scope) namespaceIndexInsertQueue

// FOLLOWUP(prateek): subsequent PR to wire up rate limiting to runtime.Options
func newNamespaceIndexInsertQueue(
	indexBatchFn nsIndexInsertBatchFn,
	nowFn clock.NowFn,
	scope tally.Scope,
) namespaceIndexInsertQueue {
	currBatch := &nsIndexInsertBatch{}
	currBatch.Reset()
	subscope := scope.SubScope("insert-queue")
	return &nsIndexInsertQueue{
		currBatch:           currBatch,
		indexBatchBackoff:   defaultIndexBatchBackoff,
		indexPerSecondLimit: defaultIndexPerSecondLimit,
		indexBatchFn:        indexBatchFn,
		nowFn:               nowFn,
		sleepFn:             time.Sleep,
		notifyInsert:        make(chan struct{}, 1),
		closeCh:             make(chan struct{}, 1),
		metrics:             newNamespaceIndexInsertQueueMetrics(subscope),
	}
}

func (q *nsIndexInsertQueue) insertLoop() {
	defer func() {
		close(q.closeCh)
	}()

	var lastInsert time.Time
	freeBatch := &nsIndexInsertBatch{}
	freeBatch.Reset()
	for range q.notifyInsert {
		// Check if inserting too fast
		elapsedSinceLastInsert := q.nowFn().Sub(lastInsert)

		// Rotate batches
		var (
			state   nsIndexInsertQueueState
			backoff time.Duration
			batch   *nsIndexInsertBatch
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

		if state != nsIndexInsertQueueStateOpen {
			return // Break if the queue closed
		}
	}
}

func (q *nsIndexInsertQueue) InsertBatch(
	batch *index.WriteBatch,
) (*sync.WaitGroup, error) {
	windowNanos := q.nowFn().Truncate(time.Second).UnixNano()

	q.Lock()
	if q.state != nsIndexInsertQueueStateOpen {
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
	q.currBatch.inserts = append(q.currBatch.inserts, batch)
	batchLen := batch.Len()
	numBatches := len(q.currBatch.inserts)
	wg := q.currBatch.wg
	q.Unlock()

	// Notify insert loop
	select {
	case q.notifyInsert <- struct{}{}:
	default:
		// Loop busy, already ready to consume notification
	}

	q.metrics.numPendingDocs.Inc(int64(batchLen))
	q.metrics.numPendingBatches.Update(float64(numBatches))
	return wg, nil
}

func (q *nsIndexInsertQueue) Start() error {
	q.Lock()
	defer q.Unlock()

	if q.state != nsIndexInsertQueueStateNotOpen {
		return errIndexInsertQueueAlreadyOpenOrClosed
	}

	q.state = nsIndexInsertQueueStateOpen
	go q.insertLoop()
	return nil
}

func (q *nsIndexInsertQueue) Stop() error {
	q.Lock()

	if q.state != nsIndexInsertQueueStateOpen {
		q.Unlock()
		return errIndexInsertQueueNotOpen
	}

	q.state = nsIndexInsertQueueStateClosed
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

type nsIndexInsertBatchFn func(inserts []*index.WriteBatch)

type nsIndexInsertBatch struct {
	wg      *sync.WaitGroup
	inserts []*index.WriteBatch
}

func (b *nsIndexInsertBatch) Reset() {
	b.wg = &sync.WaitGroup{}
	// We always expect to be waiting for an index
	b.wg.Add(1)
	for i := range b.inserts {
		// TODO(prateek): if we start pooling `[]index.WriteBatchEntry`, then we could return to the pool here.
		b.inserts[i] = nil
	}
	b.inserts = b.inserts[:0]
}

type nsIndexInsertQueueMetrics struct {
	numPendingDocs    tally.Counter
	numPendingBatches tally.Gauge
}

func newNamespaceIndexInsertQueueMetrics(
	scope tally.Scope,
) nsIndexInsertQueueMetrics {
	subScope := scope.SubScope("index-queue")
	return nsIndexInsertQueueMetrics{
		numPendingDocs:    subScope.Counter("num-pending-docs"),
		numPendingBatches: subScope.Gauge("num-pending-batches"),
	}
}
