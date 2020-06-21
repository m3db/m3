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
	"runtime"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/uber-go/tally"
)

var (
	errIndexInsertQueueNotOpen             = errors.New("index insert queue is not open")
	errIndexInsertQueueAlreadyOpenOrClosed = errors.New("index insert queue already open or is closed")
)

type nsIndexInsertQueueState int

const (
	nsIndexInsertQueueStateNotOpen nsIndexInsertQueueState = iota
	nsIndexInsertQueueStateOpen
	nsIndexInsertQueueStateClosed

	// TODO(prateek): runtime options for this stuff
	defaultIndexBatchBackoff = 2 * time.Millisecond

	indexResetAllInsertsEvery = 30 * time.Second
)

type nsIndexInsertQueue struct {
	sync.RWMutex

	namespaceMetadata namespace.Metadata

	state nsIndexInsertQueueState

	// rate limits
	indexBatchBackoff time.Duration

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
	nsIndexInsertBatchFn, namespace.Metadata, clock.NowFn, tally.Scope) namespaceIndexInsertQueue

// newNamespaceIndexInsertQueue returns a new index insert queue.
// Note: No limit appears on the index insert queue since any items making
// it into the index insert queue must first pass through the shard insert
// queue which has it's own limits in place.
// Any error returned from this queue would cause the series to not be indexed
// and there is no way to return this error to the client over the network
// (unlike the shard insert queue at which point if an error is returned
// is returned all the way back to the DB node client).
// FOLLOWUP(prateek): subsequent PR to wire up rate limiting to runtime.Options
func newNamespaceIndexInsertQueue(
	indexBatchFn nsIndexInsertBatchFn,
	namespaceMetadata namespace.Metadata,
	nowFn clock.NowFn,
	scope tally.Scope,
) namespaceIndexInsertQueue {
	subscope := scope.SubScope("insert-queue")
	q := &nsIndexInsertQueue{
		namespaceMetadata: namespaceMetadata,
		indexBatchBackoff: defaultIndexBatchBackoff,
		indexBatchFn:      indexBatchFn,
		nowFn:             nowFn,
		sleepFn:           time.Sleep,
		notifyInsert:      make(chan struct{}, 1),
		closeCh:           make(chan struct{}, 1),
		metrics:           newNamespaceIndexInsertQueueMetrics(subscope),
	}
	q.currBatch = q.newBatch()
	return q
}

func (q *nsIndexInsertQueue) newBatch() *nsIndexInsertBatch {
	return newNsIndexInsertBatch(q.namespaceMetadata, q.nowFn)
}

func (q *nsIndexInsertQueue) insertLoop() {
	defer func() {
		close(q.closeCh)
	}()

	var lastInsert time.Time
	batch := q.newBatch()
	for range q.notifyInsert {
		// Check if inserting too fast
		elapsedSinceLastInsert := q.nowFn().Sub(lastInsert)

		// Rotate batches
		var (
			state   nsIndexInsertQueueState
			backoff time.Duration
		)
		q.Lock()
		state = q.state
		if elapsedSinceLastInsert < q.indexBatchBackoff {
			// Need to backoff before rotate and insert
			backoff = q.indexBatchBackoff - elapsedSinceLastInsert
		}
		q.Unlock()
		if state != nsIndexInsertQueueStateOpen {
			return // Break if the queue closed
		}

		if backoff > 0 {
			q.sleepFn(backoff)
		}

		// Rotate after backoff
		batchWg := q.currBatch.Rotate(batch)

		all := batch.AllInserts()
		if all.Len() > 0 {
			q.indexBatchFn(all)
		}

		batchWg.Done()

		lastInsert = q.nowFn()
	}
}

func (q *nsIndexInsertQueue) rotate(target *nsIndexInsertBatch) {

}

func (q *nsIndexInsertQueue) InsertBatch(
	batch *index.WriteBatch,
) (*sync.WaitGroup, error) {
	batchLen := batch.Len()

	// Choose the queue relevant to current CPU index
	inserts := q.currBatch.insertsByCPU[xsync.IndexCPU()]
	inserts.Lock()
	inserts.shardInserts = append(inserts.shardInserts, batch)
	wg := inserts.wg
	inserts.Unlock()

	// Notify insert loop
	select {
	case q.notifyInsert <- struct{}{}:
	default:
		// Loop busy, already ready to consume notification
	}

	q.metrics.numPending.Inc(int64(batchLen))
	return wg, nil
}

func (q *nsIndexInsertQueue) InsertPending(
	pending []writes.PendingIndexInsert,
) (*sync.WaitGroup, error) {
	batchLen := len(pending)

	// Choose the queue relevant to current CPU index
	inserts := q.currBatch.insertsByCPU[xsync.IndexCPU()]
	inserts.Lock()
	inserts.batchInserts = append(inserts.batchInserts, pending...)
	wg := inserts.wg
	inserts.Unlock()

	// Notify insert loop
	select {
	case q.notifyInsert <- struct{}{}:
	default:
		// Loop busy, already ready to consume notification
	}

	q.metrics.numPending.Inc(int64(batchLen))
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

type nsIndexInsertBatchFn func(inserts *index.WriteBatch)

type nsIndexInsertBatch struct {
	namespace           namespace.Metadata
	nowFn               clock.NowFn
	wg                  *sync.WaitGroup
	insertsByCPU        []*insertsByCPU
	allInserts          *index.WriteBatch
	allInsertsLastReset time.Time
}

type insertsByCPU struct {
	sync.Mutex
	shardInserts []*index.WriteBatch
	batchInserts []writes.PendingIndexInsert
	wg           *sync.WaitGroup
}

func newNsIndexInsertBatch(
	namespace namespace.Metadata,
	nowFn clock.NowFn,
) *nsIndexInsertBatch {
	b := &nsIndexInsertBatch{
		namespace: namespace,
		nowFn:     nowFn,
	}
	numCPU := runtime.NumCPU()
	for i := 0; i < numCPU; i++ {
		b.insertsByCPU = append(b.insertsByCPU, &insertsByCPU{})
	}
	b.allocateAllInserts()
	b.Rotate(nil)
	return b
}

func (b *nsIndexInsertBatch) allocateAllInserts() {
	b.allInserts = index.NewWriteBatch(index.WriteBatchOptions{
		IndexBlockSize: b.namespace.Options().IndexOptions().BlockSize(),
	})
	b.allInsertsLastReset = b.nowFn()
}

func (b *nsIndexInsertBatch) AllInserts() *index.WriteBatch {
	b.allInserts.Reset()
	for _, inserts := range b.insertsByCPU {
		inserts.Lock()
		for _, shardInserts := range inserts.shardInserts {
			b.allInserts.AppendAll(shardInserts)
		}
		for _, insert := range inserts.batchInserts {
			b.allInserts.Append(insert.Entry, insert.Document)
		}
		inserts.Unlock()
	}
	return b.allInserts
}

func (b *nsIndexInsertBatch) Rotate(target *nsIndexInsertBatch) *sync.WaitGroup {
	prevWg := b.wg

	// We always expect to be waiting for an index.
	b.wg = &sync.WaitGroup{}
	b.wg.Add(1)

	// Rotate to target if we need to.
	if target != nil {
		for idx, inserts := range b.insertsByCPU {
			targetInserts := target.insertsByCPU[idx]
			targetInserts.Lock()

			// Reset the target inserts since we'll take ref to them in a second.
			for i := range targetInserts.shardInserts {
				// TODO(prateek): if we start pooling `[]index.WriteBatchEntry`, then we could return to the pool here.
				targetInserts.shardInserts[i] = nil
			}
			prevTargetInserts := targetInserts.shardInserts[:0]

			// memset optimization
			var zero writes.PendingIndexInsert
			for i := range targetInserts.batchInserts {
				targetInserts.batchInserts[i] = zero
			}
			prevTargetBatchInserts := targetInserts.batchInserts[:0]

			inserts.Lock()

			// Copy current slices to target.
			targetInserts.shardInserts = inserts.shardInserts
			targetInserts.batchInserts = inserts.batchInserts
			targetInserts.wg = inserts.wg

			// Reuse the target's old slices.
			inserts.shardInserts = prevTargetInserts
			inserts.batchInserts = prevTargetBatchInserts

			// Use new wait group.
			inserts.wg = b.wg

			inserts.Unlock()

			targetInserts.Unlock()
		}
	}

	if b.nowFn().Sub(b.allInsertsLastReset) > indexResetAllInsertsEvery {
		// NB(r): Sometimes this can grow very high, so we reset it relatively frequently
		b.allocateAllInserts()
	}

	return prevWg
}

type nsIndexInsertQueueMetrics struct {
	numPending tally.Counter
}

func newNamespaceIndexInsertQueueMetrics(
	scope tally.Scope,
) nsIndexInsertQueueMetrics {
	subScope := scope.SubScope("index-queue")
	return nsIndexInsertQueueMetrics{
		numPending: subScope.Counter("num-pending"),
	}
}
