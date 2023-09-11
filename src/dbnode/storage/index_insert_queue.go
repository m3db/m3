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
	"strconv"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	"github.com/m3db/m3/src/x/clock"
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

	indexResetAllInsertsEvery = 3 * time.Minute
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
	coreFn       xsync.CoreFn
	notifyInsert chan struct{}
	closeCh      chan struct{}

	scope tally.Scope

	metrics nsIndexInsertQueueMetrics
}

type newNamespaceIndexInsertQueueFn func(
	nsIndexInsertBatchFn, namespace.Metadata, clock.NowFn, xsync.CoreFn, tally.Scope) namespaceIndexInsertQueue

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
	coreFn xsync.CoreFn,
	scope tally.Scope,
) namespaceIndexInsertQueue {
	subscope := scope.SubScope("insert-queue")
	q := &nsIndexInsertQueue{
		namespaceMetadata: namespaceMetadata,
		indexBatchBackoff: defaultIndexBatchBackoff,
		indexBatchFn:      indexBatchFn,
		nowFn:             nowFn,
		sleepFn:           time.Sleep,
		coreFn:            coreFn,
		// NB(r): Use 2 * num cores so that each CPU insert queue which
		// is 1 per num CPU core can always enqueue a notification without
		// it being lost.
		notifyInsert: make(chan struct{}, 2*xsync.NumCores()),
		closeCh:      make(chan struct{}, 1),
		scope:        subscope,
		metrics:      newNamespaceIndexInsertQueueMetrics(subscope),
	}
	q.currBatch = q.newBatch(newBatchOptions{instrumented: true})
	return q
}

type newBatchOptions struct {
	instrumented bool
}

func (q *nsIndexInsertQueue) newBatch(opts newBatchOptions) *nsIndexInsertBatch {
	scope := tally.NoopScope
	if opts.instrumented {
		scope = q.scope
	}
	return newNsIndexInsertBatch(q.namespaceMetadata, q.nowFn, scope)
}

func (q *nsIndexInsertQueue) insertLoop() {
	defer func() {
		close(q.closeCh)
	}()

	var lastInsert time.Time
	batch := q.newBatch(newBatchOptions{})
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

		if state != nsIndexInsertQueueStateOpen {
			return // Break if the queue closed
		}
	}
}

func (q *nsIndexInsertQueue) InsertBatch(
	batch *index.WriteBatch,
) (*sync.WaitGroup, error) {
	batchLen := batch.Len()

	// Choose the queue relevant to current CPU index.
	// Note: since inserts by CPU core is allocated when
	// nsIndexInsertBatch is constructed and then never modified
	// it is safe to concurently read (but not modify obviously).
	inserts := q.currBatch.insertsByCPUCore[q.coreFn()]
	inserts.Lock()
	firstInsert := len(inserts.shardInserts) == 0
	inserts.shardInserts = append(inserts.shardInserts, batch)
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

	q.metrics.numPending.Inc(int64(batchLen))
	return wg, nil
}

func (q *nsIndexInsertQueue) InsertPending(
	pending []writes.PendingIndexInsert,
) (*sync.WaitGroup, error) {
	batchLen := len(pending)

	// Choose the queue relevant to current CPU index.
	// Note: since inserts by CPU core is allocated when
	// nsIndexInsertBatch is constructed and then never modified
	// it is safe to concurently read (but not modify obviously).
	inserts := q.currBatch.insertsByCPUCore[q.coreFn()]
	inserts.Lock()
	firstInsert := len(inserts.batchInserts) == 0
	inserts.batchInserts = append(inserts.batchInserts, pending...)
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
	namespace namespace.Metadata
	nowFn     clock.NowFn
	wg        *sync.WaitGroup
	// Note: since inserts by CPU core is allocated when
	// nsIndexInsertBatch is constructed and then never modified
	// it is safe to concurently read (but not modify obviously).
	insertsByCPUCore    []*nsIndexInsertsByCPUCore
	allInserts          *index.WriteBatch
	allInsertsLastReset time.Time
}

type nsIndexInsertsByCPUCore struct {
	sync.Mutex
	shardInserts []*index.WriteBatch
	batchInserts []writes.PendingIndexInsert
	wg           *sync.WaitGroup
	metrics      nsIndexInsertsByCPUCoreMetrics
}

type nsIndexInsertsByCPUCoreMetrics struct {
	rotateInsertsShard   tally.Counter
	rotateInsertsPending tally.Counter
}

func newNamespaceIndexInsertsByCPUCoreMetrics(
	cpuIndex int,
	scope tally.Scope,
) nsIndexInsertsByCPUCoreMetrics {
	scope = scope.Tagged(map[string]string{
		"cpu-index": strconv.Itoa(cpuIndex),
	})

	const rotate = "rotate-inserts"
	return nsIndexInsertsByCPUCoreMetrics{
		rotateInsertsShard: scope.Tagged(map[string]string{
			"rotate-type": "shard-insert",
		}).Counter(rotate),
		rotateInsertsPending: scope.Tagged(map[string]string{
			"rotate-type": "pending-insert",
		}).Counter(rotate),
	}
}

func newNsIndexInsertBatch(
	namespace namespace.Metadata,
	nowFn clock.NowFn,
	scope tally.Scope,
) *nsIndexInsertBatch {
	b := &nsIndexInsertBatch{
		namespace: namespace,
		nowFn:     nowFn,
	}
	numCores := xsync.NumCores()
	for i := 0; i < numCores; i++ {
		b.insertsByCPUCore = append(b.insertsByCPUCore, &nsIndexInsertsByCPUCore{
			metrics: newNamespaceIndexInsertsByCPUCoreMetrics(i, scope),
		})
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
	for _, inserts := range b.insertsByCPUCore {
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
	for idx, inserts := range b.insertsByCPUCore {
		if target == nil {
			// No target to rotate with.
			inserts.Lock()
			// Reset
			inserts.shardInserts = inserts.shardInserts[:0]
			inserts.batchInserts = inserts.batchInserts[:0]
			// Use new wait group.
			inserts.wg = b.wg
			inserts.Unlock()
			continue
		}

		// First prepare the target to take the current batch's inserts.
		targetInserts := target.insertsByCPUCore[idx]
		targetInserts.Lock()

		// Reset the target inserts since we'll take ref to them in a second.
		for i := range targetInserts.shardInserts {
			// TODO(prateek): if we start pooling `[]index.WriteBatchEntry`, then we could return to the pool here.
			targetInserts.shardInserts[i] = nil
		}
		prevTargetShardInserts := targetInserts.shardInserts[:0]

		// memset optimization
		var zero writes.PendingIndexInsert
		for i := range targetInserts.batchInserts {
			targetInserts.batchInserts[i] = zero
		}
		prevTargetBatchInserts := targetInserts.batchInserts[:0]

		// Lock the current batch inserts now ready to rotate to the target.
		inserts.Lock()

		// Update current slice refs to take target's inserts.
		targetInserts.shardInserts = inserts.shardInserts
		targetInserts.batchInserts = inserts.batchInserts
		targetInserts.wg = inserts.wg

		// Reuse the target's old slices.
		inserts.shardInserts = prevTargetShardInserts
		inserts.batchInserts = prevTargetBatchInserts

		// Use new wait group.
		inserts.wg = b.wg

		// Unlock as early as possible for writes to keep enqueuing.
		inserts.Unlock()

		numTargetInsertsShard := len(targetInserts.shardInserts)
		numTargetInsertsPending := len(targetInserts.batchInserts)

		// Now can unlock target inserts too.
		targetInserts.Unlock()

		if n := numTargetInsertsShard; n > 0 {
			inserts.metrics.rotateInsertsShard.Inc(int64(n))
		}
		if n := numTargetInsertsPending; n > 0 {
			inserts.metrics.rotateInsertsPending.Inc(int64(n))
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
