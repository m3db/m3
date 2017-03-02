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
)

var (
	errShardInsertQueueNotOpen             = errors.New("shard insert queue is not open")
	errShardInsertQueueAlreadyOpenOrClosed = errors.New("shard insert queue already open or is closed")
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
	insertEntryBatchFn dbShardInsertEntryBatchFn
	insertBatchBackoff time.Duration

	currBatch    *dbShardInsertBatch
	notifyInsert chan struct{}
}

type dbShardInsertBatch struct {
	wg      *sync.WaitGroup
	entries []*dbShardEntry
}

func (b *dbShardInsertBatch) reset() {
	b.wg = &sync.WaitGroup{}
	// We always expect to be waiting for an insert
	b.wg.Add(1)
	for i := range b.entries {
		b.entries[i] = nil
	}
	b.entries = b.entries[:0]
}

type dbShardInsertEntryBatchFn func(entries []*dbShardEntry) error

// newDbShardInsertQueue creates a new shard insert queue. The shard
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
func newDbShardInsertQueue(
	insertEntryBatchFn dbShardInsertEntryBatchFn,
	opts Options,
) *dbShardInsertQueue {
	currBatch := &dbShardInsertBatch{}
	currBatch.reset()
	return &dbShardInsertQueue{
		insertEntryBatchFn: insertEntryBatchFn,
		currBatch:          currBatch,
		notifyInsert:       make(chan struct{}, 1),
	}
}

func (q *dbShardInsertQueue) insertLoop() {
	freeBatch := &dbShardInsertBatch{}
	freeBatch.reset()
	for range q.notifyInsert {
		// Rotate batches
		q.Lock()
		batch := q.currBatch
		q.currBatch = freeBatch
		q.Unlock()

		q.insertEntryBatchFn(batch.entries)
		batch.wg.Done()

		// Set the free batch
		batch.reset()
		freeBatch = batch
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
	defer q.Unlock()

	if q.state != dbShardInsertQueueStateOpen {
		return errShardInsertQueueNotOpen
	}

	q.state = dbShardInsertQueueStateClosed

	// Final flush
	select {
	case q.notifyInsert <- struct{}{}:
	default:
		// Loop busy, already ready to consume notification
	}
	close(q.notifyInsert)

	return nil
}

func (q *dbShardInsertQueue) Insert(entry *dbShardEntry) (*sync.WaitGroup, error) {
	q.Lock()
	if q.state != dbShardInsertQueueStateOpen {
		q.Unlock()
		return nil, errShardInsertQueueNotOpen
	}
	q.currBatch.entries = append(q.currBatch.entries, entry)
	wg := q.currBatch.wg
	q.Unlock()

	// Notify insert loop
	select {
	case q.notifyInsert <- struct{}{}:
	default:
		// Loop busy, already ready to consume notification
	}

	return wg, nil
}
