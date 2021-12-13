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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func newTestIndexInsertQueue(
	namespace namespace.Metadata,
) *nsIndexInsertQueue {
	var (
		nsIndexInsertBatchFn = func(inserts *index.WriteBatch) {}
		nowFn                = time.Now
		coreFn               = xsync.CPUCore
		scope                = tally.NoopScope
	)

	q := newNamespaceIndexInsertQueue(nsIndexInsertBatchFn,
		namespace, nowFn, coreFn, scope).(*nsIndexInsertQueue)
	q.indexBatchBackoff = 10 * time.Millisecond
	return q
}

func testID(i int) ident.ID {
	return ident.StringID(fmt.Sprintf("foo%d", i))
}

func testTags(i int) ident.Tags {
	return ident.NewTags(ident.Tag{Name: testID(i), Value: testID(i)})
}

func TestIndexInsertQueueStopBeforeStart(t *testing.T) {
	q := newTestIndexInsertQueue(newTestNamespaceMetadata(t))
	assert.Error(t, q.Stop())

	q = newTestIndexInsertQueue(newTestNamespaceMetadata(t))
	assert.NoError(t, q.Start())
	assert.Error(t, q.Start())
	assert.NoError(t, q.Stop())
	assert.Error(t, q.Stop())
	assert.Error(t, q.Start())
}

func TestIndexInsertQueueLifecycleLeaks(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()
	q := newTestIndexInsertQueue(newTestNamespaceMetadata(t))
	assert.NoError(t, q.Start())
	assert.NoError(t, q.Stop())
}

func TestIndexInsertQueueCallback(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		q               = newTestIndexInsertQueue(newTestNamespaceMetadata(t))
		insertLock      sync.Mutex
		insertedBatches []*index.WriteBatch
		callback        = doc.NewMockOnIndexSeries(ctrl)
	)
	q.indexBatchFn = func(inserts *index.WriteBatch) {
		insertLock.Lock()
		insertedBatches = append(insertedBatches, inserts)
		insertLock.Unlock()
	}

	assert.NoError(t, q.Start())
	defer q.Stop()

	now := xtime.Now()
	batch := index.NewWriteBatch(index.WriteBatchOptions{
		WriteBatchMetrics: index.NewWriteBatchMetrics(tally.NoopScope),
	})
	batch.Append(testWriteBatchEntry(testID(1), testTags(1), now, callback))
	wg, err := q.InsertBatch(batch)
	assert.NoError(t, err)
	wg.Wait()

	insertLock.Lock()
	defer insertLock.Unlock()
	assert.Len(t, insertedBatches, 1)
	assert.Equal(t, 1, insertedBatches[0].Len())
	assert.Equal(t, testID(1).Bytes(), insertedBatches[0].PendingDocs()[0].ID)
	assert.Equal(t, now, insertedBatches[0].PendingEntries()[0].Timestamp)
}

func TestIndexInsertQueueBatchBackoff(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	var (
		inserts  []*index.WriteBatch
		currTime = time.Now()
		timeLock = sync.Mutex{}
		addTime  = func(d time.Duration) {
			timeLock.Lock()
			defer timeLock.Unlock()
			currTime = currTime.Add(d)
		}
		backoff           = 10 * time.Millisecond
		insertWgs         [3]sync.WaitGroup
		insertProgressWgs [3]sync.WaitGroup
	)
	for i := range insertWgs {
		insertWgs[i].Add(1)
	}
	for i := range insertProgressWgs {
		insertProgressWgs[i].Add(1)
	}
	q := newTestIndexInsertQueue(newTestNamespaceMetadata(t))
	q.nowFn = func() time.Time {
		timeLock.Lock()
		defer timeLock.Unlock()
		return currTime
	}
	q.indexBatchFn = func(values *index.WriteBatch) {
		inserts = append(inserts, values)
		insertWgs[len(inserts)-1].Done()
		insertProgressWgs[len(inserts)-1].Wait()
	}

	q.indexBatchBackoff = backoff
	callback := doc.NewMockOnIndexSeries(ctrl)

	var slept time.Duration
	var numSleeps int
	q.sleepFn = func(d time.Duration) {
		assert.Equal(t, backoff, d)

		slept += d
		numSleeps++
		addTime(d)
	}

	require.NoError(t, q.Start())
	defer func() {
		require.NoError(t, q.Stop())
	}()

	// first insert
	_, err := q.InsertBatch(testWriteBatch(testWriteBatchEntry(testID(0),
		testTags(0), 0, callback)))
	require.NoError(t, err)

	// wait for first insert batch to complete
	insertWgs[0].Wait()

	// now next batch will need to wait as we haven't progressed time
	_, err = q.InsertBatch(testWriteBatch(testWriteBatchEntry(testID(1),
		testTags(1), 0, callback)))
	require.NoError(t, err)
	_, err = q.InsertBatch(testWriteBatch(testWriteBatchEntry(testID(2),
		testTags(2), 0, callback)))
	require.NoError(t, err)

	// allow first insert to finish
	insertProgressWgs[0].Done()

	// wait for second batch to complete
	insertWgs[1].Wait()

	assert.Equal(t, backoff, slept)
	assert.Equal(t, 1, numSleeps)

	// insert third batch, will also need to wait
	_, err = q.InsertBatch(testWriteBatch(testWriteBatchEntry(testID(3),
		testTags(3), 0, callback)))
	require.NoError(t, err)

	// allow second batch to finish
	insertProgressWgs[1].Done()

	// wait for third batch to complete
	insertWgs[2].Wait()

	assert.Equal(t, 2*backoff, slept)
	assert.Equal(t, 2, numSleeps)

	assert.Equal(t, 3, len(inserts))

	// allow third batch to complete
	insertProgressWgs[2].Done()
}

func TestIndexInsertQueueFlushedOnClose(t *testing.T) {
	defer leaktest.CheckTimeout(t, 5*time.Second)()

	var (
		numInsertExpected = 10
		numInsertObserved int64
		currTime          = time.Now().Truncate(time.Second)
	)

	q := newNamespaceIndexInsertQueue(
		func(values *index.WriteBatch) {
			atomic.AddInt64(&numInsertObserved, int64(values.Len()))
		},
		newTestNamespaceMetadata(t),
		func() time.Time {
			return currTime
		},
		xsync.CPUCore,
		tally.NoopScope)

	require.NoError(t, q.Start())

	for i := 0; i < numInsertExpected; i++ {
		_, err := q.InsertBatch(testWriteBatch(testWriteBatchEntry(testID(1),
			testTags(1), 0, nil)))
		require.NoError(t, err)
	}

	require.NoError(t, q.Stop())
	require.Equal(t, int64(numInsertExpected), atomic.LoadInt64(&numInsertObserved))
}
