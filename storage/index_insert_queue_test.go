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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3ninx/doc"
	xtime "github.com/m3db/m3x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func testWriteBatchEntry(ts time.Time, d doc.Document, fn index.OnIndexSeries) []index.WriteBatchEntry {
	return []index.WriteBatchEntry{
		index.WriteBatchEntry{
			BlockStart:    xtime.ToUnixNano(ts),
			Document:      d,
			OnIndexSeries: fn,
		},
	}
}

func newTestIndexInsertQueue() *nsIndexInsertQueue {
	var (
		nsIndexInsertBatchFn = func(inserts []index.WriteBatchEntry) {}
		nowFn                = time.Now
		scope                = tally.NoopScope
	)

	q := newNamespaceIndexInsertQueue(nsIndexInsertBatchFn, nowFn, scope).(*nsIndexInsertQueue)
	q.indexBatchBackoff = 10 * time.Millisecond
	return q
}

func testDoc(i int) doc.Document {
	return doc.Document{
		Fields: []doc.Field{
			doc.Field{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
		},
	}
}

func TestIndexInsertQueueStopBeforeStart(t *testing.T) {
	q := newTestIndexInsertQueue()
	assert.Error(t, q.Stop())

	q = newTestIndexInsertQueue()
	assert.NoError(t, q.Start())
	assert.Error(t, q.Start())
	assert.NoError(t, q.Stop())
	assert.Error(t, q.Stop())
	assert.Error(t, q.Start())
}

func TestIndexInsertQueueLifecycleLeaks(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()
	q := newTestIndexInsertQueue()
	assert.NoError(t, q.Start())
	assert.NoError(t, q.Stop())
}

func TestIndexInsertQueueCallback(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		q            = newTestIndexInsertQueue()
		insertLock   sync.Mutex
		insertedDocs []index.WriteBatchEntry
		callback     = index.NewMockOnIndexSeries(ctrl)
	)
	q.indexBatchFn = func(inserts []index.WriteBatchEntry) {
		insertLock.Lock()
		insertedDocs = append(insertedDocs, inserts...)
		insertLock.Unlock()
	}

	d := testDoc(1)
	assert.NoError(t, q.Start())
	defer q.Stop()

	now := time.Now()
	wg, err := q.InsertBatch(testWriteBatchEntry(now, d, callback))
	assert.NoError(t, err)
	wg.Wait()

	insertLock.Lock()
	defer insertLock.Unlock()
	assert.Len(t, insertedDocs, 1)
	assert.Equal(t, d, insertedDocs[0].Document)
	assert.Equal(t, now.UnixNano(), int64(insertedDocs[0].BlockStart))
}

func TestIndexInsertQueueRateLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	defer leaktest.CheckTimeout(t, time.Second)()
	var (
		currTime = time.Now().Truncate(time.Second)
		timeLock = sync.Mutex{}
		addTime  = func(d time.Duration) {
			timeLock.Lock()
			defer timeLock.Unlock()
			currTime = currTime.Add(d)
		}
	)
	q := newTestIndexInsertQueue()
	q.nowFn = func() time.Time {
		timeLock.Lock()
		defer timeLock.Unlock()
		return currTime
	}

	q.indexPerSecondLimit = 2
	callback := index.NewMockOnIndexSeries(ctrl)

	assert.NoError(t, q.Start())
	defer func() {
		assert.NoError(t, q.Stop())
	}()

	_, err := q.InsertBatch(testWriteBatchEntry(time.Time{}, testDoc(1), callback))
	assert.NoError(t, err)

	addTime(250 * time.Millisecond)
	_, err = q.InsertBatch(testWriteBatchEntry(time.Time{}, testDoc(2), callback))
	assert.NoError(t, err)

	// Consecutive should be all rate limited
	for i := 0; i < 100; i++ {
		_, err = q.InsertBatch(testWriteBatchEntry(time.Time{}, testDoc(i+2), callback))
		assert.Error(t, err)
		assert.Equal(t, errNewSeriesIndexRateLimitExceeded, err)
	}

	// Start 2nd second should not be an issue
	addTime(750 * time.Millisecond)
	_, err = q.InsertBatch(testWriteBatchEntry(time.Time{}, testDoc(110), callback))
	assert.NoError(t, err)

	addTime(100 * time.Millisecond)
	_, err = q.InsertBatch(testWriteBatchEntry(time.Time{}, testDoc(111), callback))
	assert.NoError(t, err)

	addTime(100 * time.Millisecond)
	_, err = q.InsertBatch(testWriteBatchEntry(time.Time{}, testDoc(112), callback))
	assert.Error(t, err)
	assert.Equal(t, errNewSeriesIndexRateLimitExceeded, err)

	// Start 3rd second
	addTime(800 * time.Millisecond)
	_, err = q.InsertBatch(testWriteBatchEntry(time.Time{}, testDoc(113), callback))
	assert.NoError(t, err)

	q.Lock()
	expectedCurrWindow := currTime.Truncate(time.Second).UnixNano()
	assert.Equal(t, expectedCurrWindow, q.indexPerSecondLimitWindowNanos)
	assert.Equal(t, 1, q.indexPerSecondLimitWindowValues)
	q.Unlock()
}

func TestIndexInsertQueueBatchBackoff(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		inserts  [][]index.WriteBatchEntry
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
	q := newTestIndexInsertQueue()
	q.nowFn = func() time.Time {
		timeLock.Lock()
		defer timeLock.Unlock()
		return currTime
	}
	q.indexBatchFn = func(value []index.WriteBatchEntry) {
		inserts = append(inserts, value)
		insertWgs[len(inserts)-1].Done()
		insertProgressWgs[len(inserts)-1].Wait()
	}

	q.indexBatchBackoff = backoff
	callback := index.NewMockOnIndexSeries(ctrl)

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
	_, err := q.InsertBatch(testWriteBatchEntry(time.Time{}, testDoc(0), callback))
	require.NoError(t, err)

	// wait for first insert batch to complete
	insertWgs[0].Wait()

	// now next batch will need to wait as we haven't progressed time
	_, err = q.InsertBatch(testWriteBatchEntry(time.Time{}, testDoc(1), callback))
	require.NoError(t, err)
	_, err = q.InsertBatch(testWriteBatchEntry(time.Time{}, testDoc(2), callback))
	require.NoError(t, err)

	// allow first insert to finish
	insertProgressWgs[0].Done()

	// wait for second batch to complete
	insertWgs[1].Wait()

	assert.Equal(t, backoff, slept)
	assert.Equal(t, 1, numSleeps)

	// insert third batch, will also need to wait
	_, err = q.InsertBatch(testWriteBatchEntry(time.Time{}, testDoc(3), callback))
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

	q := newNamespaceIndexInsertQueue(func(value []index.WriteBatchEntry) {
		atomic.AddInt64(&numInsertObserved, int64(len(value)))
	}, func() time.Time { return currTime }, tally.NoopScope)

	require.NoError(t, q.Start())

	for i := 0; i < numInsertExpected; i++ {
		_, err := q.InsertBatch(testWriteBatchEntry(time.Time{}, doc.Document{}, nil))
		require.NoError(t, err)
	}

	require.NoError(t, q.Stop())
	require.Equal(t, int64(numInsertExpected), atomic.LoadInt64(&numInsertObserved))
}
