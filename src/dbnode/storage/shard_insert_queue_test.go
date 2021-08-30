// Copyright (c) 2016 Uber Technologies, Inc.
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

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

func TestShardInsertQueueBatchBackoff(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second)()

	var (
		inserts  [][]dbShardInsert
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
	q := newDatabaseShardInsertQueue(func(value []dbShardInsert) error {
		if len(inserts) == len(insertWgs) {
			return nil // Overflow.
		}

		inserts = append(inserts, value)
		insertWgs[len(inserts)-1].Done()
		insertProgressWgs[len(inserts)-1].Wait()
		return nil
	}, func() time.Time {
		timeLock.Lock()
		defer timeLock.Unlock()
		return currTime
	}, tally.NoopScope, zap.NewNop())

	q.insertBatchBackoff = backoff

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
	_, err := q.Insert(dbShardInsert{entry: &Entry{Index: 0}})
	require.NoError(t, err)

	// wait for first insert batch to complete
	insertWgs[0].Wait()

	// now next batch will need to wait as we haven't progressed time
	_, err = q.Insert(dbShardInsert{entry: &Entry{Index: 1}})
	require.NoError(t, err)
	_, err = q.Insert(dbShardInsert{entry: &Entry{Index: 2}})
	require.NoError(t, err)

	// allow first insert to finish
	insertProgressWgs[0].Done()

	// wait for second batch to complete
	insertWgs[1].Wait()

	assert.Equal(t, backoff, slept)
	assert.Equal(t, 1, numSleeps)

	// insert third batch, will also need to wait
	_, err = q.Insert(dbShardInsert{entry: &Entry{Index: 3}})
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

func TestShardInsertQueueRateLimit(t *testing.T) {
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
	q := newDatabaseShardInsertQueue(func(value []dbShardInsert) error {
		return nil
	}, func() time.Time {
		timeLock.Lock()
		defer timeLock.Unlock()
		return currTime
	}, tally.NoopScope, zap.NewNop())

	q.insertPerSecondLimit.Store(2)

	require.NoError(t, q.Start())
	defer func() {
		require.NoError(t, q.Stop())
	}()

	_, err := q.Insert(dbShardInsert{})
	require.NoError(t, err)

	addTime(250 * time.Millisecond)
	_, err = q.Insert(dbShardInsert{})
	require.NoError(t, err)

	// Consecutive should be all rate limited
	for i := 0; i < 100; i++ {
		_, err = q.Insert(dbShardInsert{})
		require.Error(t, err)
		require.Equal(t, errNewSeriesInsertRateLimitExceeded, err)
	}

	// Start 2nd second should not be an issue
	addTime(750 * time.Millisecond)
	_, err = q.Insert(dbShardInsert{})
	require.NoError(t, err)

	addTime(100 * time.Millisecond)
	_, err = q.Insert(dbShardInsert{})
	require.NoError(t, err)

	addTime(100 * time.Millisecond)
	_, err = q.Insert(dbShardInsert{})
	require.Error(t, err)
	require.Equal(t, errNewSeriesInsertRateLimitExceeded, err)

	// Start 3rd second
	addTime(800 * time.Millisecond)
	_, err = q.Insert(dbShardInsert{})
	require.NoError(t, err)

	q.Lock()
	expectedCurrWindow := uint64(currTime.Truncate(time.Second).UnixNano())
	assert.Equal(t, expectedCurrWindow, q.insertPerSecondLimitWindowNanos.Load())
	assert.Equal(t, uint64(1), q.insertPerSecondLimitWindowValues.Load())
	q.Unlock()
}

func TestShardInsertQueueFlushedOnClose(t *testing.T) {
	defer leaktest.CheckTimeout(t, 5*time.Second)()

	var (
		numInsertExpected = 10
		numInsertObserved int64
		currTime          = time.Now().Truncate(time.Second)
	)

	q := newDatabaseShardInsertQueue(func(value []dbShardInsert) error {
		atomic.AddInt64(&numInsertObserved, int64(len(value)))
		return nil
	}, func() time.Time { return currTime }, tally.NoopScope, zap.NewNop())

	require.NoError(t, q.Start())

	for i := 0; i < numInsertExpected; i++ {
		_, err := q.Insert(dbShardInsert{})
		require.NoError(t, err)
	}

	require.NoError(t, q.Stop())
	require.Equal(t, int64(numInsertExpected), atomic.LoadInt64(&numInsertObserved))
}
