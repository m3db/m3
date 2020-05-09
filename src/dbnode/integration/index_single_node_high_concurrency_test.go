// +build integration
//
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

package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage"
	xclock "github.com/m3db/m3/src/x/clock"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
)

func TestIndexSingleNodeHighConcurrencyManyTagsLowCardinality(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	testIndexSingleNodeHighConcurrency(t, testIndexHighConcurrencyOptions{
		concurrencyEnqueueWorker: 8,
		concurrencyWrites:        5000,
		enqueuePerWorker:         100,
		numTags:                  10,
	})
}

func TestIndexSingleNodeHighConcurrencyFewTagsHighCardinalityNoSkipWrites(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	testIndexSingleNodeHighConcurrency(t, testIndexHighConcurrencyOptions{
		concurrencyEnqueueWorker: 8,
		concurrencyWrites:        5000,
		enqueuePerWorker:         50000,
		numTags:                  2,
	})
}

func TestIndexSingleNodeHighConcurrencyFewTagsHighCardinalitySkipWrites(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	testIndexSingleNodeHighConcurrency(t, testIndexHighConcurrencyOptions{
		concurrencyEnqueueWorker: 8,
		concurrencyWrites:        5000,
		enqueuePerWorker:         10000,
		numTags:                  2,
		skipWrites:               true,
	})
}

type testIndexHighConcurrencyOptions struct {
	concurrencyEnqueueWorker int
	concurrencyWrites        int
	enqueuePerWorker         int
	numTags                  int
	// skipWrites will mix in skipped to make sure
	// it doesn't interrupt the regular real-time ingestion pipeline.
	skipWrites bool
}

func testIndexSingleNodeHighConcurrency(
	t *testing.T,
	opts testIndexHighConcurrencyOptions,
) {
	// Test setup
	md, err := namespace.NewMetadata(testNamespaces[0],
		namespace.NewOptions().
			SetRetentionOptions(defaultIntegrationTestRetentionOpts).
			SetCleanupEnabled(false).
			SetSnapshotEnabled(false).
			SetFlushEnabled(false).
			SetColdWritesEnabled(true).
			SetIndexOptions(namespace.NewIndexOptions().SetEnabled(true)))
	require.NoError(t, err)

	testOpts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{md}).
		SetWriteNewSeriesAsync(true).
		// Use default time functions (server time not frozen).
		SetNowFn(time.Now)
	testSetup, err := newTestSetup(t, testOpts, nil,
		func(s storage.Options) storage.Options {
			if opts.skipWrites {
				return s.SetDebugSkipIndexEvery(2)
			}
			return s
		})
	require.NoError(t, err)
	defer testSetup.close()

	// Start the server
	log := testSetup.storageOpts.InstrumentOptions().Logger()
	require.NoError(t, testSetup.startServer())

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Debug("server is now down")
	}()

	client := testSetup.m3dbClient
	session, err := client.DefaultSession()
	require.NoError(t, err)

	var (
		wg              sync.WaitGroup
		numTotalErrors  = atomic.NewUint32(0)
		numTotalSuccess = atomic.NewUint32(0)
	)
	nowFn := testSetup.db.Options().ClockOptions().NowFn()
	start := time.Now()
	log.Info("starting data write",
		zap.Time("serverTime", nowFn()))

	workerPool := xsync.NewWorkerPool(opts.concurrencyWrites)
	workerPool.Init()

	for i := 0; i < opts.concurrencyEnqueueWorker; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < opts.enqueuePerWorker; j++ {
				j := j
				wg.Add(1)
				workerPool.Go(func() {
					defer wg.Done()

					id, tags := genIDTags(i, j, opts.numTags)
					now := time.Now()
					timestamp := now

					err := session.WriteTagged(md.ID(), id, tags,
						timestamp, float64(j), xtime.Second, nil)
					if err != nil {
						// Only count errors that aren't bad writes.
						numTotalErrors.Inc()
						if n := numTotalErrors.Inc(); n < 10 {
							log.Error("sampled write error", zap.Error(err))
						}
					} else {
						numTotalSuccess.Inc()
					}
				})
			}
		}()
	}

	wg.Wait()

	require.Equal(t, int(0), int(numTotalErrors.Load()))

	log.Info("test data written",
		zap.Duration("took", time.Since(start)),
		zap.Int("written", int(numTotalSuccess.Load())),
		zap.Time("serverTime", nowFn()))

	log.Info("data indexing verify start")

	// Wait for at least all things to be enqueued for indexing.
	expectStatPrefix := "dbindex.index-attempt+namespace=testNs1,"
	expectStatProcess := expectStatPrefix + "stage=process"
	numIndexTotal := opts.enqueuePerWorker
	multiplyByConcurrency := multiplyBy(opts.concurrencyEnqueueWorker)
	expectNumIndex := multiplyByConcurrency(numIndexTotal)
	indexProcess := xclock.WaitUntil(func() bool {
		counters := testSetup.scope.Snapshot().Counters()
		counter, ok := counters[expectStatProcess]
		if !ok {
			return false
		}
		return int(counter.Value()) == expectNumIndex
	}, time.Minute)

	counters := testSetup.scope.Snapshot().Counters()
	counter, ok := counters[expectStatProcess]

	var value int
	if ok {
		value = int(counter.Value())
	}
	assert.True(t, indexProcess,
		fmt.Sprintf("expected to index %d but processed %d", expectNumIndex, value))

	// Now check all of them are individually indexed.
	var (
		fetchWg        sync.WaitGroup
		allIndexed     = true
		allIndexedLock sync.Mutex
	)
	for i := 0; i < opts.concurrencyEnqueueWorker; i++ {
		fetchWg.Add(1)
		idx := i
		go func() {
			defer fetchWg.Done()

			id, tags := genIDTags(idx, opts.enqueuePerWorker-1, opts.numTags)
			indexed := xclock.WaitUntil(func() bool {
				found := isIndexed(t, session, md.ID(), id, tags)
				return found
			}, 30*time.Second)
			assert.True(t, indexed)

			allIndexedLock.Lock()
			allIndexed = allIndexed && indexed
			allIndexedLock.Unlock()
		}()
	}
	fetchWg.Wait()
	log.Info("data indexing verify done",
		zap.Bool("allIndexed", allIndexed),
		zap.Duration("took", time.Since(start)))

	// Make sure attempted total indexing = skipped + written.
	counters = testSetup.scope.Snapshot().Counters()
	totalSkippedWritten := 0
	for _, expectID := range []string{
		expectStatPrefix + "stage=skip",
		expectStatPrefix + "stage=write",
	} {
		actual, ok := counters[expectID]
		assert.True(t, ok,
			fmt.Sprintf("counter not found to test value: id=%s", expectID))
		if ok {
			totalSkippedWritten += int(actual.Value())
		}
	}

	log.Info("check written + skipped",
		zap.Int("expectedValue", multiplyByConcurrency(numIndexTotal)),
		zap.Int("actualValue", totalSkippedWritten))
	assert.Equal(t, multiplyByConcurrency(numIndexTotal), totalSkippedWritten,
		"total written + skipped mismatch")
}

func multiplyBy(n int) func(int) int {
	return func(x int) int {
		return n * x
	}
}
