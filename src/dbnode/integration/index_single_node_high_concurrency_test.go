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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
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
		enqueuePerWorker:         10000,
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

func TestIndexSingleNodeHighConcurrencyFewTagsHighCardinalityQueryDuringWrites(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	testIndexSingleNodeHighConcurrency(t, testIndexHighConcurrencyOptions{
		concurrencyEnqueueWorker:     8,
		concurrencyWrites:            5000,
		enqueuePerWorker:             100000,
		numTags:                      2,
		concurrencyQueryDuringWrites: 16,
		skipVerify:                   true,
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

	// concurrencyQueryDuringWrites will issue queries while we
	// are performing writes.
	concurrencyQueryDuringWrites int

	// skipVerify will skip verifying the actual series were indexed
	// which is useful if just sanity checking can write/read concurrently
	// without issue/errors and the stats look good.
	skipVerify bool
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

	testOpts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{md}).
		SetWriteNewSeriesAsync(true).
		// Use default time functions (server time not frozen).
		SetNowFn(time.Now)
	testSetup, err := NewTestSetup(t, testOpts, nil,
		func(s storage.Options) storage.Options {
			if opts.skipWrites {
				return s.SetDoNotIndexWithFieldsMap(map[string]string{"skip": "true"})
			}
			return s
		})
	require.NoError(t, err)
	defer testSetup.Close()

	// Start the server
	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	require.NoError(t, testSetup.StartServer())

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
	}()

	client := testSetup.M3DBClient()
	session, err := client.DefaultSession()
	require.NoError(t, err)

	var (
		wg              sync.WaitGroup
		numTotalErrors  = atomic.NewUint32(0)
		numTotalSuccess = atomic.NewUint32(0)
	)
	nowFn := testSetup.DB().Options().ClockOptions().NowFn()
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

					var genOpts []genIDTagsOption
					if opts.skipWrites && j%2 == 0 {
						genOpts = append(genOpts, genIDTagsOption(func(t ident.Tags) ident.Tags {
							t.Append(ident.Tag{
								Name:  ident.StringID("skip"),
								Value: ident.StringID("true"),
							})
							return t
						}))
					}

					id, tags := genIDTags(i, j, opts.numTags, genOpts...)
					timestamp := time.Now()
					err := session.WriteTagged(md.ID(), id, tags,
						timestamp, float64(j), xtime.Second, nil)
					if err != nil {
						if n := numTotalErrors.Inc(); n < 10 {
							// Log the first 10 errors for visibility but not flood.
							log.Error("sampled write error", zap.Error(err))
						}
					} else {
						numTotalSuccess.Inc()
					}
				})
			}
		}()
	}

	// If concurrent query load enabled while writing also hit with queries.
	queryConcDuringWritesCloseCh := make(chan struct{}, 1)
	numTotalQueryErrors := atomic.NewUint32(0)
	if opts.concurrencyQueryDuringWrites == 0 {
		log.Info("no concurrent queries during writes configured")
	} else {
		log.Info("starting concurrent queries during writes",
			zap.Int("concurrency", opts.concurrencyQueryDuringWrites))
		for i := 0; i < opts.concurrencyQueryDuringWrites; i++ {
			go func() {
				src := rand.NewSource(int64(i))
				rng := rand.New(src)
				for {
					select {
					case <-queryConcDuringWritesCloseCh:
						return
					default:
						randI := rng.Intn(opts.concurrencyEnqueueWorker)
						randJ := rng.Intn(opts.enqueuePerWorker)
						id, tags := genIDTags(randI, randJ, opts.numTags)
						_, err := isIndexedChecked(t, session, md.ID(), id, tags)
						if err != nil {
							if n := numTotalQueryErrors.Inc(); n < 10 {
								// Log the first 10 errors for visibility but not flood.
								log.Error("sampled query error", zap.Error(err))
							}
						}
					}
				}
			}()
		}
	}

	// Wait for writes to at least be enqueued.
	wg.Wait()

	// Check no write errors.
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
		counters := testSetup.Scope().Snapshot().Counters()
		counter, ok := counters[expectStatProcess]
		if !ok {
			return false
		}
		return int(counter.Value()) == expectNumIndex
	}, time.Minute)

	counters := testSetup.Scope().Snapshot().Counters()
	counter, ok := counters[expectStatProcess]

	var value int
	if ok {
		value = int(counter.Value())
	}
	assert.True(t, indexProcess,
		fmt.Sprintf("expected to index %d but processed %d", expectNumIndex, value))

	// Allow concurrent query during writes to finish.
	close(queryConcDuringWritesCloseCh)

	// Check no query errors.
	require.Equal(t, int(0), int(numTotalErrors.Load()))

	if !opts.skipVerify {
		log.Info("data indexing each series visible start")
		// Now check all of them are individually indexed.
		var (
			fetchWg        sync.WaitGroup
			notIndexedErrs []error
			notIndexedLock sync.Mutex
		)
		for i := 0; i < opts.concurrencyEnqueueWorker; i++ {
			fetchWg.Add(1)
			i := i
			go func() {
				defer fetchWg.Done()

				for j := 0; j < opts.enqueuePerWorker; j++ {
					if opts.skipWrites && j%2 == 0 {
						continue // not meant to be indexed.
					}

					j := j
					fetchWg.Add(1)
					workerPool.Go(func() {
						defer fetchWg.Done()

						id, tags := genIDTags(i, j, opts.numTags)
						indexed := xclock.WaitUntil(func() bool {
							found := isIndexed(t, session, md.ID(), id, tags)
							return found
						}, 30*time.Second)
						if !indexed {
							err := fmt.Errorf("not indexed series: i=%d, j=%d", i, j)
							notIndexedLock.Lock()
							notIndexedErrs = append(notIndexedErrs, err)
							notIndexedLock.Unlock()
						}
					})
				}
			}()
		}
		fetchWg.Wait()
		log.Info("data indexing verify done",
			zap.Int("notIndexed", len(notIndexedErrs)),
			zap.Duration("took", time.Since(start)))
		require.Equal(t, 0, len(notIndexedErrs),
			fmt.Sprintf("not indexed errors: %v", notIndexedErrs[:min(5, len(notIndexedErrs))]))
	}

	// Make sure attempted total indexing = skipped + written.
	counters = testSetup.Scope().Snapshot().Counters()
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

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
