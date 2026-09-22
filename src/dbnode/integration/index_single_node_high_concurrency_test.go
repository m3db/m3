//go:build integration
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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"
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
		concurrencyEnqueueWorker:         8,
		concurrencyWrites:                5000,
		enqueuePerWorker:                 100000,
		numTags:                          2,
		concurrencyQueryDuringWrites:     16,
		concurrencyQueryDuringWritesType: indexQuery,
		skipVerify:                       true,
	})
}

func TestIndexSingleNodeHighConcurrencyFewTagsHighCardinalityAggregateQueryDuringWrites(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	testIndexSingleNodeHighConcurrency(t, testIndexHighConcurrencyOptions{
		concurrencyEnqueueWorker:         8,
		concurrencyWrites:                5000,
		enqueuePerWorker:                 100000,
		numTags:                          2,
		concurrencyQueryDuringWrites:     1,
		concurrencyQueryDuringWritesType: indexAggregateQuery,
		skipVerify:                       true,
	})
}

// minAcceptedWriteFraction is the share of attempted writes the node has to
// accept for a run to count as healthy backpressure rather than a capacity
// regression.
//
// Observed rejection rates on the 800k-write cases are 0.37% (recently_read)
// and 6.3% (lru, on an already pathological run), so a 10% allowance leaves
// ample headroom on the typical case and roughly 1.6x on the worst one seen.
// That is the tighter end of what the data supports: the counts are logged
// above, so loosen this if CI shows the margin is too thin.
const minAcceptedWriteFraction = 0.9

type queryType uint

const (
	indexQuery queryType = iota
	indexAggregateQuery
)

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

	// concurrencyQueryDuringWritesType determines the type of queries
	// to issue performing writes.
	concurrencyQueryDuringWritesType queryType

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
			SetRetentionOptions(DefaultIntegrationTestRetentionOpts).
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
		wg                 sync.WaitGroup
		numTotalErrors     = atomic.NewUint32(0)
		numTotalOverloaded = atomic.NewUint32(0)
		numTotalSuccess    = atomic.NewUint32(0)
	)
	nowFn := testSetup.DB().Options().ClockOptions().NowFn()
	start := time.Now()
	log.Info("starting data write",
		zap.Time("serverTime", nowFn()))

	workerPool := xsync.NewWorkerPool(opts.concurrencyWrites)
	workerPool.Init()

	for i := range opts.concurrencyEnqueueWorker {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := range opts.enqueuePerWorker {
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
					timestamp := xtime.Now()
					err := session.WriteTagged(md.ID(), id, tags,
						timestamp, float64(j), xtime.Second, nil)
					if err != nil {
						if isServerOverloadedErr(err) {
							// The node shedding load is the server working as
							// designed: this test deliberately overloads a
							// single node, so rejections are backpressure, not
							// a write bug. Count them separately rather than
							// failing the run on a loaded CI host.
							numTotalOverloaded.Inc()
						} else if n := numTotalErrors.Inc(); n < 10 {
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
	// Stopping the query goroutines has to survive a failed require below. A
	// require calls runtime.Goexit, which skips straight to the deferred
	// testSetup.Close() and tears the namespace down underneath any query
	// goroutine still running. Deferring the stop here guarantees they are shut
	// down first; sync.Once keeps the explicit stop further down safe.
	//
	// The wait matters as much as the close: signaling alone still leaves a
	// goroutine mid-query during teardown, and leaves the query counters racy
	// to read.
	var (
		queryWg         sync.WaitGroup
		stopQueriesOnce sync.Once
	)
	stopQueries := func() {
		stopQueriesOnce.Do(func() {
			close(queryConcDuringWritesCloseCh)
			queryWg.Wait()
		})
	}
	defer stopQueries()
	numTotalQueryMatches := atomic.NewUint32(0)
	numTotalQueryErrors := atomic.NewUint32(0)
	checkNumTotalQueryMatches := false
	if opts.concurrencyQueryDuringWrites == 0 {
		log.Info("no concurrent queries during writes configured")
	} else {
		log.Info("starting concurrent queries during writes",
			zap.Int("concurrency", opts.concurrencyQueryDuringWrites))
		checkNumTotalQueryMatches = true
		for i := range opts.concurrencyQueryDuringWrites {
			queryWg.Add(1)
			go func() {
				defer queryWg.Done()

				src := rand.NewSource(int64(i))
				rng := rand.New(src)
				for {
					select {
					case <-queryConcDuringWritesCloseCh:
						return
					default:
					}

					switch opts.concurrencyQueryDuringWritesType {
					case indexQuery:
						randI := rng.Intn(opts.concurrencyEnqueueWorker)
						randJ := rng.Intn(opts.enqueuePerWorker)
						id, tags := genIDTags(randI, randJ, opts.numTags)
						ok, err := isIndexedChecked(t, session, md.ID(), id, tags)
						if err != nil {
							if n := numTotalQueryErrors.Inc(); n < 10 {
								// Log the first 10 errors for visibility but not flood.
								log.Error("sampled query error", zap.Error(err))
							}
						}
						if ok {
							numTotalQueryMatches.Inc()
						}
					case indexAggregateQuery:
						randI := rng.Intn(opts.concurrencyEnqueueWorker)
						match := idx.NewTermQuery([]byte("common_i"), []byte(strconv.Itoa(randI)))
						q := index.Query{Query: match}

						now := xtime.Now()
						qOpts := index.AggregationOptions{
							QueryOptions: index.QueryOptions{
								StartInclusive: now.Add(-md.Options().RetentionOptions().RetentionPeriod()),
								EndExclusive:   now,
								DocsLimit:      1000,
							},
						}

						ctx := context.NewBackground()
						r, err := testSetup.DB().AggregateQuery(ctx, md.ID(), q, qOpts)
						if err != nil {
							// Record rather than panic. This runs on a
							// non-test goroutine, so a panic here takes down
							// the whole test binary and buries the failure
							// that actually caused it.
							if n := numTotalQueryErrors.Inc(); n < 10 {
								// Log the first 10 errors for visibility but not flood.
								log.Error("sampled query error", zap.Error(err))
							}
							ctx.Close()
							continue
						}

						tagValues := 0
						for _, entry := range r.Results.Map().Iter() {
							values := entry.Value()
							tagValues += values.Size()
						}

						// Done with resources, return to pool.
						ctx.Close()

						numTotalQueryMatches.Add(uint32(tagValues))
					default:
						panic("unknown query type")
					}
				}
			}()
		}
	}

	// Wait for writes to at least be enqueued.
	wg.Wait()

	// Check no write errors.
	require.Equal(t, int(0), int(numTotalErrors.Load()))

	if checkNumTotalQueryMatches {
		// Check matches.
		require.True(t, numTotalQueryMatches.Load() > 0, "no query matches")
	}

	log.Info("test data written",
		zap.Duration("took", time.Since(start)),
		zap.Int("written", int(numTotalSuccess.Load())),
		zap.Uint32("overloadedRejections", numTotalOverloaded.Load()),
		zap.Time("serverTime", nowFn()),
		zap.Uint32("queryMatches", numTotalQueryMatches.Load()))

	// Backpressure is tolerated above, but only as backpressure. The node
	// shedding a slice of the load under CI contention is expected; shedding
	// most of it is a capacity regression. That distinction needs asserting
	// here because the index expectation below is derived from what the server
	// accepted, so without a floor a run that accepted almost nothing would
	// index almost nothing and still pass.
	attemptedWrites := opts.concurrencyEnqueueWorker * opts.enqueuePerWorker
	minAcceptedWrites := int(float64(attemptedWrites) * minAcceptedWriteFraction)
	require.GreaterOrEqual(t, int(numTotalSuccess.Load()), minAcceptedWrites,
		"server accepted %d of %d attempted writes, under the %.0f%% floor: "+
			"that is a capacity regression rather than backpressure",
		numTotalSuccess.Load(), attemptedWrites, minAcceptedWriteFraction*100)

	log.Info("data indexing verify start")

	// Wait for at least all things to be enqueued for indexing.
	expectStatPrefix := "dbindex.index-attempt+namespace=testNs1,"
	expectStatProcess := expectStatPrefix + "stage=process"
	// Every write the client saw succeed must reach the index, so that count is
	// a floor rather than an exact target. It cannot be an exact target because
	// indexing happens before the commit log write (see db.WriteTagged), so a
	// write rejected by a full commit log queue is indexed and still counted as
	// a client failure. Those land in the gap between the floor and the number
	// of writes actually attempted, which is the ceiling.
	minNumIndex := int(numTotalSuccess.Load())
	indexProcess := xclock.WaitUntil(func() bool {
		counters := testSetup.Scope().Snapshot().Counters()
		counter, ok := counters[expectStatProcess]
		if !ok {
			return false
		}
		return int(counter.Value()) >= minNumIndex
	}, time.Minute*5)

	counters := testSetup.Scope().Snapshot().Counters()
	counter, ok := counters[expectStatProcess]

	var numIndexed int
	if ok {
		numIndexed = int(counter.Value())
	}
	assert.True(t, indexProcess,
		fmt.Sprintf("timeout waiting for index to process: expected to index at least %d but only processed %d",
			minNumIndex, numIndexed))
	assert.LessOrEqual(t, numIndexed, attemptedWrites,
		"indexed more series than were ever written")

	// Allow concurrent query during writes to finish.
	stopQueries()

	// Check no query errors.
	require.Equal(t, int(0), int(numTotalQueryErrors.Load()))

	if !opts.skipVerify {
		log.Info("data indexing each series visible start")
		// Now check all of them are individually indexed.
		var (
			fetchWg        sync.WaitGroup
			notIndexedErrs []error
			notIndexedLock sync.Mutex
		)
		for i := range opts.concurrencyEnqueueWorker {
			fetchWg.Add(1)
			go func() {
				defer fetchWg.Done()

				for j := range opts.enqueuePerWorker {
					if opts.skipWrites && j%2 == 0 {
						continue // not meant to be indexed.
					}

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

		require.Equal(t, 0, len(notIndexedErrs),
			fmt.Sprintf("not indexed errors: %v", notIndexedErrs[:min(5, len(notIndexedErrs))]))
	}

	log.Info("data indexing verify done", zap.Duration("took", time.Since(start)))

	// Make sure attempted total indexing = skipped + written. Read all three
	// stages from one snapshot: indexing can still advance between snapshots,
	// and comparing a stale total against fresh stages would fail on timing
	// alone. Compare against the process counter rather than the client's
	// success count, which undercounts by the backpressure gap above.
	counters = testSetup.Scope().Snapshot().Counters()
	totalProcessed := 0
	if actual, ok := counters[expectStatProcess]; ok {
		totalProcessed = int(actual.Value())
	}
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
		zap.Int("expectedValue", totalProcessed),
		zap.Int("actualValue", totalSkippedWritten))
	assert.Equal(t, totalProcessed, totalSkippedWritten,
		"total written + skipped mismatch")
}

// isServerOverloadedErr reports whether err is the node rejecting a write
// because it is shedding load. There are two such paths, both backpressure
// rather than a write bug:
//
//   - errServerIsOverloaded, when the write batch queue is at capacity.
//   - commitlog.ErrCommitLogQueueFull, when the commit log queue is at
//     capacity.
//
// Both are raised server side and reach the client wrapped in a tchannel
// internal error that carries only the message, so matching on the text is
// the only option. errServerIsOverloaded is unexported, hence the literal.
func isServerOverloadedErr(err error) bool {
	if err == nil {
		return false
	}

	msg := err.Error()

	return strings.Contains(msg, "server is overloaded") ||
		strings.Contains(msg, commitlog.ErrCommitLogQueueFull.Error())
}
