// +build integration
//
// Copyright (c) 2021  Uber Technologies, Inc.
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
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	numTestSeries     = 5
	concurrentWorkers = 25
	writesPerWorker   = 5
	blockSize         = 2 * time.Hour
)

func TestIndexBlockOrphanedEntry(t *testing.T) {
	nsOpts := namespace.NewOptions().
		SetRetentionOptions(DefaultIntegrationTestRetentionOpts).
		SetIndexOptions(namespace.NewIndexOptions().SetEnabled(true))

	setup := generateTestSetup(t, nsOpts)
	defer setup.Close()

	// Start the server
	log := setup.StorageOpts().InstrumentOptions().Logger()
	require.NoError(t, setup.StartServer())

	// Stop the server
	defer func() {
		assert.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	client := setup.M3DBClient()
	session, err := client.DefaultSession()
	require.NoError(t, err)

	// Write concurrent metrics to generate multiple entries for the same series
	ids := make([]ident.ID, 0, numTestSeries)
	for i := 0; i < numTestSeries; i++ {
		fooID := ident.StringID(fmt.Sprintf("foo.%v", i))
		ids = append(ids, fooID)

		writeConcurrentMetrics(t, setup, session, fooID)
	}

	// Write metrics for a different series to push current foreground segment
	// to the background. After this, all documents for foo.X exist in background segments
	barID := ident.StringID("bar")
	writeConcurrentMetrics(t, setup, session, barID)

	// Fast-forward to a block rotation
	newBlock := xtime.Now().Truncate(blockSize).Add(blockSize)
	newCurrentTime := newBlock.Add(30 * time.Minute) // Add extra to account for buffer past
	setup.SetNowFn(newCurrentTime)

	// Wait for flush
	log.Info("waiting for block rotation to complete")
	nsID := setup.Namespaces()[0].ID()
	found := xclock.WaitUntil(func() bool {
		filesets, err := fs.IndexFileSetsAt(setup.FilePathPrefix(), nsID, newBlock.Add(-blockSize))
		require.NoError(t, err)
		return len(filesets) == 1
	}, 60*time.Second)
	require.True(t, found)

	// Do post-block rotation writes
	for _, id := range ids {
		writeMetric(t, session, nsID, id, newCurrentTime, 999.0)
	}
	writeMetric(t, session, nsID, barID, newCurrentTime, 999.0)

	// Foreground segments should be in the background again which means updated index entry
	// is now behind the orphaned entry so index reads should fail.
	log.Info("waiting for metrics to be indexed")
	var (
		missing string
		ok      bool
	)
	found = xclock.WaitUntil(func() bool {
		for _, id := range ids {
			ok, err = isIndexedCheckedWithTime(
				t, session, nsID, id, genTags(id), newCurrentTime,
			)
			if !ok || err != nil {
				missing = id.String()
				return false
			}
		}
		return true
	}, 30*time.Second)
	assert.True(t, found, fmt.Sprintf("series %s never indexed\n", missing))
	assert.NoError(t, err)
}

func writeConcurrentMetrics(
	t *testing.T,
	setup TestSetup,
	session client.Session,
	seriesID ident.ID,
) {
	var wg sync.WaitGroup
	nowFn := setup.DB().Options().ClockOptions().NowFn()

	workerPool := xsync.NewWorkerPool(concurrentWorkers)
	workerPool.Init()

	mdID := setup.Namespaces()[0].ID()
	for i := 0; i < concurrentWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < writesPerWorker; j++ {
				j := j
				wg.Add(1)
				workerPool.Go(func() {
					defer wg.Done()
					writeMetric(t, session, mdID, seriesID, xtime.ToUnixNano(nowFn()), float64(j))
				})
			}
		}()
	}

	wg.Wait()
}

func genTags(seriesID ident.ID) ident.TagsIterator {
	return ident.NewTagsIterator(ident.NewTags(ident.StringTag("tagName", seriesID.String())))
}

func writeMetric(
	t *testing.T,
	session client.Session,
	nsID ident.ID,
	seriesID ident.ID,
	timestamp xtime.UnixNano,
	value float64,
) {
	err := session.WriteTagged(nsID, seriesID, genTags(seriesID),
		timestamp, value, xtime.Second, nil)
	require.NoError(t, err)
}

func generateTestSetup(t *testing.T, nsOpts namespace.Options) TestSetup {
	md, err := namespace.NewMetadata(testNamespaces[0], nsOpts)
	require.NoError(t, err)

	testOpts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{md}).
		SetWriteNewSeriesAsync(true)
	testSetup, err := NewTestSetup(t, testOpts, nil,
		func(s storage.Options) storage.Options {
			s = s.SetCoreFn(func() int {
				return rand.Intn(4) //nolint:gosec
			})
			compactionOpts := s.IndexOptions().ForegroundCompactionPlannerOptions()
			compactionOpts.Levels = []compaction.Level{
				{
					MinSizeInclusive: 0,
					MaxSizeExclusive: 1,
				},
			}
			return s.SetIndexOptions(
				s.IndexOptions().SetForegroundCompactionPlannerOptions(compactionOpts))
		})
	require.NoError(t, err)

	return testSetup
}

func TestIndexBlockOrphanedIndexValuesUpdatedAcrossTimes(t *testing.T) {
	tests := []struct {
		name     string
		numIDs   int
		interval time.Duration
	}{
		{
			name:     "4 series every 100 nanos",
			numIDs:   4,
			interval: 100,
		},
		{
			name:     "4 series every block",
			numIDs:   4,
			interval: blockSize,
		},
		{
			name:     "12 series every 100 nanos",
			numIDs:   12,
			interval: 100,
		},
		{
			name:     "12 series every block",
			numIDs:   12,
			interval: blockSize,
		},
		{
			name:     "120 series every 100 nanos",
			numIDs:   120,
			interval: 100,
		},
		{
			name:     "120 series every block",
			numIDs:   120,
			interval: blockSize,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testIndexBlockOrphanedIndexValuesUpdatedAcrossTimes(t, tt.numIDs, tt.interval)
		})
	}
}

func testIndexBlockOrphanedIndexValuesUpdatedAcrossTimes(
	t *testing.T, numIDs int, writeInterval time.Duration,
) {
	// Write a metric concurrently for multiple index blocks to generate
	// multiple entries for the same series
	var (
		concurrentWriteMax = runtime.NumCPU() / 2
		ids                = make([]ident.ID, 0, numIDs)
		writerCh           = make(chan func(), concurrentWriteMax)

		nsID = testNamespaces[0]

		writesPerWorker = 5
		writeTimes      = make([]xtime.UnixNano, 0, writesPerWorker)
		retention       = blockSize * time.Duration(1+writesPerWorker)

		seed = time.Now().UnixNano()
		rng  = rand.New(rand.NewSource(seed)) // nolint:gosec
	)

	retOpts := DefaultIntegrationTestRetentionOpts.SetRetentionPeriod(retention)
	nsOpts := namespace.NewOptions().
		SetRetentionOptions(retOpts).
		SetIndexOptions(namespace.NewIndexOptions().SetEnabled(true)).
		SetColdWritesEnabled(true)

	setup := generateTestSetup(t, nsOpts)
	defer setup.Close()

	// Start the server
	log := setup.StorageOpts().InstrumentOptions().Logger()
	log.Info("running test with seed", zap.Int("seed", int(seed)))
	require.NoError(t, setup.StartServer())

	// Stop the server
	defer func() {
		assert.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	client := setup.M3DBClient()
	session, err := client.DefaultSession()
	require.NoError(t, err)

	var (
		nowFn = setup.DB().Options().ClockOptions().NowFn()
		// NB: write in the middle of a block to avoid block boundaries.
		now = nowFn().Truncate(blockSize / 2)
	)

	for i := 0; i < writesPerWorker; i++ {
		writeTime := xtime.ToUnixNano(now.Add(time.Duration(i) * -writeInterval))
		writeTimes = append(writeTimes, writeTime)
	}

	fns := make([]func(), 0, numIDs*writesPerWorker)
	for i := 0; i < numIDs; i++ {
		fooID := ident.StringID(fmt.Sprintf("foo.%v", i))
		ids = append(ids, fooID)
		fns = append(fns, writeConcurrentMetricsAcrossTime(t, setup, session, writeTimes, fooID)...)
	}

	rng.Shuffle(len(fns), func(i, j int) { fns[i], fns[j] = fns[j], fns[i] })
	var wg sync.WaitGroup
	for i := 0; i < concurrentWriteMax; i++ {
		wg.Add(1)
		go func() {
			for writeFn := range writerCh {
				writeFn()
			}

			wg.Done()
		}()
	}

	for _, fn := range fns {
		writerCh <- fn
	}

	close(writerCh)
	wg.Wait()

	queryIDs := func() {
		notFoundIds := make(notFoundIDs, 0, len(ids)*len(writeTimes))
		for _, id := range ids {
			for _, writeTime := range writeTimes {
				notFoundIds = append(notFoundIds, notFoundID{id: id, runAt: writeTime})
			}
		}

		found := xclock.WaitUntil(func() bool {
			filteredIds := notFoundIds[:0]
			for _, id := range notFoundIds {
				ok, err := isIndexedCheckedWithTime(
					t, session, nsID, id.id, genTags(id.id), id.runAt,
				)
				if !ok || err != nil {
					filteredIds = append(filteredIds, id)
				}
			}

			if len(filteredIds) == 0 {
				return true
			}

			notFoundIds = filteredIds
			return false
		}, time.Second*30)

		require.True(t, found, fmt.Sprintf("series %s never indexed\n", notFoundIds))
	}

	// Ensure all IDs are eventually queryable, even when only in the foreground
	// segments.
	queryIDs()

	// Write metrics for a different series to push current foreground segment
	// to the background. After this, all documents for foo.X exist in background segments
	barID := ident.StringID("bar")
	writeConcurrentMetrics(t, setup, session, barID)

	queryIDs()

	// Fast-forward to a block rotation
	newBlock := xtime.Now().Truncate(blockSize).Add(blockSize)
	newCurrentTime := newBlock.Add(30 * time.Minute) // Add extra to account for buffer past
	setup.SetNowFn(newCurrentTime)

	// Wait for flush
	log.Info("waiting for block rotation to complete")
	found := xclock.WaitUntil(func() bool {
		filesets, err := fs.IndexFileSetsAt(setup.FilePathPrefix(), nsID, newBlock.Add(-blockSize))
		require.NoError(t, err)
		return len(filesets) == 1
	}, 30*time.Second)
	require.True(t, found)

	queryIDs()
}

type notFoundID struct {
	id    ident.ID
	runAt xtime.UnixNano
}

func (i notFoundID) String() string {
	return fmt.Sprintf("{%s: %s}", i.id.String(), i.runAt.String())
}

type notFoundIDs []notFoundID

func (ids notFoundIDs) String() string {
	strs := make([]string, 0, len(ids))
	for _, id := range ids {
		strs = append(strs, id.String())
	}

	return fmt.Sprintf("[%s]", strings.Join(strs, ", "))
}

// writeConcurrentMetricsAcrossTime writes a datapoint for the given series at
// each `writeTime` simultaneously.
func writeConcurrentMetricsAcrossTime(
	t *testing.T,
	setup TestSetup,
	session client.Session,
	writeTimes []xtime.UnixNano,
	seriesID ident.ID,
) []func() {
	workerPool := xsync.NewWorkerPool(concurrentWorkers)
	workerPool.Init()

	mdID := setup.Namespaces()[0].ID()
	fns := make([]func(), 0, len(writeTimes))

	for j, writeTime := range writeTimes {
		j, writeTime := j, writeTime
		fns = append(fns, func() {
			writeMetric(t, session, mdID, seriesID, writeTime, float64(j))
		})
	}

	return fns
}
