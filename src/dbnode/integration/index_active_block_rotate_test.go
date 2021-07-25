// +build integration
//
// Copyright (c) 2021 Uber Technologies, Inc.
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	"go.uber.org/zap/zaptest/observer"
)

func TestIndexActiveBlockRotate(t *testing.T) {
	var (
		testNsID       = ident.StringID("testns")
		numWrites      = 50
		numTags        = 10
		blockSize      = 2 * time.Hour
		indexBlockSize = blockSize
		bufferPast     = 10 * time.Minute
		rOpts          = retention.NewOptions().
				SetRetentionPeriod(blockSize).
				SetBlockSize(blockSize).
				SetBufferPast(bufferPast)

		idxOpts = namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(indexBlockSize)
		nsOpts  = namespace.NewOptions().
			SetRetentionOptions(rOpts).
			SetIndexOptions(idxOpts).
			SetColdWritesEnabled(true)

		defaultTimeout = time.Minute
		// verifyTimeout  = time.Minute
	)
	ns, err := namespace.NewMetadata(testNsID, nsOpts)
	require.NoError(t, err)

	// Set time to next warm flushable block transition
	// (i.e. align by block + bufferPast - time.Second)
	currTime := time.Now().UTC()
	progressTime := false
	progressTimeDelta := time.Duration(0)
	lockTime := sync.RWMutex{}
	setTime := func(t time.Time) {
		lockTime.Lock()
		defer lockTime.Unlock()
		progressTime = false
		currTime = t.UTC()
	}
	setProgressTime := func() {
		lockTime.Lock()
		defer lockTime.Unlock()
		progressTime = true
		actualNow := time.Now().UTC()
		progressTimeDelta = currTime.Sub(actualNow)
	}
	nowFn := func() time.Time {
		lockTime.RLock()
		at := currTime
		progress := progressTime
		progressDelta := progressTimeDelta
		lockTime.RUnlock()
		if progress {
			return time.Now().UTC().Add(progressDelta)
		}
		return at
	}

	testOpts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns}).
		SetWriteNewSeriesAsync(true).
		SetNowFn(nowFn)

	testSetup, err := NewTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.Close()

	// Set foreground compaction planner options to force index compaction.
	minCompactSize := 10
	foregroundCompactionOpts := compaction.DefaultOptions
	foregroundCompactionOpts.Levels = []compaction.Level{
		{
			MinSizeInclusive: 0,
			MaxSizeExclusive: int64(minCompactSize),
		},
	}
	indexOpts := testSetup.StorageOpts().IndexOptions().
		SetForegroundCompactionPlannerOptions(foregroundCompactionOpts)
	testSetup.SetStorageOpts(testSetup.StorageOpts().SetIndexOptions(indexOpts))

	// Configure log capture
	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	captureCore, logs := observer.New(zapcore.ErrorLevel)
	zapOpt := zap.WrapCore(func(existingCore zapcore.Core) zapcore.Core {
		return zapcore.NewTee(existingCore, captureCore)
	})
	log = log.WithOptions(zapOpt)

	// Wire up logger.
	instrumentOpts := testSetup.StorageOpts().InstrumentOptions().
		SetLogger(log)
	testSetup.SetStorageOpts(testSetup.StorageOpts().SetInstrumentOptions(instrumentOpts))
	scope := testSetup.Scope()

	// Start the server.
	require.NoError(t, testSetup.StartServer())

	// Stop the server.
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
	}()

	// Write test data.
	session, err := testSetup.M3DBClient().DefaultSession()
	require.NoError(t, err)

	var (
		metricGCSeries   = "index.block.active-block.gc-series+namespace=" + testNsID.String()
		metricFlushIndex = "database.flushIndex.success+namespace=" + testNsID.String()
	)
	prevWarmFlushes := counterValue(t, scope, metricFlushIndex)
	prevNumGCSeries := 0
	numGCSeries := counterValue(t, scope, metricGCSeries)
	require.Equal(t, 0, numGCSeries)

	prevLog := log
	for i := 0; i < 4; i++ {
		log := prevLog.With(zap.Int("checkIteration", i))

		// Progress to next time just before a flush and freeze (using setTime).
		prevTime := nowFn()
		newTime := prevTime.
			Truncate(indexBlockSize).
			Add(2 * indexBlockSize)
		setTime(newTime)
		log.Info("progressing time to before next block edge",
			zap.Stringer("prevTime", prevTime),
			zap.Stringer("newTime", newTime))

		start := time.Now()
		log.Info("writing test data")

		t0 := xtime.ToUnixNano(newTime.Add(-1 * (bufferPast / 2)))
		t1 := xtime.ToUnixNano(newTime)
		writesPeriodIter := GenerateTestIndexWrite(i, numWrites, numTags, t0, t1)
		writesPeriodIter.Write(t, testNsID, session)
		log.Info("test data written", zap.Duration("took", time.Since(start)))

		log.Info("waiting till data is indexed")
		indexed := xclock.WaitUntil(func() bool {
			indexedPeriod := writesPeriodIter.NumIndexed(t, testNsID, session)
			return indexedPeriod == len(writesPeriodIter)
		}, 15*time.Second)
		require.True(t, indexed,
			fmt.Sprintf("unexpected data indexed: actual=%d, expected=%d",
				writesPeriodIter.NumIndexedWithOptions(t, testNsID, session, NumIndexedOptions{Logger: log}),
				len(writesPeriodIter)))
		log.Info("verified data is indexed", zap.Duration("took", time.Since(start)))

		newTime = prevTime.
			Truncate(indexBlockSize).
			Add(2 * indexBlockSize).
			Add(bufferPast).
			Add(-100 * time.Millisecond)
		setTime(newTime)
		log.Info("progressing time to before next flush",
			zap.Stringer("prevTime", prevTime),
			zap.Stringer("newTime", newTime))

		log.Info("waiting till warm flush occurs")

		// Resume time progressing by wall clock.
		setProgressTime()

		// Start checks to ensure metrics are visible the whole time.
		checkFailed := atomic.NewUint64(0)
		checkIndexable := func() {
			numGCSeriesBefore := counterValue(t, scope, metricGCSeries)
			indexedPeriod := writesPeriodIter.NumIndexed(t, testNsID, session)
			numGCSeriesAfter := counterValue(t, scope, metricGCSeries)
			if len(writesPeriodIter) != indexedPeriod {
				assert.Equal(t, len(writesPeriodIter), indexedPeriod,
					fmt.Sprintf("some metrics not indexed/visible: actual=%d, expected=%d, numGCBefore=%d, numGCAfter=%d",
						writesPeriodIter.NumIndexedWithOptions(t, testNsID, session, NumIndexedOptions{Logger: log}),
						len(writesPeriodIter),
						numGCSeriesBefore,
						numGCSeriesAfter))
				checkFailed.Inc()
			}
		}

		ticker := time.NewTicker(10 * time.Millisecond)
		stopTickCh := make(chan struct{})
		closedTickCh := make(chan struct{})
		go func() {
			defer func() {
				ticker.Stop()
				close(closedTickCh)
			}()

			for {
				select {
				case <-ticker.C:
					checkIndexable()
				case <-stopTickCh:
					return
				}
			}
		}()

		start = time.Now()
		warmFlushed := xclock.WaitUntil(func() bool {
			return counterValue(t, scope, metricFlushIndex)-prevWarmFlushes > 0
		}, defaultTimeout)
		counter := counterValue(t, scope, metricFlushIndex)
		require.True(t, warmFlushed,
			fmt.Sprintf("warm flush stats: current=%d, previous=%d", counter, prevWarmFlushes))
		log.Info("verified data has been warm flushed", zap.Duration("took", time.Since(start)))
		prevWarmFlushes = int(counter)

		start = time.Now()
		log.Info("waiting for GC of series")

		expectedNumGCSeries := prevNumGCSeries + numWrites - minCompactSize
		gcSeries := xclock.WaitUntil(func() bool {
			numGCSeries := counterValue(t, scope, metricGCSeries)
			return numGCSeries >= expectedNumGCSeries
		}, defaultTimeout)
		numGCSeries := counterValue(t, scope, metricGCSeries)
		require.True(t, gcSeries,
			fmt.Sprintf("unexpected num gc series: actual=%d, expected=%d",
				numGCSeries, expectedNumGCSeries))
		require.True(t, numGCSeries >= expectedNumGCSeries)
		log.Info("verified series have been GC'd", zap.Duration("took", time.Since(start)))
		prevNumGCSeries = int(numGCSeries)

		require.Equal(t, 0, logs.Len(), "errors found in logs during flush/indexing")

		// Keep running indexable check for a few seconds, then progress next iter.
		time.Sleep(5 * time.Second)
		close(stopTickCh)
		<-closedTickCh

		// Ensure check did not fail.
		require.True(t, checkFailed.Load() == 0,
			fmt.Sprintf("check indexable errors: %d", checkFailed.Load()))
	}

	log.Info("checks passed")
}

func counterValue(t *testing.T, r tally.TestScope, key string) int {
	v, ok := r.Snapshot().Counters()[key]
	require.True(t, ok)
	return int(v.Value())
}
