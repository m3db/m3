// +build integration

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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

func TestReadAggregateWrite(t *testing.T) {
	var (
		blockSize       = 2 * time.Hour
		blockSizeT      = 6 * time.Hour
		indexBlockSize  = 2 * blockSize
		indexBlockSizeT = 2 * blockSizeT
		rOpts           = retention.NewOptions().SetRetentionPeriod(24 * blockSize).SetBlockSize(blockSize)
		rOptsT          = retention.NewOptions().SetRetentionPeriod(24 * blockSize).SetBlockSize(blockSizeT)
		idxOpts         = namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(indexBlockSize)
		idxOptsT        = namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(indexBlockSizeT)
		nsOpts          = namespace.NewOptions().
				SetRetentionOptions(rOpts).
				SetIndexOptions(idxOpts).
				SetColdWritesEnabled(true)
		nsOptsT = namespace.NewOptions().
			SetRetentionOptions(rOptsT).
			SetIndexOptions(idxOptsT).
			SetColdWritesEnabled(true)
	)

	srcNs, err := namespace.NewMetadata(testNamespaces[0], nsOpts)
	require.NoError(t, err)
	trgNs, err := namespace.NewMetadata(testNamespaces[1], nsOptsT)
	require.NoError(t, err)

	testOpts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{srcNs, trgNs}).
		SetWriteNewSeriesAsync(true)

	testSetup := newTestSetupWithCommitLogAndFilesystemBootstrapper(t, testOpts)
	defer testSetup.Close()

	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(
		tally.ScopeOptions{Reporter: reporter}, time.Millisecond)
	defer closer.Close()
	testSetup.SetStorageOpts(testSetup.StorageOpts().SetInstrumentOptions(
		instrument.NewOptions().SetMetricsScope(scope)))

	storageOpts := testSetup.StorageOpts()
	testSetup.SetStorageOpts(storageOpts)

	// Start the server.
	log := storageOpts.InstrumentOptions().Logger()
	require.NoError(t, testSetup.StartServer())

	// Stop the server.
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
	}()

	start := time.Now()
	session, err := testSetup.M3DBClient().DefaultSession()
	require.NoError(t, err)
	nowFn := testSetup.NowFn()

	tags := []ident.Tag{
		ident.StringTag("__name__", "cpu"),
		ident.StringTag("job", "job1"),
	}

	dpTimeStart := nowFn().Truncate(indexBlockSize).Add(-3 * indexBlockSize)
	dpTime := dpTimeStart

	// Write test data.
	for a := 0.0; a < 60.0; a++ {
		err = session.WriteTagged(srcNs.ID(), ident.StringID("foo"), ident.NewTagsIterator(ident.NewTags(tags...)), dpTime, 42.1+a, xtime.Second, nil)
		require.NoError(t, err)
		dpTime = dpTime.Add(10 * time.Minute)
	}
	log.Info("test data written", zap.Duration("took", time.Since(start)))

	log.Info("waiting till data is cold flushed")
	start = time.Now()
	expectedNumWrites := int64(20)
	flushed := xclock.WaitUntil(func() bool {
		counters := reporter.Counters()
		flushes, _ := counters["database.flushIndex.success"]
		writes, _ := counters["database.series.cold-writes"]
		return flushes >= 1 && writes >= expectedNumWrites
	}, time.Minute)
	require.True(t, flushed)
	log.Info("verified data has been cold flushed", zap.Duration("took", time.Since(start)))

	aggOpts, err := storage.NewAggregateTilesOptions(dpTimeStart, dpTimeStart.Add(blockSizeT), time.Hour, false)
	require.NoError(t, err)

	log.Info("Starting aggregation")
	start = time.Now()
	processedBlockCount, err := testSetup.DB().AggregateTiles(storageOpts.ContextPool().Get(), srcNs.ID(), trgNs.ID(), aggOpts)
	log.Info("Finished aggregation", zap.Duration("took", time.Since(start)))
	require.NoError(t, err)
	assert.Equal(t, int64(6), processedBlockCount)

	counters := reporter.Counters()
	writeErrorsCount, _ := counters["database.writeAggData.errors"]
	require.Equal(t, int64(0), writeErrorsCount)
	seriesWritesCount, _ := counters["dbshard.large-tiles-writes"]
	require.Equal(t, int64(1), seriesWritesCount)

	log.Info("fetching aggregated data")
	series, err := session.Fetch(trgNs.ID(), ident.StringID("foo"), dpTimeStart, nowFn())
	require.NoError(t, err)

	expectedDps := map[int64]float64{
		dpTimeStart.Add(50 * time.Minute).Unix():  47.1,
		dpTimeStart.Add(110 * time.Minute).Unix(): 53.1,
		dpTimeStart.Add(170 * time.Minute).Unix(): 59.1,
		dpTimeStart.Add(230 * time.Minute).Unix(): 65.1,
		dpTimeStart.Add(290 * time.Minute).Unix(): 71.1,
		dpTimeStart.Add(350 * time.Minute).Unix(): 77.1,
	}

	count := 0
	for series.Next() {
		dp, _, _ := series.Current()
		fmt.Println(dp)
		value, ok := expectedDps[dp.Timestamp.Unix()]
		require.True(t, ok,
			"didn't expect to find timestamp %v in aggregated result",
			dp.Timestamp.Unix())
		require.Equal(t, value, dp.Value,
			"value for timestamp %v doesn't match. Expected %v but got %v",
			dp.Timestamp.Unix(), value, dp.Value)
		count++
	}
	require.Equal(t, len(expectedDps), count)
}
