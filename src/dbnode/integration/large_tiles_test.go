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
	"os"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/index"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3/src/m3ninx/idx"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

func TestReadAggregateWrite(t *testing.T) {
	var (
		blockSize      = 2 * time.Hour
		largeBlockSize = 6 * blockSize
		indexBlockSize = 2 * blockSize
		srcROpts       = retention.NewOptions().SetRetentionPeriod(24 * blockSize).SetBlockSize(blockSize)
		trgROpts       = retention.NewOptions().SetRetentionPeriod(20 * largeBlockSize).SetBlockSize(largeBlockSize)
		srcIdxOpts     = namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(indexBlockSize)
		trgIdxOpts     = namespace.NewIndexOptions().SetEnabled(false)
		srcNsOpts      = namespace.NewOptions().
				SetRetentionOptions(srcROpts).
				SetIndexOptions(srcIdxOpts).
				SetColdWritesEnabled(true)
		trgNsOpts = namespace.NewOptions().
				SetRetentionOptions(trgROpts).
				SetIndexOptions(trgIdxOpts).
				SetColdWritesEnabled(true)
	)

	os.Setenv("RAW_NAMESPACE", testNamespaces[0].String())
	srcNs, err := namespace.NewMetadata(testNamespaces[0], srcNsOpts)
	require.NoError(t, err)
	trgNs, err := namespace.NewMetadata(testNamespaces[1], trgNsOpts)
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

	dpTimeStart := nowFn().Truncate(largeBlockSize).Add(-2 * largeBlockSize)
	dpTime := dpTimeStart

	// Write test data.
	for a := 0.0; a < 20.0; a++ {
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
		counter, ok := counters["database.series.cold-writes"]     // Wait until data is written
		warmData, ok := counters["database.flushWarmData.success"] // Wait until data is flushed
		return ok && counter == expectedNumWrites && warmData >= expectedNumWrites
	}, time.Minute)
	require.True(t, flushed)
	log.Info("verified data has been cold flushed", zap.Duration("took", time.Since(start)))

	aggOpts := storage.AggregateTilesOptions{
		Start: dpTimeStart,
		End:   dpTimeStart.Add(largeBlockSize),
		Step:  time.Hour,
	}

	// Retry aggregation as persist manager could be still locked by cold writes.
	// TODO: Remove retry when a separate persist manager will be implemented.
	for retries := 0; retries < 10; retries++ {
		err = testSetup.DB().AggregateTiles(storageOpts.ContextPool().Get(), srcNs.ID(), trgNs.ID(), aggOpts)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	require.NoError(t, err)

	log.Info("querying aggregated data")
	query := index.Query{
		Query: idx.NewTermQuery([]byte("job"), []byte("job1"))}
	result, _, err := session.FetchTagged(trgNs.ID(), query, index.QueryOptions{StartInclusive: dpTimeStart, EndExclusive: nowFn()})
	require.NoError(t, err)

	expectedDps := make(map[int64]float64)
	// TODO: Replace with exact values when aggregation will be implemented.
	timestamp := dpTimeStart
	// TODO: now we aggregate only a single block, that's why we do expect
	// 12 items in place of 20
	for a := 0; a < 12; a++ {
		expectedDps[timestamp.Unix()] = (42.1 + float64(a)) * 2
		timestamp = timestamp.Add(10 * time.Minute)
	}

	count := 0
	for _, iter := range result.Iters() {
		for iter.Next() {
			dp, _, _ := iter.Current()
			value, ok := expectedDps[dp.Timestamp.Unix()]
			require.True(t, ok,
				"didn't expect to find timestamp %v in aggregated result",
				dp.Timestamp.Unix())
			require.Equal(t, value, dp.Value,
				"value for timestamp %v doesn't match. Expected %v but got %v",
				dp.Timestamp.Unix(), value, dp.Value)
			count++
		}
	}
	require.Equal(t, len(expectedDps), count)
}
