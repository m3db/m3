// +build integration

// Copyright (c) 2020 Uber Technologies, Inc.
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
	"io"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/ts"
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

var (
	blockSize  = 2 * time.Hour
	blockSizeT = 24 * time.Hour
)

func TestReadAggregateWrite(t *testing.T) {
	testSetup, srcNs, trgNs, reporter, closer := setupServer(t)
	storageOpts := testSetup.StorageOpts()
	log := storageOpts.InstrumentOptions().Logger()

	// Stop the server.
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
		testSetup.Close()
		_ = closer.Close()
	}()

	start := time.Now()
	session, err := testSetup.M3DBClient().DefaultSession()
	require.NoError(t, err)
	nowFn := testSetup.NowFn()

	// Write test data.
	dpTimeStart := nowFn().Truncate(blockSizeT).Add(-blockSizeT)
	dpTime := dpTimeStart
	// "aab" ID is stored to the same shard 0 same as "foo", this is important
	// for a test to store them to the same shard to test data consistency
	err = session.WriteTagged(srcNs.ID(), ident.StringID("aab"),
		ident.MustNewTagStringsIterator("__name__", "cpu", "job", "job1"),
		dpTime, 15, xtime.Second, nil)

	testDataPointsCount := 60
	for a := 0; a < testDataPointsCount; a++ {
		if a < 10 {
			dpTime = dpTime.Add(10 * time.Minute)
			continue
		}
		err = session.WriteTagged(srcNs.ID(), ident.StringID("foo"),
			ident.MustNewTagStringsIterator("__name__", "cpu", "job", "job1"),
			dpTime, 42.1+float64(a), xtime.Second, nil)
		require.NoError(t, err)
		dpTime = dpTime.Add(10 * time.Minute)
	}
	log.Info("test data written", zap.Duration("took", time.Since(start)))

	log.Info("waiting till data is cold flushed")
	start = time.Now()
	flushed := xclock.WaitUntil(func() bool {
		counters := reporter.Counters()
		flushes, _ := counters["database.flushIndex.success"]
		writes, _ := counters["database.series.cold-writes"]
		successFlushes, _ := counters["database.flushColdData.success"]
		return flushes >= 1 && writes >= 30 && successFlushes >= 4
	}, time.Minute)
	require.True(t, flushed)
	log.Info("verified data has been cold flushed", zap.Duration("took", time.Since(start)))

	aggOpts, err := storage.NewAggregateTilesOptions(
		dpTimeStart, dpTimeStart.Add(blockSizeT),
		time.Hour, false)
	require.NoError(t, err)

	log.Info("Starting aggregation")
	start = time.Now()
	processedTileCount, err := testSetup.DB().AggregateTiles(
		storageOpts.ContextPool().Get(),
		srcNs.ID(), trgNs.ID(),
		aggOpts)
	log.Info("Finished aggregation", zap.Duration("took", time.Since(start)))
	require.NoError(t, err)
	assert.Equal(t, int64(10), processedTileCount)

	log.Info("validating aggregated data")

	// check shard 0 as we wrote both aab and foo to this shard.
	flushState, err := testSetup.DB().FlushState(trgNs.ID(), 0, dpTimeStart)
	require.NoError(t, err)
	require.Equal(t, 1, flushState.ColdVersionRetrievable)
	require.Equal(t, 1, flushState.ColdVersionFlushed)

	expectedDps := []ts.Datapoint{
		{Timestamp: dpTimeStart.Add(110 * time.Minute), Value: 53.1},
		{Timestamp: dpTimeStart.Add(170 * time.Minute), Value: 59.1},
		{Timestamp: dpTimeStart.Add(230 * time.Minute), Value: 65.1},
		{Timestamp: dpTimeStart.Add(290 * time.Minute), Value: 71.1},
		{Timestamp: dpTimeStart.Add(350 * time.Minute), Value: 77.1},
		{Timestamp: dpTimeStart.Add(410 * time.Minute), Value: 83.1},
		{Timestamp: dpTimeStart.Add(470 * time.Minute), Value: 89.1},
		{Timestamp: dpTimeStart.Add(530 * time.Minute), Value: 95.1},
		{Timestamp: dpTimeStart.Add(590 * time.Minute), Value: 101.1},
	}

	fetchAndValidate(t, session, trgNs.ID(),
		ident.StringID("foo"),
		dpTimeStart, nowFn(),
		expectedDps)

	expectedDps = []ts.Datapoint{
		{Timestamp: dpTimeStart, Value: 15},
	}
	fetchAndValidate(t, session, trgNs.ID(),
		ident.StringID("aab"),
		dpTimeStart, nowFn(),
		expectedDps)
}

func fetchAndValidate(
	t *testing.T,
	session client.Session,
	nsID ident.ID,
	id ident.ID,
	startInclusive, endExclusive time.Time,
	expected []ts.Datapoint,
) {
	series, err := session.Fetch(nsID, id, startInclusive, endExclusive)
	require.NoError(t, err)

	actual := make([]ts.Datapoint, 0, len(expected))
	for series.Next() {
		dp, _, _ := series.Current()
		// FIXME: init expected timestamp nano field as well for equality, or use
		// permissive equality check instead.
		dp.TimestampNanos = 0
		actual = append(actual, dp)
	}

	assert.Equal(t, expected, actual)
}

func setupServer(t *testing.T) (TestSetup, namespace.Metadata, namespace.Metadata, xmetrics.TestStatsReporter, io.Closer) {
	var (
		rOpts    = retention.NewOptions().SetRetentionPeriod(500 * blockSize).SetBlockSize(blockSize)
		rOptsT   = retention.NewOptions().SetRetentionPeriod(100 * blockSize).SetBlockSize(blockSizeT).SetBufferPast(0)
		idxOpts  = namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(blockSize)
		idxOptsT = namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(blockSizeT)
		nsOpts   = namespace.NewOptions().
				SetRetentionOptions(rOpts).
				SetIndexOptions(idxOpts).
				SetColdWritesEnabled(true)
		nsOptsT = namespace.NewOptions().
			SetRetentionOptions(rOptsT).
			SetIndexOptions(idxOptsT)

		fixedNow = time.Now().Truncate(blockSizeT)
	)

	srcNs, err := namespace.NewMetadata(testNamespaces[0], nsOpts)
	require.NoError(t, err)
	trgNs, err := namespace.NewMetadata(testNamespaces[1], nsOptsT)
	require.NoError(t, err)

	testOpts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{srcNs, trgNs}).
		SetWriteNewSeriesAsync(true).
		SetNumShards(1).
		SetFetchRequestTimeout(time.Second * 30).
		SetNowFn(func() time.Time {
			return fixedNow
		})

	testSetup := newTestSetupWithCommitLogAndFilesystemBootstrapper(t, testOpts)
	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(
		tally.ScopeOptions{Reporter: reporter}, time.Millisecond)
	storageOpts := testSetup.StorageOpts().
		SetInstrumentOptions(instrument.NewOptions().SetMetricsScope(scope))
	testSetup.SetStorageOpts(storageOpts)

	// Start the server.
	require.NoError(t, testSetup.StartServer())

	return testSetup, srcNs, trgNs, reporter, closer
}
