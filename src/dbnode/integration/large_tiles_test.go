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
	"io"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	blockSize       = 2 * time.Hour
	blockSizeT      = 6 * time.Hour
	indexBlockSize  = 2 * blockSize
	indexBlockSizeT = 2 * blockSizeT
)

//func TestReadAggregateWrite(t *testing.T) {
//	testSetup, srcNs, trgNs, reporter, closer := setupServer(t)
//	storageOpts := testSetup.StorageOpts()
//	log := storageOpts.InstrumentOptions().Logger()
//
//	// Stop the server.
//	defer func() {
//		require.NoError(t, testSetup.StopServer())
//		log.Debug("server is now down")
//		testSetup.Close()
//		closer.Close()
//	}()
//
//	start := time.Now()
//	session, err := testSetup.M3DBClient().DefaultSession()
//	require.NoError(t, err)
//	nowFn := testSetup.NowFn()
//
//	tags := []ident.Tag{
//		ident.StringTag("__name__", "cpu"),
//		ident.StringTag("job", "job1"),
//	}
//
//	// Write test data.
//	dpTimeStart := nowFn().Truncate(indexBlockSizeT).Add(-2 * indexBlockSizeT)
//	dpTime := dpTimeStart
//	err = session.WriteTagged(srcNs.ID(), ident.StringID("aab"), ident.NewTagsIterator(ident.NewTags(tags...)), dpTime, 15, xtime.Second, nil)
//
//	testDataPointsCount := 60.0
//	for a := 0.0; a < testDataPointsCount; a++ {
//		if a < 10 {
//			dpTime = dpTime.Add(10 * time.Minute)
//			continue
//		}
//		err = session.WriteTagged(srcNs.ID(), ident.StringID("foo"), ident.NewTagsIterator(ident.NewTags(tags...)), dpTime, 42.1+a, xtime.Second, nil)
//		require.NoError(t, err)
//		dpTime = dpTime.Add(10 * time.Minute)
//	}
//	log.Info("test data written", zap.Duration("took", time.Since(start)))
//
//	log.Info("waiting till data is cold flushed")
//	start = time.Now()
//	//expectedNumWrites := int64(testDataPointsCount)
//	flushed := xclock.WaitUntil(func() bool {
//		counters := reporter.Counters()
//		flushes, _ := counters["database.flushIndex.success"]
//		writes, _ := counters["database.series.cold-writes"]
//		successFlushes, _ := counters["database.flushColdData.success"]
//		return flushes >= 1 && writes >= 30 && successFlushes >= 4
//	}, time.Minute)
//	require.True(t, flushed)
//	log.Info("verified data has been cold flushed", zap.Duration("took", time.Since(start)))
//
//	aggOpts, err := storage.NewAggregateTilesOptions(dpTimeStart, dpTimeStart.Add(blockSizeT), time.Hour, false)
//	require.NoError(t, err)
//
//	log.Info("Starting aggregation")
//	start = time.Now()
//	processedBlockCount, err := testSetup.DB().AggregateTiles(storageOpts.ContextPool().Get(), srcNs.ID(), trgNs.ID(), aggOpts)
//	log.Info("Finished aggregation", zap.Duration("took", time.Since(start)))
//	require.NoError(t, err)
//	assert.Equal(t, int64(6), processedBlockCount)
//
//	counters := reporter.Counters()
//	writeErrorsCount, _ := counters["database.writeAggData.errors"]
//	require.Equal(t, int64(0), writeErrorsCount)
//	seriesWritesCount, _ := counters["dbshard.large-tiles-writes"]
//	require.Equal(t, int64(2), seriesWritesCount)
//
//	log.Info("validating aggregated data")
//	expectedDps := map[int64]float64{
//		dpTimeStart.Add(110 * time.Minute).Unix(): 53.1,
//		dpTimeStart.Add(170 * time.Minute).Unix(): 59.1,
//		dpTimeStart.Add(230 * time.Minute).Unix(): 65.1,
//		dpTimeStart.Add(290 * time.Minute).Unix(): 71.1,
//		dpTimeStart.Add(350 * time.Minute).Unix(): 77.1,
//	}
//	fetchAndValidate(t, session, srcNs.ID(), ident.StringID("foo"), dpTimeStart, nowFn(), expectedDps)
//
//	expectedDps = map[int64]float64{
//		dpTimeStart.Unix(): 15,
//	}
//	fetchAndValidate(t, session, trgNs.ID(), ident.StringID("aab"), dpTimeStart, nowFn(), expectedDps)
//}

func TestAggregationAndQueryingAtHighConcurrency(t *testing.T) {
	testSetup, srcNs, trgNs, reporter, closer := setupServer(t)
	storageOpts := testSetup.StorageOpts()
	log := storageOpts.InstrumentOptions().Logger()

	// Stop the server.
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
		testSetup.Close()
		closer.Close()
	}()

	start := time.Now()
	session, err := testSetup.M3DBClient().DefaultSession()
	require.NoError(t, err)
	nowFn := testSetup.NowFn()

	dpTimeStart := nowFn().Truncate(indexBlockSizeT).Add(-2 * indexBlockSizeT)
	writeTestData(t, testSetup, dpTimeStart, srcNs.ID())
	log.Info("test data written", zap.Duration("took", time.Since(start)))

	log.Info("waiting till data is cold flushed")
	start = time.Now()
	flushed := xclock.WaitUntil(func() bool {
		counters := reporter.Counters()
		flushes, _ := counters["database.flushIndex.success"]
		writes, _ := counters["database.series.cold-writes"]
		successFlushes, _ := counters["database.flushColdData.success"]
		return flushes >= 1 && writes >= 600000 && successFlushes >= 4
	}, time.Minute)
	require.True(t, flushed)
	log.Info("verified data has been cold flushed", zap.Duration("took", time.Since(start)))

	aggOpts, err := storage.NewAggregateTilesOptions(dpTimeStart, dpTimeStart.Add(blockSizeT), 10*time.Minute, false)
	require.NoError(t, err)

	log.Info("Starting aggregation loop")
	start = time.Now()

	inProgress := atomic.NewBool(true)
	var wg sync.WaitGroup
	for b := 0; b < 2; b++ {
		b := b
		wg.Add(1)
		go func() {
			defer wg.Done()
			for inProgress.Load() {
				//fmt.Printf("Fetch...")
				series, err := session.Fetch(srcNs.ID(), ident.StringID("foo"+string(b)), dpTimeStart, dpTimeStart.Add(blockSizeT))
				count := 0
				if err == nil {
					for series.Next() {
						_, _, _ = series.Current()
						count++
					}
				}

				//fmt.Println(count, "Done")
				if err != nil {
					fmt.Println(time.Now(), "FETCH ERR", err)
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}

	var (
		processedBlockCount int64
	)
	for a := 0; a < 10; a++ {
		fmt.Println(time.Now(), "Aggregation", a)
		processedBlockCount, err = testSetup.DB().AggregateTiles(storageOpts.ContextPool().Get(), srcNs.ID(), trgNs.ID(), aggOpts)
		if err != nil {
			fmt.Println(time.Now(), "AGG ERR", err)
		}
		fmt.Println("Aggregation-Done", processedBlockCount)
		if err != nil || processedBlockCount != 36000 {
			break
		}
	}
	inProgress.Toggle()
	log.Info("Finished aggregation", zap.Duration("took", time.Since(start)))
	wg.Wait()
	require.NoError(t, err)
	require.Equal(t, int64(36000), processedBlockCount)

	counters := reporter.Counters()
	writeErrorsCount, _ := counters["database.writeAggData.errors"]
	require.Equal(t, int64(0), writeErrorsCount)
	seriesWritesCount, _ := counters["dbshard.large-tiles-writes"]
	require.Equal(t, int64(10000), seriesWritesCount)

	_, err = session.Fetch(srcNs.ID(), ident.StringID("foo"+string(50)), dpTimeStart, dpTimeStart.Add(blockSizeT))
	require.NoError(t, err)
}

func fetchAndValidate(
	t *testing.T,
	session client.Session,
	nsID ident.ID,
	id ident.ID,
	startInclusive, endExclusive time.Time,
	expectedDps map[int64]float64,
) {
	series, err := session.Fetch(nsID, id, startInclusive, endExclusive)
	require.NoError(t, err)

	count := 0
	for series.Next() {
		dp, _, _ := series.Current()
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

func setupServer(t *testing.T) (TestSetup, namespace.Metadata, namespace.Metadata, xmetrics.TestStatsReporter, io.Closer) {
	var (
		rOpts    = retention.NewOptions().SetRetentionPeriod(76 * blockSize).SetBlockSize(blockSize)
		rOptsT   = retention.NewOptions().SetRetentionPeriod(76 * blockSize).SetBlockSize(blockSizeT)
		idxOpts  = namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(indexBlockSize)
		idxOptsT = namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(indexBlockSizeT)
		nsOpts   = namespace.NewOptions().
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
		SetWriteNewSeriesAsync(true).
		SetNumShards(1)

	testSetup := newTestSetupWithCommitLogAndFilesystemBootstrapper(t, testOpts)
	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(
		tally.ScopeOptions{Reporter: reporter}, time.Millisecond)
	testSetup.SetStorageOpts(testSetup.StorageOpts().SetInstrumentOptions(
		instrument.NewOptions().SetMetricsScope(scope)))

	storageOpts := testSetup.StorageOpts()
	testSetup.SetStorageOpts(storageOpts)

	// Start the server.
	require.NoError(t, testSetup.StartServer())

	return testSetup, srcNs, trgNs, reporter, closer
}

func writeTestData(t *testing.T, testSetup TestSetup, dpTimeStart time.Time, ns ident.ID) {
	tags := []ident.Tag{
		ident.StringTag("__name__", "cpu"),
		ident.StringTag("job", "job1"),
	}

	dpTime := dpTimeStart

	testDataPointsCount := 600.0
	testSeriesCount := 1000

	testTagEncodingPool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(),
		pool.NewObjectPoolOptions().SetSize(1))
	testTagEncodingPool.Init()
	encoder := testTagEncodingPool.Get()
	tagsIter := ident.NewTagsIterator(ident.NewTags(tags...))
	err := encoder.Encode(tagsIter)
	require.NoError(t, err)

	encodedTags, ok := encoder.Data()
	require.True(t, ok)
	encodedTagsBytes := encodedTags.Bytes()

	i := 0
	for a := 0.0; a < testDataPointsCount; a++ {
		batchWriter, err := testSetup.DB().BatchWriter(ns, int(testDataPointsCount))
		require.NoError(t, err)

		for b := 0; b < testSeriesCount; b++ {
			tagsIter.Rewind()
			err := batchWriter.AddTagged(i, ident.StringID("foo"+string(b)), tagsIter, encodedTagsBytes, dpTime, 42.1+a, xtime.Second, nil)
			require.NoError(t, err)
			i++
		}
		err = testSetup.DB().WriteTaggedBatch(context.NewContext(), ns, batchWriter, nil)
		require.NoError(t, err)

		dpTime = dpTime.Add(time.Minute)
	}
}
