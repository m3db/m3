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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/generated/proto/annotation"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/ts"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	blockSize  = 2 * time.Hour
	blockSizeT = 24 * time.Hour
	testDataPointsCount = 60
)

var (
	gaugePayload   = &annotation.Payload{MetricType: annotation.MetricType_GAUGE}
	counterPayload = &annotation.Payload{MetricType: annotation.MetricType_COUNTER, HandleValueResets: true}
)

func TestReadAggregateWrite(t *testing.T) {
	t.Skip("flaky")
	var (
		start                   = time.Now()
		testSetup, srcNs, trgNs = setupServer(t)
		storageOpts             = testSetup.StorageOpts()
		log                     = storageOpts.InstrumentOptions().Logger()
	)

	// Stop the server.
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
		testSetup.Close()
	}()

	session, err := testSetup.M3DBClient().DefaultSession()
	require.NoError(t, err)
	nowFn := storageOpts.ClockOptions().NowFn()

	// Write test data.
	dpTimeStart := nowFn().Truncate(blockSizeT)

	// "aab" ID is stored to the same shard 0 same as "foo", this is important
	// for a test to store them to the same shard to test data consistency
	err = session.WriteTagged(srcNs.ID(), ident.StringID("aab"),
		ident.MustNewTagStringsIterator("__name__", "cpu", "job", "job1"),
		dpTimeStart, 15, xtime.Second, annotationBytes(t, gaugePayload))

	dpTime := dpTimeStart
	for a := 0; a < testDataPointsCount; a++ {
		if a < 10 {
			dpTime = dpTime.Add(10 * time.Minute)
			continue
		}
		err = session.WriteTagged(srcNs.ID(), ident.StringID("foo"),
			ident.MustNewTagStringsIterator("__name__", "cpu", "job", "job1"),
			dpTime, 42.1+float64(a), xtime.Nanosecond, annotationBytes(t, counterPayload))
		require.NoError(t, err)
		dpTime = dpTime.Add(10 * time.Minute)
	}
	log.Info("test data written", zap.Duration("took", time.Since(start)))

	log.Info("waiting till data is cold flushed")
	start = time.Now()
	expectedSourceBlocks := 5
	flushed := xclock.WaitUntil(func() bool {
		for i := 0; i < expectedSourceBlocks; i++ {
			blockStart := dpTimeStart.Add(time.Duration(i) * blockSize)
			_, ok, err := fs.FileSetAt(testSetup.FilesystemOpts().FilePathPrefix(), srcNs.ID(), 0, blockStart, 1)
			require.NoError(t, err)
			if !ok {
				return false
			}
		}
		return true
	}, time.Minute)
	require.True(t, flushed)
	log.Info("verified data has been cold flushed", zap.Duration("took", time.Since(start)))

	aggOpts, err := storage.NewAggregateTilesOptions(
		dpTimeStart, dpTimeStart.Add(blockSizeT), time.Hour,
		trgNs.ID(),
		storageOpts.InstrumentOptions(),
	)
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

	log.Info("waiting till aggregated data is readable")
	start = time.Now()
	readable := xclock.WaitUntil(func() bool {
		series, err := session.Fetch(trgNs.ID(), ident.StringID("foo"), dpTimeStart, dpTimeStart.Add(blockSizeT))
		require.NoError(t, err)
		return series.Next()
	}, time.Minute)
	require.True(t, readable)
	log.Info("verified data is readable", zap.Duration("took", time.Since(start)))

	expectedDps := []ts.Datapoint{
		{Timestamp: dpTimeStart, Value: 15},
	}
	fetchAndValidate(t, session, trgNs.ID(),
		ident.StringID("aab"),
		dpTimeStart, nowFn(),
		expectedDps, xtime.Second, gaugePayload)

	expectedDps = []ts.Datapoint{
		{Timestamp: dpTimeStart.Add(100 * time.Minute), Value: 52.1},
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
		expectedDps, xtime.Nanosecond, counterPayload)
}

func fetchAndValidate(
	t *testing.T,
	session client.Session,
	nsID ident.ID,
	id ident.ID,
	startInclusive, endExclusive time.Time,
	expectedDP []ts.Datapoint,
	expectedUnit xtime.Unit,
	expectedAnnotation *annotation.Payload,
) {
	series, err := session.Fetch(nsID, id, startInclusive, endExclusive)
	require.NoError(t, err)

	actual := make([]ts.Datapoint, 0, len(expectedDP))
	first := true
	for series.Next() {
		dp, unit, annotation := series.Current()
		if first {
			assert.Equal(t, expectedAnnotation, annotationPayload(t, annotation))
			first = false
		}
		assert.Equal(t, expectedUnit, unit)
		dp.TimestampNanos = 0
		actual = append(actual, dp)
	}

	assert.Equal(t, expectedDP, actual)
}

func setupServer(t *testing.T) (TestSetup, namespace.Metadata, namespace.Metadata) {
	var (
		rOpts    = retention.NewOptions().SetRetentionPeriod(500 * blockSize).SetBlockSize(blockSize).SetBufferPast(0).SetBufferFuture(0)
		rOptsT   = retention.NewOptions().SetRetentionPeriod(100 * blockSize).SetBlockSize(blockSizeT).SetBufferPast(0).SetBufferFuture(0)
		idxOpts  = namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(blockSize)
		idxOptsT = namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(blockSizeT)
		nsOpts   = namespace.NewOptions().
				SetRetentionOptions(rOpts).
				SetIndexOptions(idxOpts).
				SetColdWritesEnabled(true)
		nsOptsT = namespace.NewOptions().
			SetRetentionOptions(rOptsT).
			SetIndexOptions(idxOptsT)

		fixedNow = time.Now().Truncate(blockSizeT).Add(11*blockSize)
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

	// Start the server.
	require.NoError(t, testSetup.StartServer())

	return testSetup, srcNs, trgNs
}

func annotationBytes(t *testing.T, payload *annotation.Payload) ts.Annotation {
	if payload != nil {
		annotationBytes, err := payload.Marshal()
		require.NoError(t, err)
		return annotationBytes
	}
	return nil
}

func annotationPayload(t *testing.T, annotationBytes ts.Annotation) *annotation.Payload {
	if annotationBytes != nil {
		payload := &annotation.Payload{}
		require.NoError(t, payload.Unmarshal(annotationBytes))
		return payload
	}
	return nil
}
