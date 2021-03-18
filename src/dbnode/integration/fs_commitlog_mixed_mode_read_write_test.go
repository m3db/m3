// +build integration

// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/m3db/m3/src/dbnode/testdata/prototest"
	"github.com/stretchr/testify/require"
)

type annotationGenerator interface {
	Next() []byte
}

func TestFsCommitLogMixedModeReadWrite(t *testing.T) {
	testFsCommitLogMixedModeReadWrite(t, nil, nil)
}

func TestProtoFsCommitLogMixedModeReadWrite(t *testing.T) {
	testFsCommitLogMixedModeReadWrite(t, setProtoTestOptions, prototest.NewProtoMessageIterator(testProtoMessages))
}

func testFsCommitLogMixedModeReadWrite(t *testing.T, setTestOpts setTestOptions, annGen annotationGenerator) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	var (
		ns1BlockSize = 1 * time.Hour
		ns1ROpts     = retention.NewOptions().SetRetentionPeriod(3 * time.Hour).SetBlockSize(ns1BlockSize)
		nsID         = testNamespaces[0]
	)

	ns1Opts := namespace.NewOptions().
		SetRetentionOptions(ns1ROpts)
	ns1, err := namespace.NewMetadata(nsID, ns1Opts)
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1})

	if setTestOpts != nil {
		opts = setTestOpts(t, opts)
		ns1 = opts.Namespaces()[0]
	}

	// Test setup
	setup := newTestSetupWithCommitLogAndFilesystemBootstrapper(t, opts)
	defer setup.Close()

	log := setup.StorageOpts().InstrumentOptions().Logger()
	log.Info("commit log & fileset files, write, read, and merge bootstrap test")

	filePathPrefix := setup.StorageOpts().CommitLogOptions().FilesystemOptions().FilePathPrefix()

	// setting time to 2017/02/13 15:30:10
	fakeStart := time.Date(2017, time.February, 13, 15, 30, 10, 0, time.Local)
	blkStart15 := fakeStart.Truncate(ns1BlockSize)
	blkStart16 := blkStart15.Add(ns1BlockSize)
	blkStart17 := blkStart16.Add(ns1BlockSize)
	blkStart18 := blkStart17.Add(ns1BlockSize)
	setup.SetNowFn(fakeStart)

	// startup server
	log.Debug("starting server")
	startServerWithNewInspection(t, opts, setup)
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		log.Debug("stopping server")
		require.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	// mimic a run of 200 minutes,
	// should flush data for hour 15, 16, 17
	// should have 50 mins of data in hour 18
	var (
		total = 200
		ids   = &idGen{longTestID}
		db    = setup.DB()
		ctx   = context.NewBackground()
	)
	defer ctx.Close()
	log.Info("writing datapoints")
	datapoints := generateDatapoints(fakeStart, total, ids, annGen)
	for _, dp := range datapoints {
		ts := dp.time
		setup.SetNowFn(ts)
		require.NoError(t, db.Write(ctx, nsID, dp.series, ts, dp.value, xtime.Second, dp.ann))
	}
	log.Info("wrote datapoints")

	// verify in-memory data matches what we expect
	expectedSeriesMap := datapoints.toSeriesMap(ns1BlockSize)
	log.Info("verifying data in database equals expected data")
	verifySeriesMaps(t, setup, nsID, expectedSeriesMap)
	log.Info("verified data in database equals expected data")

	// current time is 18:50, so we expect data for block starts [15, 18) to be written out
	// to fileset files, and flushed.
	expectedFlushedData := datapoints.toSeriesMap(ns1BlockSize)
	delete(expectedFlushedData, xtime.ToUnixNano(blkStart18))
	waitTimeout := 5 * time.Minute

	log.Info("waiting till expected fileset files have been written")
	require.NoError(t, waitUntilDataFilesFlushed(filePathPrefix, setup.ShardSet(), nsID, expectedFlushedData, waitTimeout))
	log.Info("expected fileset files have been written")

	// stopping db
	log.Info("stopping database")
	require.NoError(t, setup.StopServer())
	log.Info("database stopped")

	// the time now is 18:55
	setup.SetNowFn(setup.NowFn()().Add(5 * time.Minute))

	// recreate the db from the data files and commit log
	// should contain data from 15:30 - 17:59 on disk and 18:00 - 18:50 in mem
	log.Info("re-opening database & bootstrapping")
	startServerWithNewInspection(t, opts, setup)
	log.Info("verifying data in database equals expected data")
	verifySeriesMaps(t, setup, nsID, expectedSeriesMap)
	log.Info("verified data in database equals expected data")

	// the time now is 19:15
	setup.SetNowFn(setup.NowFn()().Add(20 * time.Minute))
	// data from hour 15 is now outdated, ensure the file has been cleaned up
	log.Info("waiting till expired fileset files have been cleanedup")
	require.NoError(t, waitUntilFileSetFilesCleanedUp(setup, nsID, blkStart15, waitTimeout))
	log.Info("fileset files have been cleaned up")

	// stopping db
	log.Info("stopping database")
	require.NoError(t, setup.StopServer())
	log.Info("database stopped")

	// recreate the db from the data files and commit log
	log.Info("re-opening database & bootstrapping")
	startServerWithNewInspection(t, opts, setup)

	// verify in-memory data matches what we expect
	// should contain data from 16:00 - 17:59 on disk and 18:00 - 18:50 in mem
	delete(expectedSeriesMap, xtime.ToUnixNano(blkStart15))
	log.Info("verifying data in database equals expected data")
	verifySeriesMaps(t, setup, nsID, expectedSeriesMap)
	log.Info("verified data in database equals expected data")
}

// We use this helper method to start the server so that a new filesystem
// inspection and commitlog bootstrapper are generated each time.
func startServerWithNewInspection(
	t *testing.T,
	opts TestOptions,
	setup TestSetup,
) {
	setCommitLogAndFilesystemBootstrapper(t, opts, setup)
	require.NoError(t, setup.StartServer())
}

func waitUntilFileSetFilesCleanedUp(
	setup TestSetup,
	namespace ident.ID,
	toDelete time.Time,
	timeout time.Duration,
) error {
	var (
		shardSet       = setup.ShardSet()
		filesetFiles   = []cleanupTimesFileSet{}
		commitLogFiles = cleanupTimesCommitLog{
			clOpts: setup.StorageOpts().CommitLogOptions(),
		}
	)
	for _, id := range shardSet.AllIDs() {
		filesetFiles = append(filesetFiles, cleanupTimesFileSet{
			filePathPrefix: setup.FilePathPrefix(),
			namespace:      namespace,
			shard:          id,
			times:          []time.Time{toDelete},
		})
	}
	return waitUntilDataCleanedUpExtended(filesetFiles, commitLogFiles, timeout)
}

func newTestSetupWithCommitLogAndFilesystemBootstrapper(t *testing.T, opts TestOptions) TestSetup {
	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)

	setCommitLogAndFilesystemBootstrapper(t, opts, setup)

	return setup
}

func setCommitLogAndFilesystemBootstrapper(t *testing.T, opts TestOptions, setup TestSetup) TestSetup {
	commitLogOpts := setup.StorageOpts().CommitLogOptions()

	commitLogOpts = commitLogOpts.
		SetFlushInterval(defaultIntegrationTestFlushInterval)
	setup.SetStorageOpts(setup.StorageOpts().SetCommitLogOptions(commitLogOpts))

	require.NoError(t, setup.InitializeBootstrappers(InitializeBootstrappersOptions{
		CommitLogOptions: commitLogOpts,
		WithCommitLog:    true,
		WithFileSystem:   true,
	}))

	// Need to make sure we have an active m3dbAdminClient because the previous one
	// may have been shutdown by StopServer().
	setup.MaybeResetClients()

	return setup
}

func generateDatapoints(start time.Time, numPoints int, ig *idGen, annGen annotationGenerator) dataPointsInTimeOrder {
	var points dataPointsInTimeOrder
	for i := 0; i < numPoints; i++ {
		t := start.Add(time.Duration(i) * time.Minute)
		if annGen == nil {
			points = append(points,
				seriesDatapoint{
					series: ig.base(),
					time:   t,
					value:  float64(i),
				},
				seriesDatapoint{
					series: ig.nth(i),
					time:   t,
					value:  float64(i),
				},
			)
		} else {
			annBytes := annGen.Next()
			points = append(points,
				seriesDatapoint{
					series: ig.base(),
					time:   t,
					ann:    annBytes,
				},
				seriesDatapoint{
					series: ig.nth(i),
					time:   t,
					ann:    annBytes,
				},
			)
		}
	}
	return points
}

type dataPointsInTimeOrder []seriesDatapoint

type seriesDatapoint struct {
	series ident.ID
	time   time.Time
	value  float64
	ann    []byte
}

func (d dataPointsInTimeOrder) toSeriesMap(blockSize time.Duration) generate.SeriesBlocksByStart {
	blockStartToSeriesMap := make(map[xtime.UnixNano]map[string]generate.Series)
	for _, point := range d {
		t := point.time
		trunc := t.Truncate(blockSize)
		seriesBlock, ok := blockStartToSeriesMap[xtime.ToUnixNano(trunc)]
		if !ok {
			seriesBlock = make(map[string]generate.Series)
		}
		idString := point.series.String()
		dp, ok := seriesBlock[idString]
		if !ok {
			dp = generate.Series{ID: point.series}
		}
		dp.Data = append(dp.Data, generate.TestValue{Datapoint: ts.Datapoint{
			Timestamp:      t,
			TimestampNanos: xtime.ToUnixNano(t),
			Value:          point.value,
		}, Annotation: point.ann})
		seriesBlock[idString] = dp
		blockStartToSeriesMap[xtime.ToUnixNano(trunc)] = seriesBlock
	}

	seriesMap := make(generate.SeriesBlocksByStart, len(blockStartToSeriesMap))
	for t, serieses := range blockStartToSeriesMap {
		seriesSlice := make([]generate.Series, 0, len(serieses))
		for _, series := range serieses {
			seriesSlice = append(seriesSlice, series)
		}
		seriesMap[t] = seriesSlice
	}
	return seriesMap
}

// before returns a slice of the dataPointsInTimeOrder that are before the
// specified time t.
func (d dataPointsInTimeOrder) before(t time.Time) dataPointsInTimeOrder {
	var i int
	for i = range d {
		if !d[i].time.Before(t) {
			break
		}
	}

	return d[:i]
}

type idGen struct {
	baseID string
}

func (i *idGen) base() ident.ID {
	return ident.StringID(i.baseID)
}

func (i *idGen) nth(n int) ident.ID {
	return ident.StringID(fmt.Sprintf("%s%d", i.baseID, n))
}

const (
	longTestID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)
