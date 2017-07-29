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

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	bcl "github.com/m3db/m3db/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestMixedModeReadWrite(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	var (
		commitLogBlockSize = 15 * time.Minute
		clROpts            = retention.NewOptions().
					SetRetentionPeriod(6 * time.Hour).
					SetBlockSize(commitLogBlockSize).
					SetBufferFuture(0).
					SetBufferPast(0)
		ns1BlockSize = 1 * time.Hour
		ns1ROpts     = retention.NewOptions().SetRetentionPeriod(3 * time.Hour).SetBlockSize(ns1BlockSize)
		nsID         = testNamespaces[0]
	)
	ns1, err := namespace.NewMetadata(nsID, namespace.NewOptions().SetRetentionOptions(ns1ROpts))
	require.NoError(t, err)
	opts := newTestOptions(t).
		SetCommitLogRetention(clROpts).
		SetNamespaces([]namespace.Metadata{ns1})

	// Test setup
	setup := newTestSetupWithCommitLogAndFilesystemBootstrapper(t, opts)
	defer func() {
		setup.close()
	}()

	log := setup.storageOpts.InstrumentOptions().Logger()
	log.Info("commit log & fileset files, write, read, and merge bootstrap test")

	// setting time to 2017/02/13 15:30:10
	fakeStart := time.Date(2017, time.February, 13, 15, 30, 10, 0, time.Local)
	blkStart15 := fakeStart.Truncate(ns1BlockSize)
	blkStart16 := blkStart15.Add(ns1BlockSize)
	blkStart17 := blkStart16.Add(ns1BlockSize)
	blkStart18 := blkStart17.Add(ns1BlockSize)
	setup.setNowFn(fakeStart)

	// startup server
	log.Debug("starting server")
	require.NoError(t, setup.startServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		log.Debug("stopping server")
		require.NoError(t, setup.stopServer())
		log.Debug("server is now down")
	}()

	// mimic a run of 200 minutes,
	// should flush data for hour 15, 16, 17
	// should have 50 mins of data in hour 18
	var (
		total = 200
		ids   = &idGen{longTestID}
		db    = setup.db
		ctx   = context.NewContext()
	)
	defer ctx.Close()
	log.Infof("writing datapoints")
	datapoints := generateDatapoints(fakeStart, total, ids)
	for _, dp := range datapoints {
		ts := dp.time
		setup.setNowFn(ts)
		require.NoError(t, db.Write(ctx, nsID, dp.series, ts, dp.value, xtime.Second, nil))
	}
	log.Infof("wrote datapoints")

	// verify in-memory data matches what we expect
	expectedSeriesMap := datapoints.toSeriesMap(ns1BlockSize)
	log.Infof("verifying data in database equals expected data")
	verifySeriesMaps(t, setup, nsID, expectedSeriesMap)
	log.Infof("verified data in database equals expected data")

	// current time is 18:50, so we expect data for block starts [15, 18) to be written out
	// to fileset files, and flushed.
	expectedFlushedData := datapoints.toSeriesMap(ns1BlockSize)
	delete(expectedFlushedData, blkStart18)
	waitTimeout := 30 * time.Second
	filePathPrefix := setup.storageOpts.CommitLogOptions().FilesystemOptions().FilePathPrefix()
	log.Infof("waiting till expected fileset files have been written")
	require.NoError(t, waitUntilDataFlushed(filePathPrefix, setup.shardSet, nsID, expectedFlushedData, waitTimeout))
	log.Infof("expected fileset files have been written")

	// stopping db
	log.Infof("stopping database")
	require.NoError(t, setup.stopServer())
	log.Infof("database stopped")

	// the time now is 18:55
	setup.setNowFn(setup.getNowFn().Add(5 * time.Minute))

	// recreate the db from the data files and commit log
	// should contain data from 15:30 - 17:59 on disk and 18:00 - 18:50 in mem
	log.Infof("re-opening database & bootstrapping")
	require.NoError(t, setup.startServer())
	log.Infof("verifying data in database equals expected data")
	verifySeriesMaps(t, setup, nsID, expectedSeriesMap)
	log.Infof("verified data in database equals expected data")

	// the time now is 19:15
	setup.setNowFn(setup.getNowFn().Add(20 * time.Minute))
	// data from hour 15 is now outdated, ensure the file has been cleaned up
	log.Infof("waiting till expired fileset files have been cleanedup")
	require.NoError(t, waitUntilFilesetFilesCleanedUp(setup, nsID, blkStart15, waitTimeout))
	log.Infof("fileset files have been cleaned up")

	// stopping db
	log.Infof("stopping database")
	require.NoError(t, setup.stopServer())
	log.Infof("database stopped")

	// recreate the db from the data files and commit log
	log.Infof("re-opening database & bootstrapping")
	require.NoError(t, setup.startServer())

	// verify in-memory data matches what we expect
	// should contain data from 16:00 - 17:59 on disk and 18:00 - 18:50 in mem
	delete(expectedSeriesMap, blkStart15)
	log.Infof("verifying data in database equals expected data")
	verifySeriesMaps(t, setup, nsID, expectedSeriesMap)
	log.Infof("verified data in database equals expected data")
}

func waitUntilFilesetFilesCleanedUp(
	setup *testSetup,
	namespace ts.ID,
	toDelete time.Time,
	timeout time.Duration,
) error {
	var (
		shardSet       = setup.shardSet
		filesetFiles   = []cleanupTimesFileset{}
		commitLogFiles = cleanupTimesCommitLog{}
	)
	for _, id := range shardSet.AllIDs() {
		filesetFiles = append(filesetFiles, cleanupTimesFileset{
			filePathPrefix: setup.filePathPrefix,
			namespace:      namespace,
			shard:          id,
			times:          []time.Time{toDelete},
		})
	}
	return waitUntilDataCleanedUpExtended(filesetFiles, commitLogFiles, timeout)
}

func newTestSetupWithCommitLogAndFilesystemBootstrapper(t *testing.T, opts testOptions) *testSetup {
	setup, err := newTestSetup(t, opts)
	require.NoError(t, err)

	commitLogOpts := setup.storageOpts.CommitLogOptions()
	fsOpts := commitLogOpts.FilesystemOptions()

	commitLogOpts = commitLogOpts.
		SetFlushInterval(defaultIntegrationTestFlushInterval)
	setup.storageOpts = setup.storageOpts.SetCommitLogOptions(commitLogOpts)

	// commit log bootstrapper
	noOpAll := bootstrapper.NewNoOpAllBootstrapper()
	bsOpts := newDefaulTestResultOptions(setup.storageOpts)
	bclOpts := bcl.NewOptions().
		SetResultOptions(bsOpts).
		SetCommitLogOptions(commitLogOpts)
	commitLogBootstrapper, err := bcl.NewCommitLogBootstrapper(bclOpts, noOpAll)
	require.NoError(t, err)

	// fs bootstrapper
	filePathPrefix := fsOpts.FilePathPrefix()
	bfsOpts := fs.NewOptions().
		SetResultOptions(bsOpts).
		SetFilesystemOptions(fsOpts)
	fsBootstrapper := fs.NewFileSystemBootstrapper(filePathPrefix, bfsOpts, commitLogBootstrapper)

	// bootstrapper storage opts
	process := bootstrap.NewProcess(fsBootstrapper, bsOpts)
	setup.storageOpts = setup.storageOpts.SetBootstrapProcess(process)

	return setup
}

func generateDatapoints(start time.Time, numPoints int, ig *idGen) dataPointsInTimeOrder {
	var points dataPointsInTimeOrder
	for i := 0; i < numPoints; i++ {
		t := start.Add(time.Duration(i) * time.Minute)
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
	}
	return points
}

type dataPointsInTimeOrder []seriesDatapoint

type seriesDatapoint struct {
	series ts.ID
	time   time.Time
	value  float64
}

type dataPointPredicate func(seriesDatapoint) bool

func (d dataPointsInTimeOrder) toSeriesMap(blockSize time.Duration) generate.SeriesBlocksByStart {
	blockStartToSeriesMap := make(map[time.Time]map[ts.Hash]generate.Series)
	for _, point := range d {
		t := point.time
		trunc := t.Truncate(blockSize)
		seriesBlock, ok := blockStartToSeriesMap[trunc]
		if !ok {
			seriesBlock = make(map[ts.Hash]generate.Series)
		}
		dp, ok := seriesBlock[point.series.Hash()]
		if !ok {
			dp = generate.Series{ID: point.series}
		}
		dp.Data = append(dp.Data, ts.Datapoint{
			Timestamp: t,
			Value:     point.value,
		})
		seriesBlock[point.series.Hash()] = dp
		blockStartToSeriesMap[trunc] = seriesBlock
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

	return nil
}

type idGen struct {
	baseID string
}

func (i *idGen) base() ts.ID {
	return ts.StringID(i.baseID)
}

func (i *idGen) nth(n int) ts.ID {
	return ts.StringID(fmt.Sprintf("%s%d", i.baseID, n))
}

const (
	longTestID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)
