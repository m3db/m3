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
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/retention"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3x/context"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestFsCommitLogMixedModeReadWriteProp(t *testing.T) {
	testMixedModeReadWriteProp(t, false)
}

func testMixedModeReadWriteProp(t *testing.T, snapshotEnabled bool) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	var (
		ns1BlockSize = 1 * time.Hour
		// setting time to 2017/02/13 15:30:10
		fakeStart          = time.Date(2017, time.February, 13, 15, 30, 10, 0, time.Local)
		blkStart15         = fakeStart.Truncate(ns1BlockSize)
		blkStart16         = blkStart15.Add(ns1BlockSize)
		blkStart17         = blkStart16.Add(ns1BlockSize)
		blkStart18         = blkStart17.Add(ns1BlockSize)
		timesToCheck       = []time.Time{blkStart15, blkStart16, blkStart17, blkStart18}
		commitLogBlockSize = 15 * time.Minute
		commitLogRetetion  = 6 * time.Hour
		bufferPast         = 5 * time.Minute
		bufferFuture       = 10 * time.Minute
		ns1ROpts           = retention.NewOptions().
					SetRetentionPeriod(10 * time.Hour).
					SetBlockSize(ns1BlockSize).
					SetBufferPast(bufferPast).
					SetBufferFuture(bufferFuture)
		nsID       = testNamespaces[0]
		numMinutes = 200
	)

	ns1Opts := namespace.NewOptions().
		SetRetentionOptions(ns1ROpts).
		SetSnapshotEnabled(snapshotEnabled)
	ns1, err := namespace.NewMetadata(nsID, ns1Opts)
	require.NoError(t, err)
	opts := newTestOptions(t).
		SetCommitLogRetentionPeriod(commitLogRetetion).
		SetCommitLogBlockSize(commitLogBlockSize).
		SetNamespaces([]namespace.Metadata{ns1})

	// Test setup
	setup := newTestSetupWithCommitLogAndFilesystemBootstrapper(t, opts)
	defer setup.close()

	log := setup.storageOpts.InstrumentOptions().Logger()
	log.Info("commit log & fileset files, write, read, and merge bootstrap test")

	setup.setNowFn(fakeStart)

	var (
		ids        = &idGen{longTestID}
		datapoints = generateDatapoints(fakeStart, numMinutes, ids)
		lastIdx    = 0
	)
	for _, timeToCheck := range timesToCheck {
		startServerWithNewInspection(t, opts, setup)
		var (
			ctx = context.NewContext()
		)
		defer ctx.Close()

		log.Infof("writing datapoints")
		for i := lastIdx; ; i++ {
			var (
				dp = datapoints[i]
				ts = dp.time
			)
			if !ts.Before(timeToCheck) {
				lastIdx = i
				break
			}

			setup.setNowFn(ts)
			require.NoError(t, setup.db.Write(ctx, nsID, dp.series, ts, dp.value, xtime.Second, nil))
		}
		log.Infof("wrote datapoints")

		expectedSeriesMap := datapoints[:lastIdx].toSeriesMap(ns1BlockSize)
		log.Infof("verifying data in database equals expected data")
		verifySeriesMaps(t, setup, nsID, expectedSeriesMap)
		log.Infof("verified data in database equals expected data")
		require.NoError(t, setup.stopServer())
	}
}
