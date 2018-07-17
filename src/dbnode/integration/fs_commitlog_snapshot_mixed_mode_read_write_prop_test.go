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
	"os"
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/retention"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3x/context"
	xtime "github.com/m3db/m3x/time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

const maxBlockSize = 12 * time.Hour

func TestFsCommitLogMixedModeReadWriteProp(t *testing.T) {
	testMixedModeReadWriteProp(t, false)
}

func testMixedModeReadWriteProp(t *testing.T, snapshotEnabled bool) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
		fakeStart  = time.Date(2017, time.February, 13, 15, 30, 10, 0, time.Local)
	)

	parameters.MinSuccessfulTests = 10
	parameters.Rng.Seed(seed)

	props.Property("Blah", prop.ForAll(
		func(input propTestInput) (bool, error) {
			// Test setup
			var (
				ns1BlockSize = input.blockSize
				// setting time to 2017/02/13 15:30:10
				blkStart15         = fakeStart.Truncate(ns1BlockSize)
				blkStart16         = blkStart15.Add(ns1BlockSize)
				blkStart17         = blkStart16.Add(ns1BlockSize)
				blkStart18         = blkStart17.Add(ns1BlockSize)
				timesToCheck       = []time.Time{blkStart15, blkStart16, blkStart17, blkStart18}
				commitLogBlockSize = 15 * time.Minute
				// TODO: Vary this?
				retentionPeriod = maxBlockSize * 5
				bufferPast      = input.bufferPast
				bufferFuture    = input.bufferFuture
				ns1ROpts        = retention.NewOptions().
						SetRetentionPeriod(retentionPeriod).
						SetBlockSize(ns1BlockSize).
						SetBufferPast(bufferPast).
						SetBufferFuture(bufferFuture)
				nsID       = testNamespaces[0]
				numMinutes = 200
			)

			if bufferPast > ns1BlockSize {
				ns1ROpts = ns1ROpts.SetBufferPast(ns1BlockSize - 1)
			}
			if bufferFuture > ns1BlockSize {
				ns1ROpts = ns1ROpts.SetBufferFuture(ns1BlockSize - 1)
			}

			ns1Opts := namespace.NewOptions().
				SetRetentionOptions(ns1ROpts).
				SetSnapshotEnabled(snapshotEnabled)
			ns1, err := namespace.NewMetadata(nsID, ns1Opts)
			require.NoError(t, err)
			opts := newTestOptions(t).
				SetCommitLogRetentionPeriod(retentionPeriod).
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
				for i := lastIdx; i < len(datapoints); i++ {
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
			return true, nil
		}, genPropTestInputs(fakeStart),
	))

	if !props.Run(reporter) {
		t.Errorf(
			"failed with initial seed: %d and startTime: %d",
			seed, fakeStart.UnixNano())
	}
}

func genPropTestInputs(blockStart time.Time) gopter.Gen {
	return gopter.CombineGens(
		gen.Int64Range(1, int64(maxBlockSize/2)*2),
		gen.Int64Range(1, int64(maxBlockSize/2)*2),
		gen.Int64Range(1, int64(maxBlockSize/2)*2),
	).Map(func(val interface{}) propTestInput {
		inputs := val.([]interface{})
		return propTestInput{
			blockSize:    time.Duration(inputs[0].(int64)),
			bufferPast:   time.Duration(inputs[1].(int64)),
			bufferFuture: time.Duration(inputs[2].(int64)),
		}
	})
}

type propTestInput struct {
	blockSize    time.Duration
	bufferPast   time.Duration
	bufferFuture time.Duration
}
