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
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/integration/generate"
	"github.com/m3db/m3db/src/dbnode/retention"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

const maxBlockSize = 12 * time.Hour
const maxPoints = 1000

func TestFsCommitLogMixedModeReadWriteProp(t *testing.T) {
	testMixedModeReadWriteProp(t, true)
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
		rng        = rand.New(rand.NewSource(seed))
	)

	parameters.MinSuccessfulTests = 30
	parameters.Rng.Seed(seed)

	props.Property("Blah", prop.ForAll(
		func(input propTestInput) (bool, error) {
			// Test setup
			var (
				ns1BlockSize       = input.blockSize.Round(time.Second)
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
				nsID      = testNamespaces[0]
				numPoints = input.numPoints
			)

			if bufferPast > ns1BlockSize {
				bufferPast = ns1BlockSize - 1
				ns1ROpts = ns1ROpts.SetBufferPast(bufferPast)
			}
			if bufferFuture > ns1BlockSize {
				bufferFuture = ns1BlockSize - 1
				ns1ROpts = ns1ROpts.SetBufferFuture(bufferFuture)
			}

			if err := ns1ROpts.Validate(); err != nil {
				return false, err
			}
			s.log.Infof("blockSize: %s\n", ns1ROpts.BlockSize().String())
			s.log.Infof("bufferPast: %s\n", ns1ROpts.BufferPast().String())
			s.log.Infof("bufferFuture: %s\n", ns1ROpts.BufferFuture().String())

			ns1Opts := namespace.NewOptions().
				SetRetentionOptions(ns1ROpts).
				SetSnapshotEnabled(snapshotEnabled)
			ns1, err := namespace.NewMetadata(nsID, ns1Opts)
			if err != nil {
				return false, err
			}
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
				ids             = &idGen{longTestID}
				datapoints      = generateDatapoints(fakeStart, numPoints, ids)
				lastIdx         = 0
				earliestToCheck = datapoints[0].time.Truncate(ns1BlockSize)
				latestToCheck   = datapoints[len(datapoints)-1].time.Add(ns1BlockSize)
				timesToCheck    = []time.Time{}
				start           = earliestToCheck
				filePathPrefix  = setup.storageOpts.CommitLogOptions().FilesystemOptions().FilePathPrefix()
			)

			for {
				if start.After(latestToCheck) || start.Equal(latestToCheck) {
					break
				}

				timesToCheck = append(timesToCheck, start)
				start = start.Add(time.Duration(rng.Intn(int(maxBlockSize))))
			}
			timesToCheck = append(timesToCheck, latestToCheck)

			for _, timeToCheck := range timesToCheck {
				startServerWithNewInspection(t, opts, setup)
				var (
					ctx = context.NewContext()
				)
				defer ctx.Close()

				log.Infof("writing datapoints")
				var i int
				for i = lastIdx; i < len(datapoints); i++ {
					var (
						dp = datapoints[i]
						ts = dp.time
					)
					if !ts.Before(timeToCheck) {
						break
					}

					setup.setNowFn(ts)

					fmt.Printf("Writing %f at %s\n", dp.value, ts.String())
					err := setup.db.Write(ctx, nsID, dp.series, ts, dp.value, xtime.Second, nil)
					if err != nil {
						return false, err
					}
				}
				lastIdx = i
				log.Infof("wrote datapoints")

				expectedSeriesMap := datapoints[:lastIdx].toSeriesMap(ns1BlockSize)
				log.Infof("verifying data in database equals expected data")
				err := verifySeriesMapsReturnError(t, setup, nsID, expectedSeriesMap)
				if err != nil {
					return false, err
				}
				log.Infof("verified data in database equals expected data")
				if input.waitForFlushFiles {
					now := setup.getNowFn()
					latestFlushTime := now.Truncate(ns1BlockSize).Add(-ns1BlockSize)
					expectedFlushedData := datapoints.before(latestFlushTime.Add(-bufferPast)).toSeriesMap(ns1BlockSize)
					err := waitUntilDataFilesFlushed(
						filePathPrefix, setup.shardSet, nsID, expectedFlushedData, 10*time.Second)
					if err != nil {
						return false, err
					}
				}

				if input.waitForSnapshotFiles {
					now := setup.getNowFn()
					snapshotBlock := now.Add(-bufferPast).Truncate(ns1BlockSize)
					require.NoError(t,
						waitUntilSnapshotFilesFlushed(
							filePathPrefix,
							setup.shardSet,
							nsID,
							[]time.Time{snapshotBlock}, 10*time.Second))
				}
				require.NoError(t, setup.stopServer())
				oldNow := setup.getNowFn()
				setup = newTestSetupWithCommitLogAndFilesystemBootstrapper(
					t, opts.SetFilePathPrefix(filePathPrefix))
				setup.setNowFn(oldNow)
			}
			if lastIdx != len(datapoints) {
				return false, fmt.Errorf(
					"expected lastIdx to be: %d but was: %d", len(datapoints), lastIdx)
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
		gen.IntRange(0, maxPoints),
		gen.Bool(),
		gen.Bool(),
	).Map(func(val interface{}) propTestInput {
		inputs := val.([]interface{})
		return propTestInput{
			blockSize:            time.Duration(inputs[0].(int64)),
			bufferPast:           time.Duration(inputs[1].(int64)),
			bufferFuture:         time.Duration(inputs[2].(int64)),
			numPoints:            inputs[3].(int),
			waitForFlushFiles:    inputs[4].(bool),
			waitForSnapshotFiles: inputs[5].(bool),
		}
	})
}

type propTestInput struct {
	blockSize            time.Duration
	bufferPast           time.Duration
	bufferFuture         time.Duration
	numPoints            int
	waitForFlushFiles    bool
	waitForSnapshotFiles bool
}

func verifySeriesMapsReturnError(
	t *testing.T,
	ts *testSetup,
	namespace ident.ID,
	seriesMaps map[xtime.UnixNano]generate.SeriesBlock,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	verifySeriesMaps(t, ts, namespace, seriesMaps)
	return nil
}
