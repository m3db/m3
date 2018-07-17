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

func TestFsCommitLogMixedModeReadWriteProp(t *testing.T) {
	testMixedModeReadWriteProp(t, false)
}

func testMixedModeReadWriteProp(t *testing.T, snapshotEnabled bool) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	var (
		parameters = gopter.DefaultTestParameters()
		// seed       = time.Now().UnixNano()
		seed      = int64(1)
		props     = gopter.NewProperties(parameters)
		reporter  = gopter.NewFormatedReporter(true, 160, os.Stdout)
		fakeStart = time.Date(2017, time.February, 13, 15, 30, 10, 0, time.Local)
		rng       = rand.New(rand.NewSource(seed))
	)

	parameters.MinSuccessfulTests = 30
	parameters.Rng.Seed(seed)

	props.Property("Blah", prop.ForAll(
		func(input propTestInput) (bool, error) {
			// Test setup
			var (
				ns1BlockSize = input.blockSize.Round(time.Second)
				// setting time to 2017/02/13 15:30:10
				// blkStart15 = fakeStart.Truncate(ns1BlockSize)
				// blkStart16 = blkStart15.Add(ns1BlockSize)
				// blkStart17 = blkStart16.Add(ns1BlockSize)
				// blkStart18 = blkStart17.Add(ns1BlockSize)
				// timesToCheck       = []time.Time{blkStart15, blkStart16, blkStart17, blkStart18}
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

			if err := ns1ROpts.Validate(); err != nil {
				return false, err
			}
			fmt.Printf("blockSize: %s\n", ns1ROpts.BlockSize().String())
			fmt.Printf("bufferPast: %s\n", ns1ROpts.BufferPast().String())
			fmt.Printf("bufferFuture: %s\n", ns1ROpts.BufferFuture().String())

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
				datapoints      = generateDatapoints(fakeStart, numMinutes, ids)
				lastIdx         = 0
				earliestToCheck = datapoints[0].time.Truncate(ns1BlockSize)
				latestToCheck   = datapoints[len(datapoints)-1].time.Add(ns1BlockSize)
				timesToCheck    = []time.Time{}
				start           = earliestToCheck
			)

			for {
				if start.After(latestToCheck) || start.Equal(latestToCheck) {
					break
				}

				timesToCheck = append(timesToCheck, start)
				start = start.Add(time.Duration(rng.Intn(int(maxBlockSize))))
			}
			timesToCheck = append(timesToCheck, latestToCheck)

			fmt.Println("timesToCheck: ", timesToCheck)
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
				fmt.Println("len(datapoints): ", len(datapoints))
				fmt.Println("len(datapoints[:lastIDx]): ", len(datapoints[:lastIdx]))
				fmt.Println("time: ", timesToCheck)
				fmt.Println("lastIDx: ", lastIdx)
				for block, vals := range expectedSeriesMap {
					fmt.Println("expected for block: ", block.ToTime().String())
					for _, series := range vals {
						fmt.Println("expected for series: ", series.ID)
						for _, dp := range series.Data {
							fmt.Println(dp)
						}
					}
				}
				err := verifySeriesMapsReturnError(t, setup, nsID, expectedSeriesMap)
				if err != nil {
					return false, err
				}
				log.Infof("verified data in database equals expected data")
				require.NoError(t, setup.stopServer())
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
	numPoints    int
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
