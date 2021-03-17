// +build integration

// Copyright (c) 2018 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/x/context"
	xtime "github.com/m3db/m3/src/x/time"
	"go.uber.org/zap"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

// Really small block sizes make certain operations take too long
// (I.E) with 50 hours of retention and a 10 second block size the
// node will try to flush 10 * 6 * 60 * 50 files.
const (
	minBlockSize       = 15 * time.Minute
	maxBlockSize       = 12 * time.Hour
	maxPoints          = 100
	minSuccessfulTests = 4
	maxFlushWaitTime   = 30 * time.Second
)

// This integration test uses property testing to make sure that the node
// can properly bootstrap all the data from a combination of fileset files,
// snapshotfiles, and commit log files. It varies the following inputs to
// the system:
// 		1) block size
// 		2) buffer past
// 		3) buffer future
// 		4) number of datapoints
// 		5) whether it waits for data files to be flushed before shutting down
// 		6) whether it waits for snapshot files to be written before shutting down
//
// It works by generating random datapoints, and then writing those data points
// to the node in order. At randomly selected times during the write process, the
// node will turn itself off and then bootstrap itself before resuming.
func TestFsCommitLogMixedModeReadWriteProp(t *testing.T) {
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

	parameters.MinSuccessfulTests = minSuccessfulTests
	parameters.Rng.Seed(seed)

	props.Property(
		"Node can bootstrap all data from filesetfiles, snapshotfiles, and commit log files", prop.ForAll(
			func(input propTestInput) (bool, error) {
				// Test setup
				var (
					// Round to a second to prevent interactions between the RPC client
					// and the node itself when blocksize is not rounded down to a second.
					ns1BlockSize = input.blockSize.Round(time.Second)
					// Make sure randomly generated data never falls out of retention
					// during the course of a test.
					retentionPeriod = maxBlockSize * 5
					bufferPast      = input.bufferPast
					bufferFuture    = input.bufferFuture
					ns1ROpts        = retention.NewOptions().
							SetBlockSize(ns1BlockSize).
							SetRetentionPeriod(retentionPeriod).
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

				ns1Opts := namespace.NewOptions().
					SetRetentionOptions(ns1ROpts).
					SetSnapshotEnabled(true)
				ns1, err := namespace.NewMetadata(nsID, ns1Opts)
				if err != nil {
					return false, err
				}
				opts := NewTestOptions(t).
					SetNamespaces([]namespace.Metadata{ns1})

				// Test setup
				setup := newTestSetupWithCommitLogAndFilesystemBootstrapper(t, opts)
				defer setup.Close()

				log := setup.StorageOpts().InstrumentOptions().Logger()
				log.Sugar().Info("blockSize: %s\n", ns1ROpts.BlockSize().String())
				log.Sugar().Info("bufferPast: %s\n", ns1ROpts.BufferPast().String())
				log.Sugar().Info("bufferFuture: %s\n", ns1ROpts.BufferFuture().String())

				setup.SetNowFn(fakeStart)

				var (
					ids        = &idGen{longTestID}
					datapoints = generateDatapoints(fakeStart, numPoints, ids, nil)
					// Used to keep track of which datapoints have been written already.
					lastDatapointsIdx = 0
					earliestToCheck   = datapoints[0].time.Truncate(ns1BlockSize)
					latestToCheck     = datapoints[len(datapoints)-1].time.Add(ns1BlockSize)
					timesToRestart    = []time.Time{}
					start             = earliestToCheck
					fsOpts            = setup.StorageOpts().CommitLogOptions().FilesystemOptions()
					filePathPrefix    = fsOpts.FilePathPrefix()
				)

				// Generate randomly selected times during which the node will restart
				// and bootstrap before continuing to write data.
				for {
					if start.After(latestToCheck) || start.Equal(latestToCheck) {
						break
					}

					timesToRestart = append(timesToRestart, start)
					start = start.Add(time.Duration(rng.Intn(int(maxBlockSize))))
				}
				timesToRestart = append(timesToRestart, latestToCheck)

				for _, timeToCheck := range timesToRestart {
					startServerWithNewInspection(t, opts, setup)
					ctx := context.NewBackground()
					defer ctx.Close()

					log.Info("writing datapoints")
					var (
						i              int
						snapshotBlocks = map[xtime.UnixNano]struct{}{}
					)
					for i = lastDatapointsIdx; i < len(datapoints); i++ {
						var (
							dp = datapoints[i]
							ts = dp.time
						)
						if !ts.Before(timeToCheck) {
							break
						}

						setup.SetNowFn(ts)

						err := setup.DB().Write(ctx, nsID, dp.series, ts, dp.value, xtime.Second, dp.ann)
						if err != nil {
							log.Warn("error writing series datapoint", zap.Error(err))
							return false, err
						}
						snapshotBlocks[xtime.ToUnixNano(ts.Truncate(ns1BlockSize))] = struct{}{}
					}
					lastDatapointsIdx = i
					log.Info("wrote datapoints")

					expectedSeriesMap := datapoints[:lastDatapointsIdx].toSeriesMap(ns1BlockSize)
					log.Info("verifying data in database equals expected data")
					if !verifySeriesMaps(t, setup, nsID, expectedSeriesMap) {
						// verifySeriesMaps will make sure the actual failure is included
						// in the go test output, but it uses assert() under the hood so
						// there is not a clean way to return the explicit error to gopter
						// as well.
						return false, nil
					}
					log.Info("verified data in database equals expected data")
					if input.waitForFlushFiles {
						log.Info("waiting for data files to be flushed")
						var (
							now                       = setup.NowFn()()
							endOfLatestFlushableBlock = retention.FlushTimeEnd(ns1ROpts, now).
								// Add block size because FlushTimeEnd will return the beginning of the
								// latest flushable block.
								Add(ns1BlockSize)
								// Any data that falls within or before the end the last flushable block should
								// be available on disk.
							expectedFlushedData = datapoints.before(endOfLatestFlushableBlock).toSeriesMap(ns1BlockSize)
							err                 = waitUntilDataFilesFlushed(
								filePathPrefix, setup.ShardSet(), nsID, expectedFlushedData, maxFlushWaitTime)
						)
						if err != nil {
							return false, fmt.Errorf("error waiting for data files to flush: %s", err)
						}
					}

					// We've written data if we've advanced the datapoints index.
					dpsWritten := i > 0
					if input.waitForSnapshotFiles && dpsWritten {
						log.Info("waiting for snapshot files to be written")
						// We only snapshot TSDB blocks that have data in them.
						expectedSnapshotBlocks := make([]snapshotID, 0, len(snapshotBlocks))
						for snapshotBlock := range snapshotBlocks {
							expectedSnapshotBlocks = append(expectedSnapshotBlocks, snapshotID{
								blockStart: snapshotBlock.ToTime(),
							})
						}
						_, err := waitUntilSnapshotFilesFlushed(
							fsOpts,
							setup.ShardSet(),
							nsID,
							expectedSnapshotBlocks,
							maxFlushWaitTime,
						)
						if err != nil {
							return false, fmt.Errorf("error waiting for snapshot files: %s", err.Error())
						}
					}

					require.NoError(t, setup.StopServer())
					// Create a new test setup because databases do not have a completely
					// clean shutdown, so they can end up in a bad state where the persist
					// manager is not idle and thus no more flushes can be done, even if
					// there are no other in-progress flushes.
					oldNow := setup.NowFn()()
					setup = newTestSetupWithCommitLogAndFilesystemBootstrapper(
						// FilePathPrefix is randomly generated if not provided, so we need
						// to make sure all our test setups have the same prefix so that
						// they can find each others files.
						t, opts.SetFilePathPrefix(filePathPrefix))
					// Make sure the new setup has the same system time as the previous one.
					setup.SetNowFn(oldNow)
				}

				if lastDatapointsIdx != len(datapoints) {
					return false, fmt.Errorf(
						"expected lastDatapointsIdx to be: %d but was: %d", len(datapoints), lastDatapointsIdx)
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
		gen.Int64Range(durationToEvenInt64(minBlockSize), durationToEvenInt64(maxBlockSize)),
		gen.Int64Range(durationToEvenInt64(minBlockSize), durationToEvenInt64(maxBlockSize)),
		gen.Int64Range(durationToEvenInt64(minBlockSize), durationToEvenInt64(maxBlockSize)),
		gen.IntRange(1, maxPoints),
		gen.Bool(),
		gen.Bool(),
	).Map(func(inputs []interface{}) propTestInput {
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

func durationToEvenInt64(d time.Duration) int64 {
	return (int64(d) / 2) * 2
}
