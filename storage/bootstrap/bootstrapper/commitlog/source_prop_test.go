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

package commitlog

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3tsz"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/spaolacci/murmur3"
)

// Don't set this too high otherwise the test will be slow as it tries to create
// thousands and thousands of files
const maxShards = 50
const blockSize = 2 * time.Hour

func TestCommitLogSourcePropCorrectlyBootstrapsFromCommitlog(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.Rng.Seed(seed)
	parameters.MinSuccessfulTests = 40
	props := gopter.NewProperties(parameters)
	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)

	startTime := time.Now().Truncate(blockSize)

	nsOpts := namespace.NewOptions()
	nsMeta, err := namespace.NewMetadata(testNamespaceID, nsOpts)
	require.NoError(t, err)
	inputsGen := genPropTestInputs(nsMeta, startTime)

	props.Property("Commitlog bootstrapping properly bootstraps the entire commitlog", prop.ForAll(
		func(input propTestInput) (bool, error) {
			retentionOpts := nsOpts.RetentionOptions().
				SetBufferPast(input.bufferPast).
				SetBufferFuture(input.bufferFuture)
			nsOpts = nsOpts.SetRetentionOptions(retentionOpts)
			nsMeta, err := namespace.NewMetadata(testNamespaceID, nsOpts)
			if err != nil {
				return false, err
			}

			// Make sure we have a new directory for each test run
			dir, err := ioutil.TempDir("", "foo")
			if err != nil {
				return false, err
			}
			defer func() {
				os.RemoveAll(dir)
			}()

			currentTime := startTime
			lock := sync.RWMutex{}
			nowFn := func() time.Time {
				lock.RLock()
				curr := currentTime
				lock.RUnlock()
				return curr
			}

			writesCh := make(chan struct{}, 5)
			go func() {
				for range writesCh {
					lock.Lock()
					// TODO: Do I still need this?
					// if !currentTime.Add(time.Millisecond).Before(startTime.Truncate(blockSize).Add(blockSize)) {
					// 	// Make sure we never advance the current time outside of the current block that we're writing
					// 	panic("messed up")
					// }
					currentTime = currentTime.Add(time.Millisecond)
					lock.Unlock()
				}
			}()
			// Configure the commitlog and FS options
			blockSize := 2 * time.Hour
			fsOpts := fs.NewOptions().
				SetFilePathPrefix(dir)
			commitLogBlockSize := 1 * time.Minute
			require.True(t, commitLogBlockSize < blockSize)
			commitLogOpts := commitlog.NewOptions().
				SetBlockSize(blockSize).
				SetFilesystemOptions(fsOpts).
				SetBlockSize(commitLogBlockSize).
				SetStrategy(commitlog.StrategyWriteWait).
				SetFlushInterval(time.Millisecond).
				SetClockOptions(commitlog.NewOptions().ClockOptions().SetNowFn(nowFn))
			bootstrapOpts := testOptions().SetCommitLogOptions(commitLogOpts)

			// Instantiate the FS writer
			start := input.currentTime.Truncate(blockSize) // Move to top?
			writer, err := fs.NewWriter(fsOpts)
			if err != nil {
				return false, err
			}

			// Determine which shards we need to bootstrap (based on the randomly
			// generated data) (TODO move?s)
			allShards := map[uint32]bool{}
			for _, write := range input.writes {
				allShards[write.series.Shard] = true
			}

			orderedWritesBySeries := map[string][]generatedWrite{}
			for _, write := range input.writes {
				id := write.series.ID
				writesForSeries, ok := orderedWritesBySeries[id.String()]
				if !ok {
					writesForSeries = []generatedWrite{}
				}
				writesForSeries = append(writesForSeries, write)
				orderedWritesBySeries[id.String()] = writesForSeries // TODO: Need this?
			}

			for _, writesForSeries := range orderedWritesBySeries {
				sort.Slice(writesForSeries, func(i, j int) bool {
					return writesForSeries[i].datapoint.Timestamp.Before(writesForSeries[j].datapoint.Timestamp)
				})
			}

			compressedWritesByShards := map[uint32]map[string][]byte{}
			for seriesID, writesForSeries := range orderedWritesBySeries {
				shard := hashIDToShard(ident.StringID(seriesID))
				encodersBySeries, ok := compressedWritesByShards[shard]
				if !ok {
					encodersBySeries = map[string][]byte{}
					compressedWritesByShards[shard] = encodersBySeries
				}

				encoder := m3tsz.NewEncoder(writesForSeries[0].datapoint.Timestamp, nil, true, encoding.NewOptions())
				for _, value := range writesForSeries {
					// Only include datapoints that are before or during the snapshot time to
					// ensure that we properly bootstrap from both snapshot files and commit logs
					// and merge them together
					if value.arrivedAt.Before(input.snapshotTime) ||
						value.arrivedAt.Equal(input.snapshotTime) {
						err := encoder.Encode(value.datapoint, value.unit, value.annotation)
						if err != nil {
							return false, err
						}
					}
				}
				reader := encoder.Stream()
				if reader != nil {
					seg, err := reader.Segment()
					if err != nil {
						return false, err
					}
					bytes := make([]byte, seg.Len())
					_, err = reader.Read(bytes)
					if err != nil {
						return false, err
					}
					encodersBySeries[seriesID] = bytes

				}
				compressedWritesByShards[shard] = encodersBySeries // TODO: DO I need this?
			}

			for shard, seriesForShard := range compressedWritesByShards {
				err = writer.Open(fs.WriterOpenOptions{
					Identifier: fs.FilesetFileIdentifier{
						Namespace:  nsMeta.ID(),
						BlockStart: start,
						Shard:      shard,
						Index:      0, // TODO: Vary this
					},
					BlockSize:   blockSize,
					FilesetType: persist.FilesetSnapshotType,
					Snapshot: fs.WriterSnapshotOptions{
						SnapshotTime: input.snapshotTime,
					},
				})

				if err != nil {
					return false, err
				}

				for seriesID, data := range seriesForShard {
					checkedBytes := checked.NewBytes(data, nil)
					checkedBytes.IncRef()
					writer.Write(ident.StringID(seriesID), checkedBytes, uint32(0)) // TODO: Calculate correct checksum
				}

				err = writer.Close()
				if err != nil {
					return false, err
				}
			}

			// Instantiate commitlog
			log, err := commitlog.NewCommitLog(commitLogOpts)
			if err != nil {
				return false, err
			}
			err = log.Open()
			if err != nil {
				return false, err
			}

			sort.Slice(input.writes, func(i, j int) bool {
				return input.writes[i].arrivedAt.Before(input.writes[j].arrivedAt)
			})
			// numWritten := 0
			// Write all the datapoints to the commitlog
			for _, write := range input.writes {
				// Only write datapoint that are not in the snapshots
				if write.arrivedAt.After(input.snapshotTime) {
					now := nowFn()
					if write.arrivedAt.After(now) { // Do I need this?
						lock.Lock()
						currentTime = write.arrivedAt
						lock.Unlock()
					}
					err := log.Write(context.NewContext(), write.series, write.datapoint, write.unit, write.annotation)
					if err != nil {
						return false, err
					}
					writesCh <- struct{}{}
				}
			}
			err = log.Close()
			if err != nil {
				return false, err
			}

			// Instantiate a commitlog source
			source, err := NewCommitLogBootstrapper(bootstrapOpts, nil)
			if err != nil {
				return false, err
			}

			// Determine time range to bootstrap
			end := input.currentTime.Add(blockSize)
			ranges := xtime.Ranges{}
			ranges = ranges.AddRange(xtime.Range{
				Start: start,
				End:   end,
			})

			// Assign the previously-determined bootstrap range to each known shard
			shardTimeRanges := result.ShardTimeRanges{}
			for shard := range allShards {
				shardTimeRanges[shard] = ranges
			}

			// Perform the bootstrap
			bootstrapResult, err := source.Bootstrap(nsMeta, shardTimeRanges, testDefaultRunOpts)
			if err != nil {
				return false, err
			}

			// Create testValues for each datapoint for comparison
			values := []testValue{}
			for _, write := range input.writes {
				values = append(values, testValue{write.series, write.datapoint.Timestamp, write.datapoint.Value, write.unit, write.annotation})
			}

			var shardResults result.ShardResults
			if bootstrapResult != nil {
				shardResults = bootstrapResult.ShardResults()
			}
			err = verifyShardResultsAreCorrect(values, blockSize, shardResults, bootstrapOpts)
			if err != nil {
				for seriesID, w := range orderedWritesBySeries {
					fmt.Println(seriesID, " ", "series: ", hashIDToShard(ident.StringID(seriesID)), " ", w[0].series.Shard)
				}
				return false, err
			}
			return true, nil
		},
		inputsGen,
	))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

type propTestInput struct {
	currentTime  time.Time
	snapshotTime time.Time
	bufferPast   time.Duration
	bufferFuture time.Duration
	writes       []generatedWrite
}

type generatedWrite struct {
	// arrivedAt is used to simulate out-of-order writes which arrive somewhere
	// between time.Now().Add(-bufferFuture) and time.Now().Add(bufferPast)
	arrivedAt  time.Time
	series     commitlog.Series
	datapoint  ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
}

type generatedWriteTimes struct {
	arrivedAt time.Time
	timestamp time.Time
}

func (w generatedWrite) String() string {
	return fmt.Sprintf("ID = %v, Datapoint = %+v", w.series.ID.String(), w.datapoint)
}

func genPropTestInputs(ns namespace.Metadata, blockStart time.Time) gopter.Gen {
	curriedGenPropTestInput := func(input interface{}) gopter.Gen {
		inputs := input.([]interface{})
		snapshotTime := inputs[0].(time.Time)
		bufferPast := time.Duration(inputs[1].(int64))
		bufferFuture := time.Duration(inputs[2].(int64))
		numDatapoints := inputs[3].(int)
		return genPropTestInput(
			blockStart, bufferPast, bufferFuture, snapshotTime, numDatapoints, ns.ID().String())
	}
	return gopter.CombineGens(
		// Run iterations of the test with the snapshot time set at any
		// point between the beginning and end of the block.
		gen.TimeRange(blockStart, blockSize-15*time.Minute),
		// Run iterations with any bufferPast/bufferFuture between zero and
		// the namespace blocksize (distinct from the commitlog blockSize)
		gen.Int64Range(0, int64(blockSize)),
		gen.Int64Range(0, int64(blockSize)),
		// Run iterations of the test with between 0 and 1000 datapoints
		gen.IntRange(0, 100),
	).FlatMap(curriedGenPropTestInput, reflect.TypeOf(propTestInput{}))
}

func genPropTestInput(start time.Time, bufferPast, bufferFuture time.Duration, snapshotTime time.Time, numDatapoints int, ns string) gopter.Gen {
	return gen.SliceOfN(numDatapoints, genWrite(start, bufferPast, bufferFuture, ns)).
		Map(func(val interface{}) propTestInput {
			return propTestInput{
				currentTime:  start,
				bufferFuture: bufferFuture,
				bufferPast:   bufferPast,
				snapshotTime: snapshotTime,
				writes:       val.([]generatedWrite),
			}
		})
}

func genWrite(start time.Time, bufferPast, bufferFuture time.Duration, ns string) gopter.Gen {
	latestDatapointTime := time.Duration(start.Truncate(blockSize).Add(blockSize).UnixNano() - start.UnixNano())
	return gopter.CombineGens(
		gen.Identifier(),
		// Only generate writes within the current block period
		// TODO: simplify this
		gen.TimeRange(start, latestDatapointTime),
		gen.Bool(),
		// M3TSZ is lossy, so we want to avoid very large numbers with high amounts of precision
		gen.Float64Range(-9999999, 99999999),
		// Some of the commitlog bootstrapping code is O(N) with respect to the
		// number of shards, so we cap it to prevent timeouts
		gen.UInt32Range(0, maxShards),
	).Map(func(val []interface{}) generatedWrite {
		id := val[0].(string)
		t := val[1].(time.Time)

		var a = t
		if val[2].(bool) {
			a = t.Add(-bufferFuture)
		} else {
			a = t.Add(bufferPast)
		}

		v := val[3].(float64)

		return generatedWrite{
			arrivedAt: a,
			series: commitlog.Series{
				ID:          ident.StringID(id),
				Namespace:   ident.StringID(ns),
				Shard:       hashIDToShard(ident.StringID(id)),
				UniqueIndex: seriesUniqueIndex(id),
			},
			datapoint: ts.Datapoint{
				Timestamp: t,
				Value:     v,
			},
			unit: xtime.Nanosecond,
		}
	})
}

type globalMetricIdx struct {
	sync.Mutex

	idx     uint64
	idToIdx map[string]uint64
}

var metricIdx = globalMetricIdx{
	idToIdx: make(map[string]uint64),
}

// seriesUniqueIndex ensures that each string series ID maps to exactly one UniqueIndex
func seriesUniqueIndex(series string) uint64 {
	metricIdx.Lock()
	defer metricIdx.Unlock()

	idx, ok := metricIdx.idToIdx[series]
	if ok {
		return idx
	}

	idx = metricIdx.idx
	metricIdx.idx++
	metricIdx.idToIdx[series] = idx
	return idx
}

// hashIDToShard generates a HashFn based on murmur32
func hashIDToShard(id ident.ID) uint32 {
	return murmur3.Sum32(id.Data().Get()) % uint32(maxShards)
}
