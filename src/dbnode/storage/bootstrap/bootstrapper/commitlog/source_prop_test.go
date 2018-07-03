// +build big
//
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

	"github.com/m3db/m3db/src/dbnode/digest"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3db/src/dbnode/persist"
	"github.com/m3db/m3db/src/dbnode/persist/fs"
	"github.com/m3db/m3db/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
)

const maxShards = 8192
const blockSize = 2 * time.Hour

func TestCommitLogSourcePropCorrectlyBootstrapsFromCommitlog(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
		startTime  = time.Now().Truncate(blockSize)

		nsOpts = namespace.NewOptions().SetIndexOptions(
			namespace.NewOptions().IndexOptions().SetEnabled(true),
		)
	)
	parameters.MinSuccessfulTests = 80
	parameters.Rng.Seed(seed)
	nsMeta, err := namespace.NewMetadata(testNamespaceID, nsOpts)
	require.NoError(t, err)

	props.Property("Commitlog bootstrapping properly bootstraps the entire commitlog", prop.ForAll(
		func(input propTestInput) (bool, error) {
			var (
				retentionOpts = nsOpts.RetentionOptions().
						SetBufferPast(input.bufferPast).
						SetBufferFuture(input.bufferFuture)
				nsOpts = nsOpts.SetRetentionOptions(retentionOpts)
			)
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

			var (
				// This is the earliest system time that we would be willing to write
				// a datapoint for, so start with that and let the write themselves
				// continue to increment the current time.
				currentTime = startTime.Add(-input.bufferFuture)
				lock        = sync.RWMutex{}
				writesCh    = make(chan struct{}, 5)

				nowFn = func() time.Time {
					lock.RLock()
					curr := currentTime
					lock.RUnlock()
					return curr
				}
			)

			go func() {
				for range writesCh {
					lock.Lock()
					currentTime = currentTime.Add(time.Millisecond)
					lock.Unlock()
				}
			}()

			commitLogBlockSize := 1 * time.Minute
			require.True(t, commitLogBlockSize < blockSize)

			var (
				fsOpts = fs.NewOptions().
					SetFilePathPrefix(dir)
				commitLogOpts = commitlog.NewOptions().
						SetBlockSize(blockSize).
						SetFilesystemOptions(fsOpts).
						SetBlockSize(commitLogBlockSize).
						SetStrategy(commitlog.StrategyWriteBehind).
						SetFlushInterval(time.Millisecond).
						SetClockOptions(commitlog.NewOptions().ClockOptions().SetNowFn(nowFn))
				bootstrapOpts = testOptions().SetCommitLogOptions(commitLogOpts)

				start = input.currentTime.Truncate(blockSize)
			)

			writer, err := fs.NewWriter(fsOpts)
			if err != nil {
				return false, err
			}

			// Determine which shards we need to bootstrap (based on the randomly
			// generated data) (TODO move?)
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

			if input.snapshotExists {
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
						// Only include datapoints that are before or during the snapshot time to ensure that we
						// properly bootstrap from both snapshot files and commit logs and merge them together.
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
					compressedWritesByShards[shard] = encodersBySeries
				}

				for shard, seriesForShard := range compressedWritesByShards {
					err = writer.Open(fs.DataWriterOpenOptions{
						Identifier: fs.FileSetFileIdentifier{
							Namespace:   nsMeta.ID(),
							BlockStart:  start,
							Shard:       shard,
							VolumeIndex: 0,
						},
						BlockSize:   blockSize,
						FileSetType: persist.FileSetSnapshotType,
						Snapshot: fs.DataWriterSnapshotOptions{
							SnapshotTime: input.snapshotTime,
						},
					})

					if err != nil {
						return false, err
					}

					for seriesID, data := range seriesForShard {
						checkedBytes := checked.NewBytes(data, nil)
						checkedBytes.IncRef()
						tags := orderedWritesBySeries[seriesID][0].series.Tags
						writer.Write(ident.StringID(seriesID), tags, checkedBytes, digest.Checksum(data))
					}

					err = writer.Close()
					if err != nil {
						return false, err
					}
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

			// Write all the datapoints to the commitlog
			for _, write := range input.writes {
				// Only write datapoints that are not in the snapshots.
				if input.snapshotExists && !write.arrivedAt.After(input.snapshotTime) {
					continue
				}

				lock.Lock()
				currentTime = write.arrivedAt
				lock.Unlock()

				err := log.Write(context.NewContext(), write.series, write.datapoint, write.unit, write.annotation)
				if err != nil {
					return false, err
				}
				writesCh <- struct{}{}
			}
			close(writesCh)

			err = log.Close()
			if err != nil {
				return false, err
			}

			// Instantiate a commitlog source
			inspection, err := fs.InspectFilesystem(fsOpts)
			if err != nil {
				return false, err
			}
			provider, err := NewCommitLogBootstrapperProvider(bootstrapOpts, inspection, nil)
			if err != nil {
				return false, err
			}
			source, err := provider.Provide()
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
			// runOpts := testDefaultRunOpts.SetCacheSeriesMetadata(input.shouldCacheSeriesMetadata)
			runOpts := testDefaultRunOpts
			dataResult, err := source.BootstrapData(nsMeta, shardTimeRanges, runOpts)
			if err != nil {
				return false, err
			}

			// Create testValues for each datapoint for comparison
			values := []testValue{}
			for _, write := range input.writes {
				values = append(values, testValue{write.series, write.datapoint.Timestamp, write.datapoint.Value, write.unit, write.annotation})
			}

			err = verifyShardResultsAreCorrect(values, blockSize, dataResult.ShardResults(), bootstrapOpts)
			if err != nil {
				return false, err
			}

			// indexResult, err := source.BootstrapIndex(nsMeta, shardTimeRanges, testDefaultRunOpts)
			// if err != nil {
			// 	return false, err
			// }

			// indexBlockSize := nsMeta.Options().IndexOptions().BlockSize()
			// err = verifyIndexResultsAreCorrect(
			// 	values, map[string]struct{}{}, indexResult.IndexResults(), indexBlockSize)
			// if err != nil {
			// 	return false, err
			// }

			return true, nil
		},
		genPropTestInputs(nsMeta, startTime),
	))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d and startTime: %d", seed, startTime.UnixNano())
	}
}

type propTestInput struct {
	currentTime               time.Time
	snapshotTime              time.Time
	snapshotExists            bool
	bufferPast                time.Duration
	bufferFuture              time.Duration
	writes                    []generatedWrite
	shouldCacheSeriesMetadata bool
}

type generatedWrite struct {
	// arrivedAt is used to simulate out-of-order writes which arrive somewhere
	// between time.Now().Add(-bufferFuture) and time.Now().Add(bufferPast).
	arrivedAt  time.Time
	series     commitlog.Series
	datapoint  ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
}

func (w generatedWrite) String() string {
	return fmt.Sprintf("ID = %v, Datapoint = %+v", w.series.ID.String(), w.datapoint)
}

func genPropTestInputs(nsMeta namespace.Metadata, blockStart time.Time) gopter.Gen {
	curriedGenPropTestInput := func(input interface{}) gopter.Gen {
		var (
			inputs                    = input.([]interface{})
			snapshotTime              = inputs[0].(time.Time)
			snapshotExists            = inputs[1].(bool)
			bufferPast                = time.Duration(inputs[2].(int64))
			bufferFuture              = time.Duration(inputs[3].(int64))
			numDatapoints             = inputs[4].(int)
			shouldCacheSeriesMetadata = inputs[5].(bool)
		)

		return genPropTestInput(
			blockStart, bufferPast, bufferFuture, snapshotTime, snapshotExists, numDatapoints, shouldCacheSeriesMetadata, nsMeta.ID().String())
	}

	return gopter.CombineGens(
		// Run iterations of the test with the snapshot time set at any point
		// between the beginning and end of the block.
		gen.TimeRange(blockStart, blockSize),
		// SnapshotExists
		gen.Bool(),
		// Run iterations with any bufferPast/bufferFuture between zero and
		// the namespace blockSize (distinct from the commitLog blockSize).
		gen.Int64Range(0, int64(blockSize)),
		gen.Int64Range(0, int64(blockSize)),
		// Run iterations of the test with between 0 and 100 datapoints
		gen.IntRange(0, 100),
		// ShouldCacheSeriesMetadata
		gen.Bool(),
	).FlatMap(curriedGenPropTestInput, reflect.TypeOf(propTestInput{}))
}

func genPropTestInput(
	start time.Time,
	bufferPast,
	bufferFuture time.Duration,
	snapshotTime time.Time,
	snapshotExists bool,
	numDatapoints int,
	shouldCacheSeriesMetadata bool,
	ns string,
) gopter.Gen {
	return gen.SliceOfN(numDatapoints, genWrite(start, bufferPast, bufferFuture, ns)).
		Map(func(val interface{}) propTestInput {
			return propTestInput{
				currentTime:    start,
				bufferFuture:   bufferFuture,
				bufferPast:     bufferPast,
				snapshotTime:   snapshotTime,
				snapshotExists: snapshotExists,
				writes:         val.([]generatedWrite),
				shouldCacheSeriesMetadata: shouldCacheSeriesMetadata,
			}
		})
}

func genWrite(start time.Time, bufferPast, bufferFuture time.Duration, ns string) gopter.Gen {
	latestDatapointTime := time.Duration(start.Truncate(blockSize).Add(blockSize).UnixNano() - start.UnixNano())

	return gopter.CombineGens(
		// Identifier
		gen.Identifier(),
		// Only generate writes within the current block period
		// TODO: Simplify this
		gen.TimeRange(start, latestDatapointTime),
		// TODO: Explain with comment
		gen.Bool(),
		// Tag key/val
		gen.Identifier(),
		gen.Identifier(),
		// Boolean indicating whether or not to include tags for this series. We want to
		// sometimes not include tags to ensure that the commitlog writer/readers can
		// handle both series that have tags and those that don't.
		gen.Bool(),
		// M3TSZ is lossy, so we want to avoid very large numbers with high amounts of precision
		gen.Float64Range(-9999999, 99999999),
	).Map(func(val []interface{}) generatedWrite {
		var (
			id                 = val[0].(string)
			t                  = val[1].(time.Time)
			a                  = t
			bufferPastOrFuture = val[2].(bool)
			tagKey             = val[3].(string)
			tagVal             = val[4].(string)
			includeTags        = val[5].(bool)
			v                  = val[6].(float64)
		)

		if bufferPastOrFuture {
			a = a.Add(-bufferFuture)
		} else {
			a = a.Add(bufferPast)
		}

		return generatedWrite{
			arrivedAt: a,
			series: commitlog.Series{
				ID:          ident.StringID(id),
				Tags:        seriesUniqueTags(id, tagKey, tagVal, includeTags),
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

type globalSeriesRegistry struct {
	sync.Mutex

	idx      uint64
	idToIdx  map[string]uint64
	idToTags map[string]ident.Tags
}

var seriesRegistry = globalSeriesRegistry{
	idToIdx:  make(map[string]uint64),
	idToTags: make(map[string]ident.Tags),
}

// seriesUniqueIndex ensures that each string series ID maps to exactly one UniqueIndex
func seriesUniqueIndex(series string) uint64 {
	seriesRegistry.Lock()
	defer seriesRegistry.Unlock()

	idx, ok := seriesRegistry.idToIdx[series]
	if ok {
		return idx
	}

	idx = seriesRegistry.idx
	seriesRegistry.idx++
	seriesRegistry.idToIdx[series] = idx
	return idx
}

// seriesUniqueTag ensures that each string series ID ALWAYS maps to the same set of tags
func seriesUniqueTags(seriesID, proposedTagKey, proposedTagVal string, includeTags bool) ident.Tags {
	seriesRegistry.Lock()
	defer seriesRegistry.Unlock()

	tags, ok := seriesRegistry.idToTags[seriesID]
	if ok {
		return tags
	}

	if includeTags {
		tags = ident.NewTags(ident.StringTag(proposedTagKey, proposedTagVal))
	}
	seriesRegistry.idToTags[seriesID] = tags
	return tags
}

// hashIDToShard generates a HashFn based on murmur32
func hashIDToShard(id ident.ID) uint32 {
	return murmur3.Sum32(id.Bytes()) % uint32(maxShards)
}
