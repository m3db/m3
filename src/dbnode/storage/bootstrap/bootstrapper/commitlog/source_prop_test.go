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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/topology"
	tu "github.com/m3db/m3/src/dbnode/topology/testutil"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	murmur3 "github.com/m3db/stackmurmur3/v2"
	"github.com/stretchr/testify/require"
)

const maxShards = 1024
const blockSize = 2 * time.Hour

var (
	testFsOpts        = fs.NewOptions()
	testCommitlogOpts = commitlog.NewOptions()
)

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
	parameters.MinSuccessfulTests = 40
	parameters.Rng.Seed(seed)
	nsMeta, err := namespace.NewMetadata(testNamespaceID, nsOpts)
	require.NoError(t, err)

	props.Property("Commitlog bootstrapping properly bootstraps the entire commitlog", prop.ForAll(
		func(input propTestInput) (bool, error) {
			if !input.commitLogExists {
				// If there is no commit log then we need to make sure
				// snapshot exists, regardless of what the prop test generated.
				input.snapshotExists = true
			}

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

				nowFn = func() time.Time {
					lock.RLock()
					curr := currentTime
					lock.RUnlock()
					return curr
				}
			)

			var (
				fsOpts = testFsOpts.
					SetFilePathPrefix(dir)
				commitLogOpts = testCommitlogOpts.
						SetBlockSize(blockSize).
						SetFilesystemOptions(fsOpts).
						SetStrategy(commitlog.StrategyWriteBehind).
						SetFlushInterval(time.Millisecond).
						SetClockOptions(testCommitlogOpts.ClockOptions().SetNowFn(nowFn))
				bootstrapOpts = testDefaultOpts.SetCommitLogOptions(commitLogOpts).
						SetReturnUnfulfilledForCorruptCommitLogFiles(true)

				start        = xtime.ToUnixNano(input.currentTime.Truncate(blockSize))
				snapshotTime = xtime.ToUnixNano(input.snapshotTime)
			)

			writer, err := fs.NewWriter(fsOpts)
			if err != nil {
				return false, err
			}

			orderedWritesBySeries := map[string][]generatedWrite{}
			for _, write := range input.writes {
				id := write.series.ID
				writesForSeries, ok := orderedWritesBySeries[id.String()]
				if !ok {
					writesForSeries = []generatedWrite{}
				}
				writesForSeries = append(writesForSeries, write)
				orderedWritesBySeries[id.String()] = writesForSeries
			}

			for _, writesForSeries := range orderedWritesBySeries {
				sort.Slice(writesForSeries, func(i, j int) bool {
					return writesForSeries[i].datapoint.TimestampNanos.
						Before(writesForSeries[j].datapoint.TimestampNanos)
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

					encoder := m3tsz.NewEncoder(writesForSeries[0].datapoint.TimestampNanos,
						nil, true, encoding.NewOptions())
					for _, value := range writesForSeries {
						// Only include datapoints that are before or during the snapshot time to ensure that we
						// properly bootstrap from both snapshot files and commit logs and merge them together.
						// Note that if the commit log does not exist we ignore the snapshot time because we need
						// the snapshot to include all the data.
						if !input.commitLogExists ||
							value.arrivedAt.Before(input.snapshotTime) ||
							value.arrivedAt.Equal(input.snapshotTime) {
							err := encoder.Encode(value.datapoint, value.unit, value.annotation)
							if err != nil {
								return false, err
							}
						}
					}

					ctx := context.NewBackground()
					reader, ok := encoder.Stream(ctx)
					if ok {
						bytes, err := xio.ToBytes(reader)
						if !errors.Is(err, io.EOF) {
							return false, err
						}
						encodersBySeries[seriesID] = bytes
					}
					ctx.Close()

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
							SnapshotTime: snapshotTime,
						},
					})

					if err != nil {
						return false, err
					}

					for seriesID, data := range seriesForShard {
						checkedBytes := checked.NewBytes(data, nil)
						checkedBytes.IncRef()
						tags := orderedWritesBySeries[seriesID][0].tags
						metadata := persist.NewMetadataFromIDAndTags(ident.StringID(seriesID), tags,
							persist.MetadataOptions{})
						writer.Write(metadata, checkedBytes, digest.Checksum(data))
					}

					err = writer.Close()
					if err != nil {
						return false, err
					}
				}
			}

			if input.commitLogExists {
				writesCh := make(chan struct{}, 5)
				go func() {
					for range writesCh {
						lock.Lock()
						currentTime = currentTime.Add(time.Millisecond)
						lock.Unlock()
					}
				}()

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
					if input.snapshotExists &&
						!write.arrivedAt.After(input.snapshotTime) {
						continue
					}

					lock.Lock()
					currentTime = write.arrivedAt
					lock.Unlock()

					err := log.Write(context.NewBackground(), write.series, write.datapoint, write.unit, write.annotation)
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

				if input.includeCorruptedCommitlogFile {
					// Write out an additional commit log file with a corrupt info header to
					// make sure that the commitlog source skips it in the single node scenario.
					commitLogFiles, corruptFiles, err := commitlog.Files(commitLogOpts)
					if err != nil {
						return false, err
					}
					if len(corruptFiles) > 0 {
						return false, fmt.Errorf("found corrupt commit log files: %v", corruptFiles)
					}

					if len(commitLogFiles) > 0 {
						nextCommitLogFile, _, err := commitlog.NextFile(commitLogOpts)
						if err != nil {
							return false, err
						}

						err = ioutil.WriteFile(nextCommitLogFile, []byte("corruption"), 0644)
						if err != nil {
							return false, err
						}
					}
				}
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
			end := xtime.ToUnixNano(input.currentTime.Add(blockSize))
			ranges := xtime.NewRanges(xtime.Range{Start: start, End: end})

			// Determine which shards we need to bootstrap (based on the randomly
			// generated data)
			var (
				allShardsMap   = map[uint32]bool{}
				allShardsSlice = []uint32{}
			)
			for _, write := range input.writes {
				shard := write.series.Shard
				if _, ok := allShardsMap[shard]; !ok {
					allShardsSlice = append(allShardsSlice, shard)
				}
				allShardsMap[shard] = true
			}

			// Assign the previously-determined bootstrap range to each known shard
			shardTimeRanges := result.NewShardTimeRanges()
			for shard := range allShardsMap {
				shardTimeRanges.Set(shard, ranges)
			}

			// Perform the bootstrap
			var initialTopoState *topology.StateSnapshot
			if input.multiNodeCluster {
				initialTopoState = tu.NewStateSnapshot(2, tu.HostShardStates{
					tu.SelfID:   tu.Shards(allShardsSlice, shard.Available),
					"not-self1": tu.Shards(allShardsSlice, shard.Available),
					"not-self2": tu.Shards(allShardsSlice, shard.Available),
				})
			} else {
				initialTopoState = tu.NewStateSnapshot(1, tu.HostShardStates{
					tu.SelfID: tu.Shards(allShardsSlice, shard.Available),
				})
			}

			runOpts := testDefaultRunOpts.SetInitialTopologyState(initialTopoState)
			tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, runOpts, shardTimeRanges, fsOpts, nsMeta)

			ctx := context.NewBackground()
			defer ctx.Close()

			bootstrapResults, err := source.Bootstrap(ctx, tester.Namespaces, tester.Cache)
			if err != nil {
				return false, err
			}

			// Create testValues for each datapoint for comparison
			values := testValues{}
			for _, write := range input.writes {
				values = append(values, testValue{
					write.series, write.datapoint.TimestampNanos,
					write.datapoint.Value, write.unit, write.annotation})
			}

			commitLogFiles, corruptFiles, err := commitlog.Files(commitLogOpts)
			if err != nil {
				return false, err
			}
			if len(corruptFiles) > 0 && !input.includeCorruptedCommitlogFile {
				return false, fmt.Errorf("found corrupt commit log files: %v", corruptFiles)
			}

			var (
				commitLogFilesExist = len(commitLogFiles) > 0
				// In the multi-node setup we want to return unfulfilled if there are any corrupt files, but
				// we always want to return fulfilled in the single node setup. In addition, the source will not
				// return unfulfilled in the presence of corrupt files if the range we request it to bootstrap
				// is empty so we need to handle that case too.
				shouldReturnUnfulfilled = input.multiNodeCluster &&
					input.includeCorruptedCommitlogFile &&
					commitLogFilesExist &&
					!shardTimeRanges.IsEmpty()
			)

			nsResult, found := bootstrapResults.Results.Get(nsMeta.ID())
			if !found {
				return false, fmt.Errorf("could not find id: %s", nsMeta.ID().String())
			}

			dataResult := nsResult.DataResult
			if shouldReturnUnfulfilled {
				if dataResult.Unfulfilled().IsEmpty() {
					return false, fmt.Errorf(
						"data result unfulfilled should not be empty in multi node cluster but was")
				}
			} else {
				if !dataResult.Unfulfilled().IsEmpty() {
					return false, fmt.Errorf(
						"data result unfulfilled in single node cluster should be empty but was: %s",
						dataResult.Unfulfilled().String())
				}
			}

			written, err := tester.EnsureDumpAllForNamespace(nsMeta)
			if err != nil {
				return false, err
			}

			indexResult := nsResult.IndexResult
			if shouldReturnUnfulfilled {
				if indexResult.Unfulfilled().IsEmpty() {
					return false, fmt.Errorf(
						"index result unfulfilled should not be empty in multi node cluster but was")
				}
			} else {
				if !indexResult.Unfulfilled().IsEmpty() {
					return false, fmt.Errorf(
						"index result unfulfilled in single node cluster should be empty but was: %s",
						indexResult.Unfulfilled().String())
				}
			}

			err = verifyValuesAreCorrect(values, written)
			if err != nil {
				return false, err
			}

			return true, nil
		},
		genPropTestInputs(t, nsMeta, startTime),
	))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d and startTime: %d", seed, startTime.UnixNano())
	}
}

type propTestInput struct {
	currentTime                   time.Time
	snapshotTime                  time.Time
	snapshotExists                bool
	commitLogExists               bool
	bufferPast                    time.Duration
	bufferFuture                  time.Duration
	writes                        []generatedWrite
	includeCorruptedCommitlogFile bool
	multiNodeCluster              bool
}

type generatedWrite struct {
	// arrivedAt is used to simulate out-of-order writes which arrive somewhere
	// between time.Now().Add(-bufferFuture) and time.Now().Add(bufferPast).
	arrivedAt  time.Time
	series     ts.Series
	tags       ident.Tags
	datapoint  ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
}

func (w generatedWrite) String() string {
	return fmt.Sprintf("ID = %v, Datapoint = %+v", w.series.ID.String(), w.datapoint)
}

func genPropTestInputs(t *testing.T, nsMeta namespace.Metadata, blockStart time.Time) gopter.Gen {
	curriedGenPropTestInput := func(input interface{}) gopter.Gen {
		var (
			inputs                        = input.([]interface{})
			snapshotTime                  = inputs[0].(time.Time)
			snapshotExists                = inputs[1].(bool)
			commitLogExists               = inputs[2].(bool)
			bufferPast                    = time.Duration(inputs[3].(int64))
			bufferFuture                  = time.Duration(inputs[4].(int64))
			numDatapoints                 = inputs[5].(int)
			includeCorruptedCommitlogFile = inputs[6].(bool)
			multiNodeCluster              = inputs[7].(bool)
		)

		return genPropTestInput(
			t,
			blockStart, bufferPast, bufferFuture,
			snapshotTime, snapshotExists, commitLogExists,
			numDatapoints, nsMeta.ID().String(), includeCorruptedCommitlogFile, multiNodeCluster)
	}

	return gopter.CombineGens(
		// Run iterations of the test with the snapshot time set at any point
		// between the beginning and end of the block.
		gen.TimeRange(blockStart, blockSize),
		// SnapshotExists
		gen.Bool(),
		// CommitLogExists
		gen.Bool(),
		// Run iterations with any bufferPast/bufferFuture between zero and
		// the namespace blockSize (distinct from the commitLog blockSize).
		gen.Int64Range(0, int64(blockSize)),
		gen.Int64Range(0, int64(blockSize)),
		// Run iterations of the test with between 0 and 100 datapoints
		gen.IntRange(0, 100),
		// Whether the test should generate an additional corrupt commitlog file
		// to ensure the commit log bootstrapper skips it correctly.
		gen.Bool(),
		// Whether the test should simulate the InitialTopologyState to mimic a
		// multi node cluster or not.
		gen.Bool(),
	).FlatMap(curriedGenPropTestInput, reflect.TypeOf(propTestInput{}))
}

func genPropTestInput(
	t *testing.T,
	start time.Time,
	bufferPast,
	bufferFuture time.Duration,
	snapshotTime time.Time,
	snapshotExists bool,
	commitLogExists bool,
	numDatapoints int,
	ns string,
	includeCorruptedCommitlogFile bool,
	multiNodeCluster bool,
) gopter.Gen {
	return gen.SliceOfN(numDatapoints, genWrite(t, start, bufferPast, bufferFuture, ns)).
		Map(func(val []generatedWrite) propTestInput {
			return propTestInput{
				currentTime:                   start,
				bufferFuture:                  bufferFuture,
				bufferPast:                    bufferPast,
				snapshotTime:                  snapshotTime,
				snapshotExists:                snapshotExists,
				commitLogExists:               commitLogExists,
				writes:                        val,
				includeCorruptedCommitlogFile: includeCorruptedCommitlogFile,
				multiNodeCluster:              multiNodeCluster,
			}
		})
}

func genWrite(t *testing.T, start time.Time, bufferPast, bufferFuture time.Duration, ns string) gopter.Gen {
	latestDatapointTime := start.Truncate(blockSize).Add(blockSize).Sub(start)

	return gopter.CombineGens(
		// Identifier
		gen.Identifier(),
		// Only generate writes within the current block period
		gen.TimeRange(start, latestDatapointTime),
		// Boolean indicating whether we should move offset the datapoint by
		// the maximum of eithe bufferPast or bufferFuture.
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
			tm                 = val[1].(time.Time)
			a                  = tm
			bufferPastOrFuture = val[2].(bool)
			tagKey             = val[3].(string)
			tagVal             = val[4].(string)
			includeTags        = val[5].(bool)
			v                  = val[6].(float64)

			tagEncoderPool = testCommitlogOpts.FilesystemOptions().TagEncoderPool()
			tagSliceIter   = ident.NewTagsIterator(ident.Tags{})
		)

		if bufferPastOrFuture {
			a = a.Add(-bufferFuture)
		} else {
			a = a.Add(bufferPast)
		}

		tags := seriesUniqueTags(id, tagKey, tagVal, includeTags)
		tagSliceIter.Reset(tags)

		tagEncoder := tagEncoderPool.Get()
		err := tagEncoder.Encode(tagSliceIter)
		require.NoError(t, err)

		encodedTagsChecked, ok := tagEncoder.Data()
		require.True(t, ok)

		return generatedWrite{
			arrivedAt: a,
			series: ts.Series{
				ID:          ident.StringID(id),
				Namespace:   ident.StringID(ns),
				Shard:       hashIDToShard(ident.StringID(id)),
				UniqueIndex: seriesUniqueIndex(id),
				EncodedTags: ts.EncodedTags(encodedTagsChecked.Bytes()),
			},
			tags: tags,
			datapoint: ts.Datapoint{
				TimestampNanos: xtime.ToUnixNano(tm),
				Value:          v,
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
