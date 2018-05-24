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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/persist/fs"
	"github.com/m3db/m3db/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/spaolacci/murmur3"
)

const maxShards = 8192
const blockSize = 2 * time.Hour

func TestCommitLogSourcePropCorrectlyBootstrapsFromCommitlog(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.Rng.Seed(123456789)
	parameters.MinSuccessfulTests = 40
	props := gopter.NewProperties(parameters)

	props.Property("Commitlog bootstrapping properly bootstraps the entire commitlog", prop.ForAll(
		func(input propTestInput) (bool, error) {
			// Make sure we have a new directory for each test run
			dir, err := ioutil.TempDir("", "foo")
			if err != nil {
				return false, err
			}
			defer func() {
				os.RemoveAll(dir)
			}()

			// Configure the commitlog to use the test directory and set the blocksize
			fsOpts := fs.NewOptions().SetFilePathPrefix(dir)
			commitLogOpts := commitlog.NewOptions().
				SetBlockSize(2 * time.Hour).
				SetFilesystemOptions(fsOpts)
			bootstrapOpts := testOptions().
				SetCommitLogOptions(commitLogOpts)

			// Instantiate commitlog
			log, err := commitlog.NewCommitLog(commitLogOpts)
			if err != nil {
				return false, err
			}
			err = log.Open()
			if err != nil {
				return false, err
			}

			// Write all the datapoints to the commitlog
			for _, write := range input.writes {
				err := log.Write(context.NewContext(), write.series, write.datapoint, write.unit, write.annotation)
				if err != nil {
					return false, err
				}
			}
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
			nsOpts := namespace.NewOptions()
			nsOpts = nsOpts.SetIndexOptions(
				nsOpts.IndexOptions().SetEnabled(true),
			)
			md, err := namespace.NewMetadata(testNamespaceID, nsOpts)
			if err != nil {
				return false, err
			}
			blockSize := md.Options().RetentionOptions().BlockSize()
			start := input.currentTime.Truncate(blockSize)
			end := input.currentTime.Add(blockSize)
			ranges := xtime.Ranges{}
			ranges = ranges.AddRange(xtime.Range{
				Start: start,
				End:   end,
			})

			// Determine which shards we need to bootstrap (based on the randomly
			// generated data)
			allShards := map[uint32]bool{}
			for _, write := range input.writes {
				allShards[write.series.Shard] = true
			}

			// Assign the previously-determined bootstrap range to each known shard
			shardTimeRanges := result.ShardTimeRanges{}
			for shard := range allShards {
				shardTimeRanges[shard] = ranges
			}

			// Perform the bootstrap
			runOpts := testDefaultRunOpts.SetCacheSeriesMetadata(input.shouldCacheSeriesMetadata)
			dataResult, err := source.BootstrapData(md, shardTimeRanges, runOpts)
			if err != nil {
				return false, err
			}

			// Create testValues for each datapoint for comparison
			values := []testValue{}
			for _, write := range input.writes {
				values = append(values, testValue{write.series, write.datapoint.Timestamp, write.datapoint.Value, write.unit, write.annotation})
			}

			err = verifyShardResultsAreCorrect(values, dataResult.ShardResults(), bootstrapOpts)
			if err != nil {
				return false, err
			}

			indexResult, err := source.BootstrapIndex(md, shardTimeRanges, testDefaultRunOpts)
			if err != nil {
				return false, err
			}

			indexBlockSize := md.Options().IndexOptions().BlockSize()
			err = verifyIndexResultsAreCorrect(
				values, map[string]struct{}{}, indexResult.IndexResults(), indexBlockSize)
			if err != nil {
				return false, err
			}

			return true, nil
		},
		genPropTestInputs(testNamespaceID.String()),
	))

	props.TestingRun(t)
}

type propTestInput struct {
	currentTime               time.Time
	writes                    []generatedWrite
	shouldCacheSeriesMetadata bool
}

type generatedWrite struct {
	series     commitlog.Series
	datapoint  ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
}

func (w generatedWrite) String() string {
	return fmt.Sprintf("ID = %v, Datapoint = %+v", w.series.ID.String(), w.datapoint)
}

func genPropTestInputs(ns string) gopter.Gen {
	curriedGenPropTestInput := func(input interface{}) gopter.Gen {
		inputs := input.([]interface{})
		start := inputs[0].(time.Time)
		numDatapoints := inputs[1].(int)
		shouldCacheSeriesMetadata := inputs[2].(bool)
		return genPropTestInput(start, numDatapoints, shouldCacheSeriesMetadata, ns)
	}
	return gopter.CombineGens(
		// Runs iterations of the test starting 1000 hours in the past/future
		gen.TimeRange(time.Now(), blockSize),
		// Run iterations of the test with between 0 and 1000 datapoints
		gen.IntRange(0, 1000),
		// ShouldCacheSeriesMetadata
		gen.Bool(),
	).FlatMap(curriedGenPropTestInput, reflect.TypeOf(propTestInput{}))
}

func genPropTestInput(start time.Time, numDatapoints int, shouldCacheSeriesMetadata bool, ns string) gopter.Gen {
	return gen.SliceOfN(numDatapoints, genWrite(start, ns)).
		Map(func(val interface{}) propTestInput {
			return propTestInput{
				currentTime: start,
				writes:      val.([]generatedWrite),
				shouldCacheSeriesMetadata: shouldCacheSeriesMetadata,
			}
		})
}

func genWrite(start time.Time, ns string) gopter.Gen {
	return gopter.CombineGens(
		// Identifier
		gen.Identifier(),
		// Tag key/val
		gen.Identifier(),
		gen.Identifier(),
		// Boolean indicating whether or not to include tags for this series. We want to
		// sometimes not include tags to ensure that the commitlog writer/readers can
		// handle both series that have tags and those that don't.
		gen.Bool(),
		gen.TimeRange(start, 15*time.Minute),
		// M3TSZ is lossy, so we want to avoid very large numbers with high amounts of precision
		gen.Float64Range(-9999999, 99999999),
	).Map(func(val []interface{}) generatedWrite {
		id := val[0].(string)
		tagKey := val[1].(string)
		tagVal := val[2].(string)
		includeTags := val[3].(bool)
		t := val[4].(time.Time)
		v := val[5].(float64)

		return generatedWrite{
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
