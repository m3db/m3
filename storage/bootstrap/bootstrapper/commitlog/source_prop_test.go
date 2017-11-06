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
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/spaolacci/murmur3"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/ts"
	xtime "github.com/m3db/m3x/time"
)

const maxShards = 8192

func TestCommitLogSourcePropCorrectlyBootstrapsFromCommitlog(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.Rng.Seed(123456789)
	parameters.MinSuccessfulTests = 100
	props := gopter.NewProperties(parameters)

	props.Property("Commitlog bootstrapping properly bootstraps the entire commitlog", prop.ForAll(
		func(input propTestInput) (bool, error) {
			// Make sure we have a new directory for each test run
			dir, err := ioutil.TempDir("", "foo")
			if err != nil {
				return false, err
			}

			// Configure the commitlog to use the test directory and set the blocksize
			commitLogOpts := commitlog.NewOptions().
				SetBlockSize(2 * time.Hour).
				SetFilesystemOptions(fs.NewOptions().SetFilePathPrefix(dir))
			bootstrapOpts := testOptions().SetCommitLogOptions(commitLogOpts)

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
				log.Write(context.NewContext(), write.series, write.datapoint, write.unit, write.annotation)
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
			md := testNsMetadata(t)
			blockSize := md.Options().RetentionOptions().BlockSize()
			start := input.currentTime.Truncate(blockSize)
			end := input.currentTime.Add(blockSize)
			ranges := xtime.NewRanges()
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
			result, err := source.Bootstrap(md, shardTimeRanges, testDefaultRunOpts)
			if err != nil {
				return false, err
			}

			// Create testValues for each datapoint for comparison
			values := []testValue{}
			for _, write := range input.writes {
				values = append(values, testValue{write.series, write.datapoint.Timestamp, write.datapoint.Value, write.unit, write.annotation})
			}

			err = verifyShardResultsAreCorrect(values, result.ShardResults(), bootstrapOpts)
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
	currentTime time.Time
	writes      []generatedWrite
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
		return genPropTestInput(start, numDatapoints, ns)
	}
	return gopter.CombineGens(
		// Runs iterations of the test starting 1000 hours in the past/future
		gen.TimeRange(time.Now().Add(-1000*time.Hour), 1000*time.Hour),
		// Run iterations of the test with between 0 and 1000 datapoints
		gen.IntRange(0, 1000),
	).FlatMap(curriedGenPropTestInput, reflect.TypeOf(propTestInput{}))
}

func genPropTestInput(start time.Time, numDatapoints int, ns string) gopter.Gen {
	return gen.SliceOfN(numDatapoints, genWrite(start, ns)).
		Map(func(val interface{}) propTestInput {
			return propTestInput{
				currentTime: start,
				writes:      val.([]generatedWrite),
			}
		})
}

func genWrite(start time.Time, ns string) gopter.Gen {
	return gopter.CombineGens(
		gen.Identifier(),
		gen.TimeRange(start, 15*time.Minute),
		// M3TSZ is lossy, so we want to avoid very large numbers with high amounts of precision
		gen.Float64Range(-9999999, 99999999),
		// Some of the commitlog bootstrapping code is O(N) with respect to the
		// number of shards, so we cap it to prevent timeouts
		gen.UInt32Range(0, maxShards),
	).Map(func(val []interface{}) generatedWrite {
		id := val[0].(string)
		t := val[1].(time.Time)
		v := val[2].(float64)

		return generatedWrite{
			series: commitlog.Series{
				ID:          ts.StringID(id),
				Namespace:   ts.StringID(ns),
				Shard:       hashIDToShard(ts.StringID(id)),
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

type globalMetricShard struct {
	sync.Mutex

	shard     uint32
	idToShard map[string]uint32
}

var metricIdx = globalMetricIdx{
	idToIdx: make(map[string]uint64),
}

var metricShard = globalMetricShard{
	idToShard: make(map[string]uint32),
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
func hashIDToShard(id ts.ID) uint32 {
	return murmur3.Sum32(id.Data().Get()) % uint32(maxShards)
}
