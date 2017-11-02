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
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/ts"
	xtime "github.com/m3db/m3x/time"
)

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
			source := newCommitLogSource(bootstrapOpts)

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

			// Assogm the previously-determined bootstrap range to each known shard
			shardTimeRanges := result.ShardTimeRanges{}
			for shard := range allShards {
				shardTimeRanges[shard] = ranges
			}

			// Perform the bootstrap
			result, err := source.Read(md, shardTimeRanges, testDefaultRunOpts)
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
		gen.TimeRange(time.Now(), 24*time.Hour),
		gen.IntRange(0, 1000),
	).FlatMap(curriedGenPropTestInput, reflect.TypeOf(propTestInput{}))
}

func genPropTestInput(start interface{}, numDatapoints int, ns string) gopter.Gen {
	startTime := start.(time.Time)
	return gopter.CombineGens(
		gen.SliceOfN(numDatapoints, genWrite(startTime, ns)),
	).Map(func(val []interface{}) propTestInput {
		return propTestInput{
			currentTime: startTime,
			writes:      val[0].([]generatedWrite),
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
		gen.UInt32Range(0, 8192),
	).Map(func(val []interface{}) generatedWrite {
		id := val[0].(string)
		t := val[1].(time.Time)
		v := val[2].(float64)

		return generatedWrite{
			series: commitlog.Series{
				ID:          ts.StringID(id),
				Namespace:   ts.StringID(ns),
				Shard:       idToShard(id),
				UniqueIndex: idToIdx(id),
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

func idToIdx(s string) uint64 {
	metricIdx.Lock()
	defer metricIdx.Unlock()

	idx, ok := metricIdx.idToIdx[s]
	if ok {
		return idx
	}

	idx = metricIdx.idx
	metricIdx.idx++
	metricIdx.idToIdx[s] = idx
	return idx
}

func idToShard(s string) uint32 {
	metricShard.Lock()
	defer metricShard.Unlock()

	shard, ok := metricShard.idToShard[s]
	if ok {
		return shard
	}

	shard = metricShard.shard
	metricShard.shard++
	metricShard.idToShard[s] = shard
	return shard
}
