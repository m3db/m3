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

package m3

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/pools"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/test/compare"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/pkg/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withPool(t testing.TB, options Options) Options {
	opts := xsync.
		NewPooledWorkerPoolOptions().
		SetGrowOnDemand(false).
		SetKillWorkerProbability(0).
		SetNumShards(1)

	readWorkerPools, err := xsync.NewPooledWorkerPool(64, opts)
	require.NoError(t, err)
	readWorkerPools.Init()

	return options.SetReadWorkerPool(readWorkerPools)
}

var consolidatedStepIteratorTests = []struct {
	name     string
	stepSize time.Duration
	expected [][]float64
}{
	{
		name:     "1 minute",
		stepSize: time.Minute,
		expected: [][]float64{
			{nan, nan, nan},
			{nan, nan, nan},
			{nan, 10, nan},
			{nan, 20, 100},
			{nan, 20, 100},
			{nan, nan, nan},
			{1, nan, nan},
			{1, nan, nan},
			{3, nan, nan},
			{4, nan, 200},
			{5, nan, 200},
			{6, nan, nan},
			{7, nan, nan},
			{7, 30, nan},
			{nan, 30, nan},
			{nan, nan, 300},
			{nan, nan, 300},
			{8, nan, nan},
			{8, nan, nan},
			{nan, 40, nan},
			{nan, nan, nan},
			{nan, nan, 400},
			{9, nan, 400},
			{9, nan, nan},
			{nan, nan, 500},
			{nan, nan, 500},
			{nan, nan, nan},
			{nan, nan, nan},
			{nan, nan, nan},
			{nan, nan, nan},
		},
	},
	{
		name:     "2 minute",
		stepSize: time.Minute * 2,
		expected: [][]float64{
			{nan, nan, nan},
			{nan, 10, nan},
			{nan, 20, 100},
			{1, nan, nan},
			{3, nan, nan},
			{5, nan, 200},
			{7, nan, nan},
			{nan, 30, nan},
			{nan, nan, 300},
			{8, nan, nan},
			{nan, nan, nan},
			{9, nan, 400},
			{nan, nan, 500},
			{nan, nan, nan},
			{nan, nan, nan},
		},
	},
	{
		name:     "3 minute",
		stepSize: time.Minute * 3,
		expected: [][]float64{
			{nan, nan, nan},
			{nan, 20, 100},
			{1, nan, nan},
			{4, nan, 200},
			{7, nan, nan},
			{nan, nan, 300},
			{8, nan, nan},
			{nan, nan, 400},
			{nan, nan, 500},
			{nan, nan, nan},
		},
	},
}

func testConsolidatedStepIteratorMinuteLookback(t *testing.T, withPools bool) {
	for _, tt := range consolidatedStepIteratorTests {
		opts := newTestOpts().
			SetLookbackDuration(1 * time.Minute).
			SetSplitSeriesByBlock(false)
		require.NoError(t, opts.Validate())
		if withPools {
			opts = withPool(t, opts)
		}

		blocks, bounds := generateBlocks(t, tt.stepSize, opts)
		j := 0
		for i, bl := range blocks {
			iters, err := bl.StepIter()
			require.NoError(t, err)
			assert.Equal(t, block.BlockM3TSZCompressed, bl.Info().Type())

			require.True(t, bounds.Equals(bl.Meta().Bounds))
			verifyMetas(t, i, bl.Meta(), iters.SeriesMeta())
			for iters.Next() {
				step := iters.Current()
				vals := step.Values()
				compare.EqualsWithNans(t, tt.expected[j], vals)
				j++
			}

			require.NoError(t, iters.Err())
		}
	}
}

func TestConsolidatedStepIteratorMinuteLookbackParallel(t *testing.T) {
	testConsolidatedStepIteratorMinuteLookback(t, true)
}

func TestConsolidatedStepIteratorMinuteLookbackSequential(t *testing.T) {
	testConsolidatedStepIteratorMinuteLookback(t, false)
}

var consolidatedStepIteratorTestsSplitByBlock = []struct {
	name     string
	stepSize time.Duration
	expected [][][]float64
}{
	{
		name:     "1 minute",
		stepSize: time.Minute,
		expected: [][][]float64{
			{
				{nan, nan, nan},
				{nan, nan, nan},
				{nan, 10, nan},
				{nan, 20, 100},
				{nan, nan, nan},
				{nan, nan, nan},
			},
			{
				{1, nan, nan},
				{nan, nan, nan},
				{3, nan, nan},
				{4, nan, 200},
				{5, nan, nan},
				{6, nan, nan},
			},
			{
				{7, nan, nan},
				{nan, nan, nan},
				{nan, nan, nan},
				{nan, nan, 300},
				{nan, nan, nan},
				{8, nan, nan},
			},
			{
				{nan, nan, nan},
				{nan, nan, nan},
				{nan, nan, nan},
				{nan, nan, 400},
				{9, nan, nan},
				{nan, nan, nan},
			},
			{
				{nan, nan, 500},
				{nan, nan, nan},
				{nan, nan, nan},
				{nan, nan, nan},
				{nan, nan, nan},
				{nan, nan, nan},
			},
		},
	},
	{
		name:     "2 minute",
		stepSize: time.Minute * 2,
		expected: [][][]float64{
			{
				{nan, nan, nan},
				{nan, 10, nan},
				{nan, nan, nan},
			},
			{
				{1, nan, nan},
				{3, nan, nan},
				{5, nan, nan},
			},
			{
				{7, nan, nan},
				{nan, nan, nan},
				{nan, nan, nan},
			},
			{
				{nan, nan, nan},
				{nan, nan, nan},
				{9, nan, nan},
			},
			{
				{nan, nan, 500},
				{nan, nan, nan},
				{nan, nan, nan},
			},
		},
	},
	{
		name:     "3 minute",
		stepSize: time.Minute * 3,
		expected: [][][]float64{
			{
				{nan, nan, nan},
				{nan, 20, 100},
			},
			{
				{1, nan, nan},
				{4, nan, 200},
			},
			{
				{7, nan, nan},
				{nan, nan, 300},
			},
			{
				{nan, nan, nan},
				{nan, nan, 400},
			},
			{
				{nan, nan, 500},
				{nan, nan, nan},
			},
		},
	},
}

func testConsolidatedStepIteratorSplitByBlock(t *testing.T, withPools bool) {
	for _, tt := range consolidatedStepIteratorTestsSplitByBlock {
		opts := newTestOpts().
			SetLookbackDuration(0).
			SetSplitSeriesByBlock(true)
		require.NoError(t, opts.Validate())
		if withPools {
			opts = withPool(t, opts)
		}

		blocks, bounds := generateBlocks(t, tt.stepSize, opts)
		for i, block := range blocks {
			iters, err := block.StepIter()
			require.NoError(t, err)

			j := 0
			idx := verifyBoundsAndGetBlockIndex(t, bounds, block.Meta().Bounds)
			verifyMetas(t, i, block.Meta(), iters.SeriesMeta()) // <-
			for iters.Next() {
				step := iters.Current()
				vals := step.Values()
				compare.EqualsWithNans(t, tt.expected[idx][j], vals)
				j++
			}

			require.NoError(t, iters.Err())
		}
	}
}

func TestConsolidatedStepIteratorSplitByBlockParallel(t *testing.T) {
	testConsolidatedStepIteratorSplitByBlock(t, true)
}

func TestConsolidatedStepIteratorSplitByBlockSequential(t *testing.T) {
	testConsolidatedStepIteratorSplitByBlock(t, false)
}

func benchmarkSingleBlock(b *testing.B, withPools bool) {
	opts := newTestOpts().
		SetLookbackDuration(1 * time.Minute).
		SetSplitSeriesByBlock(false)
	require.NoError(b, opts.Validate())
	if withPools {
		opts = withPool(b, opts)
	}

	for i := 0; i < b.N; i++ {
		for _, tt := range consolidatedStepIteratorTests {
			b.StopTimer()
			blocks, _ := generateBlocks(b, tt.stepSize, opts)
			b.StartTimer()
			for _, block := range blocks {
				iters, _ := block.StepIter()
				for iters.Next() {
					// no-op
				}

				require.NoError(b, iters.Err())
			}
		}
	}
}

func BenchmarkSingleBlockParallel(b *testing.B) {
	benchmarkSingleBlock(b, true)
}

func BenchmarkSingleBlockSerialll(b *testing.B) {
	benchmarkSingleBlock(b, false)
}

type noopCollector struct{}

func (n noopCollector) AddPoint(dp ts.Datapoint) {}
func (n noopCollector) BufferStep()              {}
func (n noopCollector) BufferStepCount() int     { return 0 }

type iterType uint

const (
	stepSequential iterType = iota
	stepParallel
	seriesSequential
	seriesBatch
)

func (t iterType) name(name string) string {
	var n string
	switch t {
	case stepParallel:
		n = "parallel"
	case stepSequential:
		n = "sequential"
	case seriesSequential:
		n = "series"
	case seriesBatch:
		n = "series_batch"
	default:
		panic(fmt.Sprint("bad iter type", t))
	}

	return fmt.Sprintf("%s_%s", n, name)
}

type reset func()
type stop func()

// newTestOptions provides options with very small/non-existent pools
// so that memory profiles don't get cluttered with pooled allocated objects.
func newTestOpts() Options {
	poolOpts := pool.NewObjectPoolOptions().SetSize(1)
	bytesPool := pool.NewCheckedBytesPool(nil, poolOpts,
		func(s []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(s, poolOpts)
		})
	bytesPool.Init()

	iteratorPools := pools.BuildIteratorPools(encoding.NewOptions(),
		pools.BuildIteratorPoolsOptions{
			Replicas:               1,
			SeriesIteratorPoolSize: 1,
			SeriesIteratorsPoolBuckets: []pool.Bucket{
				{Capacity: 1, Count: 1},
			},
			SeriesIDBytesPoolBuckets: []pool.Bucket{
				{Capacity: 1, Count: 1},
			},
			CheckedBytesWrapperPoolSize: 1,
		})
	return newOptions(bytesPool, iteratorPools)
}

func iterToFetchResult(
	iters []encoding.SeriesIterator,
) (consolidators.SeriesFetchResult, error) {
	return consolidators.NewSeriesFetchResult(
		encoding.NewSeriesIterators(iters),
		nil,
		block.NewResultMetadata(),
	)
}

func setupBlock(b *testing.B, iterations int, t iterType) (block.Block, reset, stop) {
	var (
		seriesCount   = 1000
		replicasCount = 3
		start         = xtime.Now()
		stepSize      = time.Second * 10
		window        = stepSize * time.Duration(iterations)
		end           = start.Add(window)
		iters         = make([]encoding.SeriesIterator, seriesCount)
		itersReset    = make([]func(), seriesCount)

		encodingOpts = encoding.NewOptions()
		namespaceID  = ident.StringID("namespace")
	)

	for i := 0; i < seriesCount; i++ {
		encoder := m3tsz.NewEncoder(start, checked.NewBytes(nil, nil),
			m3tsz.DefaultIntOptimizationEnabled, encodingOpts)

		timestamp := start
		for j := 0; j < iterations; j++ {
			timestamp = timestamp.Add(time.Duration(j) * stepSize)
			dp := ts.Datapoint{TimestampNanos: timestamp, Value: float64(j)}
			err := encoder.Encode(dp, xtime.Second, nil)
			require.NoError(b, err)
		}

		data := encoder.Discard()
		replicas := make([]struct {
			readers []xio.SegmentReader
			iter    encoding.MultiReaderIterator
		}, replicasCount)
		replicasIters := make([]encoding.MultiReaderIterator, replicasCount)
		for j := 0; j < replicasCount; j++ {
			readers := []xio.SegmentReader{xio.NewSegmentReader(data)}
			replicas[j].readers = readers

			// Use the same decoder over and over to avoid allocations.
			readerIter := m3tsz.NewReaderIterator(nil,
				m3tsz.DefaultIntOptimizationEnabled, encodingOpts)

			iterAlloc := func(
				r xio.Reader64,
				d namespace.SchemaDescr,
			) encoding.ReaderIterator {
				readerIter.Reset(r, d)
				return readerIter
			}

			iter := encoding.NewMultiReaderIterator(iterAlloc, nil)
			iter.Reset(readers, start, window, nil)
			replicas[j].iter = iter

			replicasIters[j] = iter
		}

		seriesID := ident.StringID(fmt.Sprintf("foo.%d", i+1))
		tags, err := ident.NewTagStringsIterator("foo", "bar", "baz", "qux")
		require.NoError(b, err)

		iter := encoding.NewSeriesIterator(
			encoding.SeriesIteratorOptions{}, nil)

		iters[i] = iter
		itersReset[i] = func() {
			// Reset the replica iters.
			for _, replica := range replicas {
				for _, reader := range replica.readers {
					reader.Reset(data)
				}
				replica.iter.Reset(replica.readers, start, window, nil)
			}
			// Reset the series iterator.
			iter.Reset(encoding.SeriesIteratorOptions{
				ID:             seriesID,
				Namespace:      namespaceID,
				Tags:           tags,
				Replicas:       replicasIters,
				StartInclusive: start,
				EndExclusive:   end,
			})
		}
	}

	usePools := t == stepParallel

	opts := newTestOpts()
	if usePools {
		poolOpts := xsync.NewPooledWorkerPoolOptions()
		readWorkerPools, err := xsync.NewPooledWorkerPool(1024, poolOpts)
		require.NoError(b, err)
		readWorkerPools.Init()
		opts = opts.SetReadWorkerPool(readWorkerPools)
	}

	for _, reset := range itersReset {
		reset()
	}

	res, err := iterToFetchResult(iters)
	require.NoError(b, err)

	block, err := NewEncodedBlock(
		res,
		models.Bounds{
			Start:    start,
			StepSize: stepSize,
			Duration: window,
		}, false, opts)

	require.NoError(b, err)
	return block, func() {
			for _, reset := range itersReset {
				reset()
			}
		},
		setupProf(usePools, iterations)
}

func setupProf(usePools bool, iterations int) stop {
	var prof interface {
		Stop()
	}
	if os.Getenv("PROFILE_TEST_CPU") == "true" {
		key := profileTakenKey{
			profile:    "cpu",
			pools:      usePools,
			iterations: iterations,
		}
		if v := profilesTaken[key]; v == 2 {
			prof = profile.Start(profile.CPUProfile)
		}

		profilesTaken[key] = profilesTaken[key] + 1
	}

	if os.Getenv("PROFILE_TEST_MEM") == "true" {
		key := profileTakenKey{
			profile:    "mem",
			pools:      usePools,
			iterations: iterations,
		}

		if v := profilesTaken[key]; v == 2 {
			prof = profile.Start(profile.MemProfile)
		}

		profilesTaken[key] = profilesTaken[key] + 1
	}
	return func() {
		if prof != nil {
			prof.Stop()
		}
	}
}

func benchmarkNextIteration(b *testing.B, iterations int, t iterType) {
	bl, reset, close := setupBlock(b, iterations, t)
	defer close()

	if t == seriesSequential {
		it, err := bl.SeriesIter()
		require.NoError(b, err)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reset()
			for it.Next() {
			}

			require.NoError(b, it.Err())
		}

		return
	}

	if t == seriesBatch {
		batches, err := bl.MultiSeriesIter(runtime.GOMAXPROCS(0))
		require.NoError(b, err)

		var wg sync.WaitGroup
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reset()

			for _, batch := range batches {
				it := batch.Iter
				wg.Add(1)
				go func() {
					for it.Next() {
					}

					wg.Done()
				}()
			}

			wg.Wait()
			for _, batch := range batches {
				require.NoError(b, batch.Iter.Err())
			}
		}

		return
	}

	it, err := bl.StepIter()
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reset()
		for it.Next() {
		}

		require.NoError(b, it.Err())
	}
}

type profileTakenKey struct {
	profile    string
	pools      bool
	iterations int
}

var (
	profilesTaken = make(map[profileTakenKey]int)
)

/*
	$ go test -v -run none -bench BenchmarkNextIteration
	goos: darwin
	goarch: amd64
	pkg: github.com/m3db/m3/src/query/ts/m3db

	BenchmarkNextIteration/sequential_10-12      4112  282491 ns/op
	BenchmarkNextIteration/parallel_10-12        4214  249335 ns/op
	BenchmarkNextIteration/series_10-12          4515  248946 ns/op
	BenchmarkNextIteration/series_batch_10-12    4434  269776 ns/op

	BenchmarkNextIteration/sequential_100-12     4069  267836 ns/op
	BenchmarkNextIteration/parallel_100-12       4126  283069 ns/op
	BenchmarkNextIteration/series_100-12         4146  266928 ns/op
	BenchmarkNextIteration/series_batch_100-12   4399  255991 ns/op

	BenchmarkNextIteration/sequential_200-12     4267  245249 ns/op
	BenchmarkNextIteration/parallel_200-12       4233  239597 ns/op
	BenchmarkNextIteration/series_200-12         4365  245924 ns/op
	BenchmarkNextIteration/series_batch_200-12   4485  235055 ns/op

	BenchmarkNextIteration/sequential_500-12     5108  230085 ns/op
	BenchmarkNextIteration/parallel_500-12       4802  230694 ns/op
	BenchmarkNextIteration/series_500-12         4831  229797 ns/op
	BenchmarkNextIteration/series_batch_500-12   4880  246588 ns/op

	BenchmarkNextIteration/sequential_1000-12    3807  265449 ns/op
	BenchmarkNextIteration/parallel_1000-12      5062  254942 ns/op
	BenchmarkNextIteration/series_1000-12        4423  236796 ns/op
	BenchmarkNextIteration/series_batch_1000-12  4772  251977 ns/op

	BenchmarkNextIteration/sequential_2000-12    4916  243593 ns/op
	BenchmarkNextIteration/parallel_2000-12      4743  253677 ns/op
	BenchmarkNextIteration/series_2000-12        4078  256375 ns/op
	BenchmarkNextIteration/series_batch_2000-12  4465  242323 ns/op
*/
func BenchmarkNextIteration(b *testing.B) {
	iterTypes := []iterType{
		stepSequential,
		stepParallel,
		seriesSequential,
		seriesBatch,
	}

	for _, s := range []int{10, 100, 200, 500, 1000, 2000} {
		for _, t := range iterTypes {
			name := t.name(fmt.Sprintf("%d", s))
			b.Run(name, func(b *testing.B) {
				benchmarkNextIteration(b, s, t)
			})
		}

		// NB: this is for clearer groupings.
		println()
	}
}
