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

package m3db

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
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
		opts := NewOptions().
			SetLookbackDuration(1 * time.Minute).
			SetSplitSeriesByBlock(false)
		require.NoError(t, opts.Validate())
		if withPools {
			opts = withPool(t, opts)
		}

		blocks, bounds := generateBlocks(t, tt.stepSize, opts)
		j := 0
		for i, block := range blocks {
			iters, err := block.StepIter()
			require.NoError(t, err)

			require.True(t, bounds.Equals(iters.Meta().Bounds))
			verifyMetas(t, i, iters.Meta(), iters.SeriesMeta())
			for iters.Next() {
				step := iters.Current()
				vals := step.Values()
				test.EqualsWithNans(t, tt.expected[j], vals)
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
		opts := NewOptions().
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
			idx := verifyBoundsAndGetBlockIndex(t, bounds, iters.Meta().Bounds)
			verifyMetas(t, i, iters.Meta(), iters.SeriesMeta())
			for iters.Next() {
				step := iters.Current()
				vals := step.Values()
				test.EqualsWithNans(t, tt.expected[idx][j], vals)
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
	opts := NewOptions().
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
func (n noopCollector) BufferReset()             {}

func benchmarkNextIteration(b *testing.B, iterations int, usePools bool) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	var (
		seriesCount   = 1
		replicasCount = 3
		start         = time.Now()
		stepSize      = time.Second * 10
		window        = stepSize * time.Duration(iterations)
		end           = start.Add(window)
		iters         = make([]encoding.SeriesIterator, seriesCount)
		itersReset    = make([]func(), seriesCount)
		collectors    = make([]consolidators.StepCollector, seriesCount)
		peeks         = make([]peekValue, seriesCount)

		encodingOpts = encoding.NewOptions()
		namespaceID  = ident.StringID("namespace")
	)

	for i := 0; i < seriesCount; i++ {
		collectors[i] = noopCollector{}

		encoder := m3tsz.NewEncoder(start, checked.NewBytes(nil, nil),
			m3tsz.DefaultIntOptimizationEnabled, encodingOpts)

		timestamp := start
		for j := 0; j < iterations; j++ {
			timestamp = timestamp.Add(time.Duration(j) * stepSize)
			dp := ts.Datapoint{Timestamp: timestamp, Value: float64(j)}
			err := encoder.Encode(dp, xtime.Second, nil)
			require.NoError(b, err)
		}

		iterAlloc := func(
			r io.Reader,
			d namespace.SchemaDescr,
		) encoding.ReaderIterator {
			decoder := m3tsz.NewDecoder(m3tsz.DefaultIntOptimizationEnabled,
				encodingOpts)
			return decoder.Decode(r)
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

			iter := encoding.NewMultiReaderIterator(iterAlloc, nil)
			iter.Reset(readers, start, window, nil)
			replicas[j].iter = iter

			replicasIters[j] = iter
		}

		seriesID := ident.StringID(fmt.Sprintf("foo.%d", i))

		tags, err := ident.NewTagStringsIterator("foo", "bar", "baz", "qux")
		require.NoError(b, err)

		iters[i] = encoding.NewSeriesIterator(
			encoding.SeriesIteratorOptions{}, nil)

		itersReset[i] = func() {
			// Reset the replica iters.
			for _, replica := range replicas {
				for _, reader := range replica.readers {
					reader.Reset(data)
				}
				replica.iter.Reset(replica.readers, start, window, nil)
			}
			// Reset the series iterator.
			iters[i].Reset(encoding.SeriesIteratorOptions{
				ID:             seriesID,
				Namespace:      namespaceID,
				Tags:           tags,
				Replicas:       replicasIters,
				StartInclusive: start,
				EndExclusive:   end,
			})
		}
	}

	duration := stepSize * time.Duration(iterations)
	it := &encodedStepIterWithCollector{
		stepTime: start,
		blockEnd: start.Add(duration),
		meta: block.Metadata{
			Bounds: models.Bounds{
				Start:    start,
				StepSize: stepSize,
				Duration: duration,
			},
		},

		seriesCollectors: collectors,
		seriesPeek:       peeks,
		seriesIters:      iters,
	}

	if usePools {
		opts := xsync.NewPooledWorkerPoolOptions()
		readWorkerPools, err := xsync.NewPooledWorkerPool(1024, opts)
		require.NoError(b, err)
		readWorkerPools.Init()
		it.workerPool = readWorkerPools
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it.stepTime = start
		it.finished = false
		for i := range it.seriesPeek {
			it.seriesPeek[i] = peekValue{}
		}

		// Reset all the underlying compressed series iterators.
		for _, reset := range itersReset {
			reset()
		}

		for it.Next() {
		}
		require.NoError(b, it.Err())
	}
}

// pooled
// BenchmarkNextIteration/10_pooled-8    	 100	  14394817 ns/op   31149 B/op   508 allocs/op
// BenchmarkNextIteration/100_pooled-8   	  10	 145047833 ns/op  280805 B/op  4607 allocs/op
// BenchmarkNextIteration/200_pooled-8   	   5	 289788726 ns/op  558550 B/op  9163 allocs/op
// BenchmarkNextIteration/500_pooled-8   	   2	 730133093 ns/op 1393132 B/op 22828 allocs/op
// BenchmarkNextIteration/1000_pooled-8  	   1	1418842429 ns/op 2790352 B/op 45593 allocs/op
// BenchmarkNextIteration/2000_pooled-8  	   1	2895354142 ns/op 5626000 B/op 91090 allocs/op
// unpooled
// BenchmarkNextIteration/10_unpooled-8  	 100	  14421499 ns/op   28868 B/op   481 allocs/op
// BenchmarkNextIteration/100_unpooled-8 	  10	 138518888 ns/op  259644 B/op  4354 allocs/op
// BenchmarkNextIteration/200_unpooled-8 	   5	 281995023 ns/op  516390 B/op  8658 allocs/op
// BenchmarkNextIteration/500_unpooled-8 	   2	 709990332 ns/op 1287936 B/op 21567 allocs/op
// BenchmarkNextIteration/1000_unpooled-8	   1	1386331977 ns/op 2581040 B/op 43083 allocs/op
// BenchmarkNextIteration/2000_unpooled-8	   1	2666497332 ns/op 5209520 B/op 86089 allocs/op
func BenchmarkNextIteration(b *testing.B) {
	for _, useGoroutineWorkerPools := range []bool{true, false} {
		for _, s := range []int{10, 100, 200, 500, 1000, 2000} {
			name := fmt.Sprintf("%d", s)
			if useGoroutineWorkerPools {
				name = name + "_parallel"
			} else {
				name = name + "_sequential"
			}
			b.Run(name, func(b *testing.B) {
				benchmarkNextIterationReal(b, s, useGoroutineWorkerPools)
			})
		}
	}
}

func benchmarkNextIterationReal(b *testing.B, iterations int, usePools bool) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	var (
		seriesCount = 1
		start       = time.Now()
		stepSize    = time.Second * 10
		iters       = make([]encoding.SeriesIterator, seriesCount)
		collectors  = make([]consolidators.StepCollector, seriesCount)
		peeks       = make([]peekValue, seriesCount)

		points = make([]test.Datapoint, iterations)
	)

	for i := 0; i < iterations; i++ {
		points[i] = test.Datapoint{Offset: time.Duration(i) * 5 * time.Second}
	}

	for i := 0; i < seriesCount; i++ {
		collectors[i] = noopCollector{}
	}

	duration := stepSize * time.Duration(iterations)
	it := &encodedStepIterWithCollector{
		stepTime: start,
		blockEnd: start.Add(duration),
		meta: block.Metadata{
			Bounds: models.Bounds{
				Start:    start,
				StepSize: stepSize,
				Duration: duration,
			},
		},

		seriesCollectors: collectors,
		seriesPeek:       peeks,
	}

	if usePools {
		opts := xsync.NewPooledWorkerPoolOptions()
		readWorkerPools, err := xsync.NewPooledWorkerPool(128, opts)
		require.NoError(b, err)
		readWorkerPools.Init()
		it.workerPool = readWorkerPools
	}

	for i := 0; i < b.N; i++ {
		it.stepTime = start
		it.finished = false
		for i := range it.seriesPeek {
			it.seriesPeek[i] = peekValue{}
		}

		for i := range iters {
			iter, _, _ := test.BuildCustomIterator(
				[][]test.Datapoint{points},
				map[string]string{},
				"id",
				"ns",
				start,
				stepSize*time.Duration(iterations), stepSize,
			)

			iters[i] = iter
		}

		for it.Next() {
		}
	}
}
