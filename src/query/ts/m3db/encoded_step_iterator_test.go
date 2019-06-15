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
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/test"
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

func (n noopCollector) AddPoint(ts.Datapoint) {}

func benchmarkNextIteration(b *testing.B, iterations int, usePools bool) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	var (
		seriesCount = 100
		start       = time.Now()
		stepSize    = time.Second * 10
		annotation  = ts.Annotation{}
		iters       = make([]encoding.SeriesIterator, seriesCount)
		peeks       = make([]peekValue, seriesCount)
		collectors  = make([]consolidators.StepCollector, seriesCount)
	)

	for i := range collectors {
		collectors[i] = noopCollector{}
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for z := 0; z < seriesCount; z++ {
			series := encoding.NewMockSeriesIterator(ctrl)
			for i := 0; i < iterations; i++ {
				dp := ts.Datapoint{Timestamp: start.Add(time.Duration(i) * time.Second)}
				series.EXPECT().Current().Return(dp, xtime.Second, annotation)
			}

			series.EXPECT().Next().Return(true).Times(iterations)
			series.EXPECT().Next().Return(false).AnyTimes()
			series.EXPECT().Err().Return(nil).AnyTimes()
			iters[z] = series
		}

		it := &encodedStepIterWithCollector{
			stepTime: start,
			meta: block.Metadata{
				Bounds: models.Bounds{
					Start:    start,
					StepSize: stepSize,
					Duration: stepSize * time.Duration(iterations),
				},
			},

			seriesCollectors: collectors,
			seriesPeek:       peeks,
			seriesIters:      iters,
		}

		if usePools {
			opts := xsync.
				NewPooledWorkerPoolOptions().
				SetGrowOnDemand(false).
				SetKillWorkerProbability(0).
				SetNumShards(1)

			readWorkerPools, err := xsync.NewPooledWorkerPool(64, opts)
			require.NoError(b, err)
			readWorkerPools.Init()
			it.workerPool = readWorkerPools
		}

		b.StartTimer()
		for it.Next() {
		}
	}
}

// pooled
// BenchmarkNextIteration/10_pooled-8    	  100	  18177326 ns/op	 2763900 B/op	   39759 allocs/op
// BenchmarkNextIteration/100_pooled-8   	   10	 150897231 ns/op	25925763 B/op	  374966 allocs/op
// BenchmarkNextIteration/200_pooled-8   	    5	 291013756 ns/op	51654841 B/op	  747171 allocs/op
// BenchmarkNextIteration/500_pooled-8   	    2	 729015537 ns/op	128613736 B/op	 1863580 allocs/op
// BenchmarkNextIteration/1000_pooled-8  	    1	1432259618 ns/op	257045408 B/op	 3724192 allocs/op
// BenchmarkNextIteration/2000_pooled-8  	    1	2938592417 ns/op	516488480 B/op	 7445496 allocs/op
// unpooled
// BenchmarkNextIteration/10_unpooled-8  	  100	  13790754 ns/op	 2618534 B/op	   38606 allocs/op
// BenchmarkNextIteration/100_unpooled-8 	   10	 112089616 ns/op	24622001 B/op	  364706 allocs/op
// BenchmarkNextIteration/200_unpooled-8 	    5	 228854771 ns/op	49066000 B/op	  726809 allocs/op
// BenchmarkNextIteration/500_unpooled-8 	    2	 559993830 ns/op	122169848 B/op	 1812916 allocs/op
// BenchmarkNextIteration/1000_unpooled-8	    1	1185663351 ns/op	244177392 B/op	 3623023 allocs/op
// BenchmarkNextIteration/2000_unpooled-8	    1	2459521396 ns/op	490771584 B/op	 7243324 allocs/op
func BenchmarkNextIteration(b *testing.B) {
	for _, usePools := range []bool{true, false} {
		for _, s := range []int{10, 100, 200, 500, 1000, 2000} {
			name := fmt.Sprintf("%d", s)
			if usePools {
				name = name + "_pooled"
			} else {
				name = name + "_unpooled"
			}
			b.Run(name, func(b *testing.B) {
				benchmarkNextIteration(b, s, usePools)
			})
		}
	}
}
