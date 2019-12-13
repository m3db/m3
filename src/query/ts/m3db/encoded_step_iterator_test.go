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
	"os"
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
		opts := NewOptions().
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
			idx := verifyBoundsAndGetBlockIndex(t, bounds, block.Meta().Bounds)
			verifyMetas(t, i, block.Meta(), iters.SeriesMeta())
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

type iterType uint

const (
	stepSequential iterType = iota
	stepParallel
	seriesSequential
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
	default:
		panic(fmt.Sprint("bad iter type", t))
	}

	return fmt.Sprintf("%s_%s", n, name)
}

func benchmarkNextIteration(b *testing.B, iterations int, t iterType) {
	var (
		seriesCount   = 100
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
		collectors[i] = consolidators.NewStepLookbackConsolidator(
			stepSize,
			stepSize,
			start,
			consolidators.TakeLast)

		encoder := m3tsz.NewEncoder(start, checked.NewBytes(nil, nil),
			m3tsz.DefaultIntOptimizationEnabled, encodingOpts)

		timestamp := start
		for j := 0; j < iterations; j++ {
			timestamp = timestamp.Add(time.Duration(j) * stepSize)
			dp := ts.Datapoint{Timestamp: timestamp, Value: float64(j)}
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
				r io.Reader,
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

		seriesID := ident.StringID(fmt.Sprintf("foo.%d", i))

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
	if os.Getenv("PROFILE_TEST_CPU") == "true" {
		key := profileTakenKey{
			profile:    "cpu",
			pools:      usePools,
			iterations: iterations,
		}
		if v := profilesTaken[key]; v == 2 {
			p := profile.Start(profile.CPUProfile)
			defer p.Stop()
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
			p := profile.Start(profile.MemProfile)
			defer p.Stop()
		}

		profilesTaken[key] = profilesTaken[key] + 1
	}

	if t == seriesSequential {
		sm := make([]block.SeriesMeta, seriesCount)
		for i := range iters {
			sm[i] = block.SeriesMeta{}
		}

		it := encodedSeriesIterUnconsolidated{
			idx: -1,
			meta: block.Metadata{
				Bounds: models.Bounds{
					Start:    start,
					StepSize: stepSize,
					Duration: window,
				},
			},

			seriesIters:      iters,
			seriesMeta:       sm,
			lookbackDuration: time.Minute * 5,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it.idx = -1
			// Reset all the underlying compressed series iterators.
			for _, reset := range itersReset {
				reset()
			}

			for it.Next() {
			}
			require.NoError(b, it.Err())
		}

		return
	}

	it := &encodedStepIterWithCollector{
		stepTime: start,
		blockEnd: end,
		meta: block.Metadata{
			Bounds: models.Bounds{
				Start:    start,
				StepSize: stepSize,
				Duration: window,
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
		it.bufferTime = time.Time{}
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

type profileTakenKey struct {
	profile    string
	pools      bool
	iterations int
}

var (
	profilesTaken = make(map[profileTakenKey]int)
)

// $ go test -v -run none -bench BenchmarkNextIteration
// goos: darwin
// goarch: amd64
// pkg: github.com/m3db/m3/src/query/ts/m3db
// BenchmarkNextIteration/sequential_10-12			1776			642349 ns/op
// BenchmarkNextIteration/parallel_10-12	  		2538			466186 ns/op
// BenchmarkNextIteration/series_10-12	    		1915			601583 ns/op

// BenchmarkNextIteration/sequential_100-12			621				1945963 ns/op
// BenchmarkNextIteration/parallel_100-12	 			1118			1042822 ns/op
// BenchmarkNextIteration/series_100-12	    		834				1451031 ns/op

// BenchmarkNextIteration/sequential_200-12			398				3002165 ns/op
// BenchmarkNextIteration/parallel_200-12	  		699				1613085 ns/op
// BenchmarkNextIteration/series_200-12	    		614				1969783 ns/op

// BenchmarkNextIteration/sequential_500-12			214				5522765 ns/op
// BenchmarkNextIteration/parallel_500-12	  		382				2904843 ns/op
// BenchmarkNextIteration/series_500-12	    		400				2996965 ns/op

// BenchmarkNextIteration/sequential_1000-12		129				9050684 ns/op
// BenchmarkNextIteration/parallel_1000-12	 		238				4775567 ns/op
// BenchmarkNextIteration/series_1000-12	   		289				4176052 ns/op

// BenchmarkNextIteration/sequential_2000-12		64				16190003 ns/op
// BenchmarkNextIteration/parallel_2000-12	 		136				8238382 ns/op
// BenchmarkNextIteration/series_2000-12	   		207				5744589 ns/op
func BenchmarkNextIteration(b *testing.B) {
	iterTypes := []iterType{
		stepSequential,
		stepParallel,
		seriesSequential,
	}

	for _, s := range []int{10, 100, 200, 500, 1000, 2000} {
		for _, t := range iterTypes {
			name := t.name(fmt.Sprintf("%d", s))
			b.Run(name, func(b *testing.B) {
				benchmarkNextIteration(b, s, t)
			})
		}

		// NB: this is for clearer groupings.
		fmt.Println()
	}
}
