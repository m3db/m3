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

package aggregation

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/ts/m3db"
	xsync "github.com/m3db/m3x/sync"

	"github.com/stretchr/testify/require"
)

func buildSeriesIterators(
	b *testing.B,
	seriesCount int,
) []encoding.SeriesIterator {
	iters := make([]encoding.SeriesIterator, seriesCount)
	for i := range iters {
		it, err := test.BuildTestSeriesIterator()
		require.NoError(b, err)
		iters[i] = it
	}

	return iters
}

func buildRealBlock(b *testing.B, seriesCount int) block.Block {
	iterators := buildSeriesIterators(b, seriesCount)
	require.True(b, len(iterators) > 0)
	it := iterators[0]
	bounds := models.Bounds{
		Start:    it.Start(),
		Duration: it.End().Sub(it.Start()),
		StepSize: time.Minute,
	}

	bl, err := m3db.NewEncodedBlock(
		iterators,
		bounds,
		models.NewTagOptions(),
		true,
	)
	require.NoError(b, err)
	return bl
}

func buildRealAsyncBlock(
	b *testing.B,
	workerPools xsync.PooledWorkerPool,
	seriesCount int,
) block.AsyncBlock {
	iterators := buildSeriesIterators(b, seriesCount)
	require.True(b, len(iterators) > 0)
	it := iterators[0]
	bounds := models.Bounds{
		Start:    it.Start(),
		Duration: it.End().Sub(it.Start()),
		StepSize: time.Minute,
	}

	bl, err := m3db.NewEncodedAsyncBlock(
		iterators,
		bounds,
		models.NewTagOptions(),
		workerPools,
		true,
	)
	require.NoError(b, err)
	return bl
}

func buildExpectedMultiple(seriesCount int) [][]float64 {
	expected := make([]float64, 0, 57)
	for i := 3; i <= 30; i++ {
		expected = append(expected, float64(i*seriesCount))
	}

	for i := 101; i <= 130; i++ {
		expected = append(expected, float64(i*seriesCount))
	}

	return [][]float64{expected}
}

var realTests = []struct {
	name        string
	seriesCount int
}{
	{"1   series", 1},
	{"100 series", 100},
	{"1k  series", 1000},
	{"10k series", 10000},
}

func BenchmarkRealSync(b *testing.B) {
	for _, t := range realTests {
		b.Run(fmt.Sprint("Sync steps ", t.name), func(b *testing.B) {
			benchmarkRealSync(b, t.seriesCount)
		})
	}
}

func BenchmarkRealSteps(b *testing.B) {
	for _, t := range realTests {
		b.Run(fmt.Sprint("Async steps", t.name), func(b *testing.B) {
			benchmarkRealSteps(b, t.seriesCount)
		})
	}
}

func BenchmarkRealValueChannel(b *testing.B) {
	for _, t := range realTests {
		b.Run(fmt.Sprint("Async chans", t.name), func(b *testing.B) {
			benchmarkRealValueChannel(b, t.seriesCount)
		})
	}
}

func benchmarkRealSync(b *testing.B, seriesCount int) {
	node, _ := benchNode(b, benchWorkerPools(b))
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		bl := buildRealBlock(b, seriesCount)
		b.StartTimer()
		node.Process(parser.NodeID(1), bl)
	}
}

func benchmarkRealValueChannel(b *testing.B, seriesCount int) {
	node, _ := benchAsyncNode(b, benchWorkerPools(b))
	pools := benchWorkerPools(b)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		bl := buildRealAsyncBlock(b, pools, seriesCount)
		b.StartTimer()
		node.ProcessValueChannel(parser.NodeID(1), bl)
	}
}

func benchmarkRealSteps(b *testing.B, seriesCount int) {
	node, _ := benchAsyncNode(b, benchWorkerPools(b))
	pools := benchWorkerPools(b)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		bl := buildRealAsyncBlock(b, pools, seriesCount)
		b.StartTimer()
		node.ProcessSteps(parser.NodeID(1), bl)
	}
}
