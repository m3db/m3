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

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test/executor"
	"github.com/m3db/m3/src/query/ts"
	xsync "github.com/m3db/m3x/sync"

	"github.com/stretchr/testify/require"
)

/*
NB: Benchmark results (average across 10 runs)

1 Series,   100 Steps
Steps: 20000 runs, 83403.8 ns/op,  29250.8 allocs/op
Chan:  50000 runs, 25404.4 ns/op,  13686 allocs/op

100 Series, 100 Steps
Steps: 8000  runs, 203090 ns/op,   147566 allocs/op
Chan:  10000 runs, 142081 ns/op,   131903 allocs/op

10k Series, 100 Steps
Steps: 100 runs, 11473111.4 ns/op, 11334547.2 allocs/op
Chan:  100 runs, 12721183.8 ns/op, 11317352.9 allocs/op

100 Series, 10k Steps
Steps: 100 runs, 15840414.9 ns/op, 11696304.5 allocs/op
Chan:  100 runs, 10403478.3 ns/op, 10052040.3 allocs/op

10k Series, 10k Steps
Steps: 2.5 runs, 507341991 ns/op,  827119034.7 allocs/op
Chan:  2.8 runs, 499723487 ns/op,  823376332.7 allocs/op
*/

func benchNode(
	b *testing.B,
	workerPool xsync.PooledWorkerPool,
) (transform.OpNode, *executor.SinkNode) {
	op, err := NewAggregationOp(SumType, NodeParams{})
	require.NoError(b, err)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	require.NoError(b, err)

	asyncOp, valid := op.(baseOp)
	require.True(b, valid)
	return asyncOp.Node(c, transform.Options{}), sink
}

func benchAsyncNode(
	b *testing.B,
	workerPool xsync.PooledWorkerPool,
) (transform.OpNodeAsync, *executor.SinkNode) {
	op, err := NewAggregationOp(SumType, NodeParams{})
	require.NoError(b, err)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	require.NoError(b, err)

	asyncOp, valid := op.(baseOp)
	require.True(b, valid)
	return asyncOp.AsyncNode(c, workerPool), sink
}

func benchWorkerPools(b *testing.B) xsync.PooledWorkerPool {
	poolSize := 1024
	poolOpts := xsync.NewPooledWorkerPoolOptions().
		SetGrowOnDemand(true).
		SetNumShards(int64(poolSize))
	workerPool, err := xsync.NewPooledWorkerPool(poolSize, poolOpts)
	require.NoError(b, err)
	workerPool.Init()
	return workerPool
}

type mockBlock struct {
	seriesCount int
	stepCount   int
}

func (b *mockBlock) Unconsolidated() (block.UnconsolidatedBlock, error) { return nil, nil }
func (b *mockBlock) SeriesIter() (block.SeriesIter, error)              { return nil, nil }
func (b *mockBlock) WithMetadata(
	_ block.Metadata,
	_ []block.SeriesMeta,
) (block.Block, error) {
	return nil, nil
}
func (b *mockBlock) Close() error { return nil }
func (b *mockBlock) AsyncStepIter() (block.AsyncStepIter, error) {
	metas := make([]block.SeriesMeta, b.seriesCount)
	for i := 0; i < b.seriesCount; i++ {
		b := []byte(fmt.Sprint(i))
		metas[i] = block.SeriesMeta{
			Name: "a",
			Tags: models.NewTags(1, models.NewTagOptions()).AddTag(
				models.Tag{
					Name:  b,
					Value: b,
				},
			),
		}
	}

	return &mockAsyncIter{
		stepCount:   b.stepCount,
		seriesCount: b.seriesCount,
		metas:       metas,
	}, nil
}

type mockAsyncIter struct {
	seriesCount int
	stepCount   int
	step        int
	metas       []block.SeriesMeta
}

func (it *mockAsyncIter) SeriesMeta() []block.SeriesMeta {
	return it.metas
}

func (it *mockAsyncIter) Meta() block.Metadata {
	return block.Metadata{}
}

func (it *mockAsyncIter) StepCount() int {
	return it.stepCount
}

func (it *mockAsyncIter) Next() bool {
	it.step = it.step + 1
	return it.step < it.stepCount+1
}

func (it *mockAsyncIter) Current() <-chan block.Step {
	ch := make(chan block.Step)
	go func() {
		ch <- &mockStep{
			seriesCount: it.seriesCount,
		}
	}()
	return ch
}

func (it *mockAsyncIter) ValuesChannel() <-chan block.IndexedStep {
	ch := make(chan block.IndexedStep, it.stepCount)
	for i := 0; i < it.stepCount; i++ {
		ch <- block.IndexedStep{
			Idx: i,
			Step: &mockStep{
				seriesCount: it.seriesCount,
			},
		}
	}
	close(ch)
	return ch
}

func (it *mockAsyncIter) Err() error { return nil }
func (it *mockAsyncIter) Close()     {}

func (b *mockBlock) StepIter() (block.StepIter, error) {
	metas := make([]block.SeriesMeta, b.seriesCount)
	for i := 0; i < b.seriesCount; i++ {
		b := []byte(fmt.Sprint(i))
		metas[i] = block.SeriesMeta{
			Name: "a",
			Tags: models.NewTags(1, models.NewTagOptions()).AddTag(
				models.Tag{
					Name:  b,
					Value: b,
				},
			),
		}
	}

	return &mockIter{
		stepCount:   b.stepCount,
		seriesCount: b.seriesCount,
		metas:       metas,
	}, nil
}

type mockIter struct {
	seriesCount int
	stepCount   int
	step        int
	metas       []block.SeriesMeta
}

func (it *mockIter) SeriesMeta() []block.SeriesMeta {
	return it.metas
}

func (it *mockIter) Meta() block.Metadata {
	return block.Metadata{}
}

func (it *mockIter) StepCount() int {
	return it.stepCount
}

func (it *mockIter) Next() bool {
	it.step = it.step + 1
	return it.step < it.stepCount+1
}

func (it *mockIter) Current() (block.Step, error) {
	return &mockStep{
		seriesCount: it.seriesCount,
	}, nil
}

func (it *mockIter) Close() {}

type mockStep struct {
	seriesCount int
}

func (s *mockStep) Time() time.Time {
	return time.Time{}
}

func (s *mockStep) Values() []float64 {
	values := make([]float64, s.seriesCount)
	ts.Memset(values, float64(12))
	return values
}

var tests = []struct {
	name        string
	stepCount   int
	seriesCount int
}{
	{"1   series 100 steps", 1, 100},
	{"100 series 1 steps", 100, 1},
	{"100 series 100 steps", 100, 100},
	{"10k series 100 steps", 10000, 100},
	{"100 series 10k steps", 100, 10000},
}

func BenchmarkSteps(b *testing.B) {
	for _, t := range tests {
		b.Run(fmt.Sprint("Sync steps ", t.name), func(b *testing.B) {
			benchmarkSync(b, t.stepCount, t.seriesCount)
		})
	}
}

func BenchmarkAsyncSteps(b *testing.B) {
	for _, t := range tests {
		b.Run(fmt.Sprint("Async steps", t.name), func(b *testing.B) {
			benchmarkSteps(b, t.stepCount, t.seriesCount)
		})
	}
}

func BenchmarkAsyncChans(b *testing.B) {
	for _, t := range tests {
		b.Run(fmt.Sprint("Async chans", t.name), func(b *testing.B) {
			benchmarkValueChannel(b, t.stepCount, t.seriesCount)
		})
	}
}

func benchmarkSync(b *testing.B, seriesCount, stepCount int) {
	node, _ := benchNode(b, benchWorkerPools(b))
	bl := &mockBlock{
		seriesCount: seriesCount,
		stepCount:   stepCount,
	}

	for i := 0; i < b.N; i++ {
		node.Process(parser.NodeID(1), bl)
	}
}

func benchmarkValueChannel(b *testing.B, seriesCount, stepCount int) {
	node, _ := benchAsyncNode(b, benchWorkerPools(b))
	bl := &mockBlock{
		seriesCount: seriesCount,
		stepCount:   stepCount,
	}

	for i := 0; i < b.N; i++ {
		node.ProcessValueChannel(parser.NodeID(1), bl)
	}
}

func benchmarkSteps(b *testing.B, seriesCount, stepCount int) {
	node, _ := benchAsyncNode(b, benchWorkerPools(b))
	bl := &mockBlock{
		seriesCount: seriesCount,
		stepCount:   stepCount,
	}

	for i := 0; i < b.N; i++ {
		node.ProcessSteps(parser.NodeID(1), bl)
	}
}
