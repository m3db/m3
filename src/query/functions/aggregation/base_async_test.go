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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"
	"github.com/m3db/m3/src/query/ts/m3db"
	xsync "github.com/m3db/m3x/sync"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildNode(
	t *testing.T,
	workerPool xsync.PooledWorkerPool,
) (transform.OpNodeAsync, *executor.SinkNode) {
	op, err := NewAggregationOp(SumType, NodeParams{})
	require.NoError(t, err)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	require.NoError(t, err)

	asyncOp, valid := op.(baseOp)
	require.True(t, valid)
	return asyncOp.AsyncNode(c, workerPool), sink
}

func applyToNode(t *testing.T) (
	transform.OpNodeAsync,
	block.AsyncBlock,
	*executor.SinkNode,
) {
	poolSize := 16
	poolOpts := xsync.NewPooledWorkerPoolOptions().
		SetGrowOnDemand(true).
		SetNumShards(int64(poolSize))
	workerPool, err := xsync.NewPooledWorkerPool(poolSize, poolOpts)
	workerPool.Init()

	require.NoError(t, err)

	node, sink := buildNode(t, workerPool)
	it, err := test.BuildTestSeriesIterator()
	require.NoError(t, err)

	bounds := models.Bounds{
		Start:    it.Start(),
		Duration: it.End().Sub(it.Start()),
		StepSize: time.Minute,
	}

	bl, err := m3db.NewEncodedAsyncBlock(
		[]encoding.SeriesIterator{it},
		bounds,
		models.NewTagOptions(),
		workerPool,
		true,
	)
	require.NoError(t, err)
	return node, bl, sink
}

func buildExpected() [][]float64 {
	expected := make([]float64, 0, 57)
	for i := 3; i <= 30; i++ {
		expected = append(expected, float64(i))
	}

	for i := 101; i <= 130; i++ {
		expected = append(expected, float64(i))
	}

	return [][]float64{expected}
}

func TestAsync(t *testing.T) {
	node, bl, sink := applyToNode(t)
	node.ProcessValueChannel(parser.NodeID(1), bl)
	assert.Equal(t, buildExpected(), sink.Values)
}

func TestAsyncSteps(t *testing.T) {
	node, bl, sink := applyToNode(t)
	node.ProcessSteps(parser.NodeID(1), bl)
	assert.Equal(t, buildExpected(), sink.Values)
}
