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

package temporal

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"
	"github.com/m3db/m3/src/query/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type processor struct {
}

func (p processor) Init(op baseOp, controller *transform.Controller, opts transform.Options) Processor {
	return &p
}

func (p *processor) Process(dps ts.Datapoints, _ time.Time) float64 {
	vals := dps.Values()
	sum := 0.0
	for _, n := range vals {
		sum += n
	}

	return sum
}

func compareCacheState(t *testing.T, node *baseNode, bounds models.Bounds, state []bool, debugMsg string) {
	actualState := make([]bool, len(state))
	for i := range state {
		_, exists := node.cache.get(bounds.Next(i).Start)
		actualState[i] = exists
	}

	assert.Equal(t, actualState, state, debugMsg)
}

func TestBaseWithB0(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	boundStart := bounds.Start
	block := test.NewUnconsolidatedBlockFromDatapoints(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  processor{},
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: boundStart,
			End:   boundStart.Add(time.Hour),
			Step:  time.Second,
		},
	})
	err := node.Process(models.NoopQueryContext(), parser.NodeID(0), block)
	require.NoError(t, err)
	assert.Len(t, sink.Values, 2)
	require.IsType(t, node, &baseNode{})
	bNode := node.(*baseNode)
	_, exists := bNode.cache.get(boundStart)
	assert.True(t, exists, "block cached since the query end is larger")

	c, _ = executor.NewControllerWithSink(parser.NodeID(1))
	node = baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: boundStart,
			End:   bounds.End(),
			Step:  time.Second,
		},
	})

	err = node.Process(models.NoopQueryContext(), parser.NodeID(0), block)
	require.NoError(t, err)
	bNode = node.(*baseNode)
	_, exists = bNode.cache.get(boundStart)
	assert.False(t, exists, "block not cached since no other blocks left to process")

	c, _ = executor.NewControllerWithSink(parser.NodeID(1))
	node = baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: boundStart.Add(bounds.StepSize),
			End:   bounds.End().Add(-1 * bounds.StepSize),
			Step:  time.Second,
		},
	})

	err = node.Process(models.NoopQueryContext(), parser.NodeID(0), block)
	require.NoError(t, err)
	bNode = node.(*baseNode)
	_, exists = bNode.cache.get(boundStart)
	assert.False(t, exists, "block not cached since no other blocks left to process")
}

func TestBaseWithB1B0(t *testing.T) {
	tc := setup(2, 5*time.Minute, 1)

	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 0, "nothing processed yet")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true}, "B1 cached")

	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 4, "output from both blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false}, "everything removed from cache")
	blks, err := tc.Node.cache.multiGet(tc.Bounds, 2, false)
	require.NoError(t, err)
	assert.Len(t, blks, 0)
}

func TestBaseWithB0B1(t *testing.T) {
	tc := setup(2, 5*time.Minute, 1)

	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, false}, "B0 cached for future")

	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 4, "output from both blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false}, "B0 removed from cache, B1 not cached")
	blks, err := tc.Node.cache.multiGet(tc.Bounds, 2, false)
	require.NoError(t, err)
	assert.Len(t, blks, 0)
}

func TestBaseWithB0B1B2(t *testing.T) {
	tc := setup(3, 5*time.Minute, 2)

	// B0 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, false, false}, "B0 cached for future")

	// B1 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 4, "output from B0, B1")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, false}, "B0 removed from cache, B1 cached")

	// B2 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "output from all blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB0B2B1(t *testing.T) {
	tc := setup(3, 5*time.Minute, 2)

	// B0 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, false, false}, "B0 cached for future")

	// B2 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "Only B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, false, true}, "B0, B2 cached")

	// B1 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "output from all blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB1B0B2(t *testing.T) {
	tc := setup(3, 5*time.Minute, 2)

	// B1 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 0, "Nothing processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, false}, "B1 cached for future")

	// B0 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 4, "B0, B1 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, false}, "B1 still cached, B0 not cached")

	// B2 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "output from all blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB1B2B0(t *testing.T) {
	tc := setup(3, 5*time.Minute, 2)

	// B1 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 0, "Nothing processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, false}, "B1 cached for future")

	// B2 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B1 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, false}, "B1 still cached, B2 not cached")

	// B0 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "output from all blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB2B0B1(t *testing.T) {
	tc := setup(3, 5*time.Minute, 2)

	// B2 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 0, "Nothing processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, true}, "B2 cached for future")

	// B0 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, false, true}, "B0, B2 cached")

	// B1 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "output from all blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB2B1B0(t *testing.T) {
	tc := setup(3, 5*time.Minute, 2)

	// B2 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 0, "Nothing processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, true}, "B2 cached for future")

	// B1 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, false}, "B1 cached, B2 removed")

	// B0 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "output from all blocks")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithSize3B0B1B2B3B4(t *testing.T) {
	tc := setup(5, 15*time.Minute, 4)

	// B0 arrives
	err := tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[0])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 2, "B0 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, false, false, false, false}, "B0 cached for future")

	// B1 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[1])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 4, "B0, B1 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, true, false, false, false}, "B0, B1 cached")

	// B2 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[2])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 6, "B0, B1, B2 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{true, true, true, false, false}, "B0, B1, B2 cached")

	// B3 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[3])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 8, "B0, B1, B2, B3 processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, true, true, true, false}, "B0 removed, B1, B2, B3 cached")

	// B4 arrives
	err = tc.Node.Process(models.NoopQueryContext(), parser.NodeID(0), tc.Blocks[4])
	require.NoError(t, err)
	assert.Len(t, tc.Sink.Values, 10, "all 5 blocks processed")
	compareCacheState(t, tc.Node, tc.Bounds, []bool{false, false, false, false, false}, "nothing cached")
}

type testContext struct {
	Bounds models.Bounds
	Blocks []block.Block
	Sink   *executor.SinkNode
	Node   *baseNode
}

func setup(numBlocks int, duration time.Duration, nextBound int) *testContext {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	blocks := test.NewMultiUnconsolidatedBlocksFromValues(bounds, values, test.NoopMod, numBlocks)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     duration,
		processorFn:  processor{},
	}
	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: bounds.Start,
			End:   bounds.Next(nextBound).End(),
			Step:  time.Second,
		},
	})
	return &testContext{
		Bounds: bounds,
		Blocks: blocks,
		Sink:   sink,
		Node:   node.(*baseNode),
	}
}

func TestSingleProcessRequest(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	boundStart := bounds.Start

	seriesMetas := []block.SeriesMeta{{
		Name: []byte("s1"),
		Tags: models.EmptyTags().AddTags([]models.Tag{{
			Name:  []byte("t1"),
			Value: []byte("v1"),
		}})}, {
		Name: []byte("s2"),
		Tags: models.EmptyTags().AddTags([]models.Tag{{
			Name:  []byte("t1"),
			Value: []byte("v2"),
		}}),
	}}

	b := test.NewUnconsolidatedBlockFromDatapointsWithMeta(bounds, seriesMetas, values)
	block2, _ := b.Unconsolidated()
	values = [][]float64{{10, 11, 12, 13, 14}, {15, 16, 17, 18, 19}}

	block1Bounds := models.Bounds{
		Start:    bounds.Start.Add(-1 * bounds.Duration),
		Duration: bounds.Duration,
		StepSize: bounds.StepSize,
	}

	b = test.NewUnconsolidatedBlockFromDatapointsWithMeta(block1Bounds, seriesMetas, values)
	block1, _ := b.Unconsolidated()

	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  processor{},
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: boundStart.Add(-2 * bounds.Duration),
			End:   bounds.End(),
			Step:  time.Second,
		},
	})
	bNode := node.(*baseNode)
	request := processRequest{
		blk:      block2,
		bounds:   bounds,
		deps:     []block.UnconsolidatedBlock{block1},
		queryCtx: models.NoopQueryContext(),
	}
	bl, err := bNode.processSingleRequest(request)
	require.NoError(t, err)

	bNode.propagateNextBlocks([]processRequest{request}, []block.Block{bl}, 1)
	assert.Len(t, sink.Values, 2, "block processed")
	// Current Block:     0  1  2  3  4  5
	// Previous Block:   10 11 12 13 14 15
	// i = 0; prev values [11, 12, 13, 14, 15], current values [0], sum = 50
	// i = 1; prev values [12, 13, 14, 15], current values [0, 1], sum = 40
	assert.Equal(t, sink.Values[0], []float64{50, 40, 30, 20, 10},
		"first series is 10 - 14 which sums to 60, the current block first series is 0-4 which sums to 10, we need 5 values per aggregation")
	assert.Equal(t, sink.Values[1], []float64{75, 65, 55, 45, 35},
		"second series is 15 - 19 which sums to 85 and second series is 5-9 which sums to 35")

	// processSingleRequest renames the series to use their ids; reflect this in our expectation.
	expectedSeriesMetas := make([]block.SeriesMeta, len(seriesMetas))
	require.Equal(t, len(expectedSeriesMetas), copy(expectedSeriesMetas, seriesMetas))
	expectedSeriesMetas[0].Name = []byte("t1=v1,")
	expectedSeriesMetas[1].Name = []byte("t1=v2,")

	assert.Equal(t, expectedSeriesMetas, sink.Metas, "Process should pass along series meta, renaming to the ID")
}
