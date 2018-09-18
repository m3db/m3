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

func (p *processor) Process(dps ts.Datapoints) float64 {
	vals := dps.Values()
	sum := 0.0
	for _, n := range vals {
		sum += n
	}

	return sum
}

func dummyProcessor(_ baseOp, _ *transform.Controller, _ transform.Options) Processor {
	return &processor{}
}

func compareCacheState(t *testing.T, bNode *baseNode, bounds models.Bounds, state []bool, debugMsg string) {
	actualState := make([]bool, len(state))
	for i := range state {
		_, exists := bNode.cache.get(bounds.Next(i).Start)
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
		processorFn:  dummyProcessor,
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: boundStart,
			End:   boundStart.Add(time.Hour),
			Step:  time.Second,
		},
	})
	err := node.Process(parser.NodeID(0), block)
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

	err = node.Process(parser.NodeID(0), block)
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

	err = node.Process(parser.NodeID(0), block)
	require.NoError(t, err)
	bNode = node.(*baseNode)
	_, exists = bNode.cache.get(boundStart)
	assert.False(t, exists, "block not cached since no other blocks left to process")
}

func TestBaseWithB1B0(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	blocks := test.NewMultiUnconsolidatedBlocksFromValues(bounds, values, test.NoopMod, 2)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  dummyProcessor,
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: bounds.Start,
			End:   bounds.Next(1).End(),
			Step:  time.Second,
		},
	})
	bNode := node.(*baseNode)
	err := node.Process(parser.NodeID(0), blocks[1])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 0, "nothing processed yet")
	compareCacheState(t, bNode, bounds, []bool{false, true}, "B1 cached")

	err = node.Process(parser.NodeID(0), blocks[0])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 4, "output from both blocks")
	compareCacheState(t, bNode, bounds, []bool{false, false}, "everything removed from cache")
	blks, err := bNode.cache.multiGet(bounds, 2, false)
	require.NoError(t, err)
	assert.Len(t, blks, 0)
}

func TestBaseWithB0B1(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	blocks := test.NewMultiUnconsolidatedBlocksFromValues(bounds, values, test.NoopMod, 2)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  dummyProcessor,
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: bounds.Start,
			End:   bounds.Next(1).End(),
			Step:  time.Second,
		},
	})
	bNode := node.(*baseNode)
	err := node.Process(parser.NodeID(0), blocks[0])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 2, "B0 processed")
	compareCacheState(t, bNode, bounds, []bool{true, false}, "B0 cached for future")

	err = node.Process(parser.NodeID(0), blocks[1])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 4, "output from both blocks")
	compareCacheState(t, bNode, bounds, []bool{false, false}, "B0 removed from cache, B1 not cached")
	blks, err := bNode.cache.multiGet(bounds, 2, false)
	require.NoError(t, err)
	assert.Len(t, blks, 0)
}

func TestBaseWithB0B1B2(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	blocks := test.NewMultiUnconsolidatedBlocksFromValues(bounds, values, test.NoopMod, 3)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  dummyProcessor,
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: bounds.Start,
			End:   bounds.Next(2).End(),
			Step:  time.Second,
		},
	})
	bNode := node.(*baseNode)

	// B0 arrives
	err := node.Process(parser.NodeID(0), blocks[0])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 2, "B0 processed")
	compareCacheState(t, bNode, bounds, []bool{true, false, false}, "B0 cached for future")

	// B1 arrives
	err = node.Process(parser.NodeID(0), blocks[1])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 4, "output from B0, B1")
	compareCacheState(t, bNode, bounds, []bool{false, true, false}, "B0 removed from cache, B1 cached")

	// B2 arrives
	err = node.Process(parser.NodeID(0), blocks[2])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 6, "output from all blocks")
	compareCacheState(t, bNode, bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB0B2B1(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	blocks := test.NewMultiUnconsolidatedBlocksFromValues(bounds, values, test.NoopMod, 3)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  dummyProcessor,
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: bounds.Start,
			End:   bounds.Next(2).End(),
			Step:  time.Second,
		},
	})
	bNode := node.(*baseNode)

	// B0 arrives
	err := node.Process(parser.NodeID(0), blocks[0])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 2, "B0 processed")
	compareCacheState(t, bNode, bounds, []bool{true, false, false}, "B0 cached for future")

	// B2 arrives
	err = node.Process(parser.NodeID(0), blocks[2])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 2, "Only B0 processed")
	compareCacheState(t, bNode, bounds, []bool{true, false, true}, "B0, B2 cached")

	// B1 arrives
	err = node.Process(parser.NodeID(0), blocks[1])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 6, "output from all blocks")
	compareCacheState(t, bNode, bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB1B0B2(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	blocks := test.NewMultiUnconsolidatedBlocksFromValues(bounds, values, test.NoopMod, 3)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  dummyProcessor,
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: bounds.Start,
			End:   bounds.Next(2).End(),
			Step:  time.Second,
		},
	})
	bNode := node.(*baseNode)

	// B1 arrives
	err := node.Process(parser.NodeID(0), blocks[1])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 0, "Nothing processed")
	compareCacheState(t, bNode, bounds, []bool{false, true, false}, "B1 cached for future")

	// B0 arrives
	err = node.Process(parser.NodeID(0), blocks[0])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 4, "B0, B1 processed")
	compareCacheState(t, bNode, bounds, []bool{false, true, false}, "B1 still cached, B0 not cached")

	// B2 arrives
	err = node.Process(parser.NodeID(0), blocks[2])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 6, "output from all blocks")
	compareCacheState(t, bNode, bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB1B2B0(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	blocks := test.NewMultiUnconsolidatedBlocksFromValues(bounds, values, test.NoopMod, 3)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  dummyProcessor,
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: bounds.Start,
			End:   bounds.Next(2).End(),
			Step:  time.Second,
		},
	})
	bNode := node.(*baseNode)

	// B1 arrives
	err := node.Process(parser.NodeID(0), blocks[1])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 0, "Nothing processed")
	compareCacheState(t, bNode, bounds, []bool{false, true, false}, "B1 cached for future")

	// B2 arrives
	err = node.Process(parser.NodeID(0), blocks[2])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 2, "B1 processed")
	compareCacheState(t, bNode, bounds, []bool{false, true, false}, "B1 still cached, B2 not cached")

	// B0 arrives
	err = node.Process(parser.NodeID(0), blocks[0])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 6, "output from all blocks")
	compareCacheState(t, bNode, bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB2B0B1(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	blocks := test.NewMultiUnconsolidatedBlocksFromValues(bounds, values, test.NoopMod, 3)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  dummyProcessor,
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: bounds.Start,
			End:   bounds.Next(2).End(),
			Step:  time.Second,
		},
	})
	bNode := node.(*baseNode)

	// B2 arrives
	err := node.Process(parser.NodeID(0), blocks[2])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 0, "Nothing processed")
	compareCacheState(t, bNode, bounds, []bool{false, false, true}, "B2 cached for future")

	// B0 arrives
	err = node.Process(parser.NodeID(0), blocks[0])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 2, "B0 processed")
	compareCacheState(t, bNode, bounds, []bool{true, false, true}, "B0, B2 cached")

	// B1 arrives
	err = node.Process(parser.NodeID(0), blocks[1])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 6, "output from all blocks")
	compareCacheState(t, bNode, bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithB2B1B0(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	blocks := test.NewMultiUnconsolidatedBlocksFromValues(bounds, values, test.NoopMod, 3)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  dummyProcessor,
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: bounds.Start,
			End:   bounds.Next(2).End(),
			Step:  time.Second,
		},
	})
	bNode := node.(*baseNode)

	// B2 arrives
	err := node.Process(parser.NodeID(0), blocks[2])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 0, "Nothing processed")
	compareCacheState(t, bNode, bounds, []bool{false, false, true}, "B2 cached for future")

	// B1 arrives
	err = node.Process(parser.NodeID(0), blocks[1])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 2, "B0 processed")
	compareCacheState(t, bNode, bounds, []bool{false, true, false}, "B1 cached, B2 removed")

	// B0 arrives
	err = node.Process(parser.NodeID(0), blocks[0])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 6, "output from all blocks")
	compareCacheState(t, bNode, bounds, []bool{false, false, false}, "nothing cached")
}

func TestBaseWithSize3B0B1B2B3B4(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	blocks := test.NewMultiUnconsolidatedBlocksFromValues(bounds, values, test.NoopMod, 5)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     15 * time.Minute,
		processorFn:  dummyProcessor,
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: bounds.Start,
			End:   bounds.Next(4).End(),
			Step:  time.Second,
		},
	})
	bNode := node.(*baseNode)

	// B0 arrives
	err := node.Process(parser.NodeID(0), blocks[0])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 2, "B0 processed")
	compareCacheState(t, bNode, bounds, []bool{true, false, false, false, false}, "B0 cached for future")

	// B1 arrives
	err = node.Process(parser.NodeID(0), blocks[1])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 4, "B0, B1 processed")
	compareCacheState(t, bNode, bounds, []bool{true, true, false, false, false}, "B0, B1 cached")

	// B2 arrives
	err = node.Process(parser.NodeID(0), blocks[2])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 6, "B0, B1, B2 processed")
	compareCacheState(t, bNode, bounds, []bool{true, true, true, false, false}, "B0, B1, B2 cached")

	// B3 arrives
	err = node.Process(parser.NodeID(0), blocks[3])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 8, "B0, B1, B2, B3 processed")
	compareCacheState(t, bNode, bounds, []bool{false, true, true, true, false}, "B0 removed, B1, B2, B3 cached")

	// B4 arrives
	err = node.Process(parser.NodeID(0), blocks[4])
	require.NoError(t, err)
	assert.Len(t, sink.Values, 10, "all 5 blocks processed")
	compareCacheState(t, bNode, bounds, []bool{false, false, false, false, false}, "nothing cached")
}

func TestSingleProcessRequest(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	boundStart := bounds.Start
	b := test.NewUnconsolidatedBlockFromDatapoints(bounds, values)
	block2, _ := b.Unconsolidated()
	values = [][]float64{{10, 11, 12, 13, 14}, {15, 16, 17, 18, 19}}

	block1Bounds := models.Bounds{
		Start:    bounds.Start.Add(-1 * bounds.Duration),
		Duration: bounds.Duration,
		StepSize: bounds.StepSize,
	}

	b = test.NewUnconsolidatedBlockFromDatapoints(block1Bounds, values)
	block1, _ := b.Unconsolidated()

	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  dummyProcessor,
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: boundStart.Add(-2 * bounds.Duration),
			End:   bounds.End(),
			Step:  time.Second,
		},
	})
	bNode := node.(*baseNode)
	err := bNode.processSingleRequest(processRequest{blk: block2, bounds: bounds, deps: []block.UnconsolidatedBlock{block1}})
	assert.NoError(t, err)
	assert.Len(t, sink.Values, 2, "block processed")
	// Current Block:     0  1  2  3  4  5
	// Previous Block:   10 11 12 13 14 15
	// i = 0; prev values [11, 12, 13, 14, 15], current values [0], sum = 50
	// i = 1; prev values [12, 13, 14, 15], current values [0, 1], sum = 40
	assert.Equal(t, sink.Values[0], []float64{50, 40, 30, 20, 10}, "first series is 10 - 14 which sums to 60, the current block first series is 0-4 which sums to 10, we need 5 values per aggregation")
	assert.Equal(t, sink.Values[1], []float64{75, 65, 55, 45, 35}, "second series is 15 - 19 which sums to 85 and second series is 5-9 which sums to 35")
}
