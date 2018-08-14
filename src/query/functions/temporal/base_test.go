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

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/executor/transform"
	"github.com/m3db/m3db/src/coordinator/parser"
	"github.com/m3db/m3db/src/coordinator/test"
	"github.com/m3db/m3db/src/coordinator/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type processor struct {
}

func (p *processor) Process([]float64) float64 {
	return 0
}

func dummyProcessor(op baseOp, controller *transform.Controller) Processor {
	return &processor{}
}

func TestBaseWithStartBlock(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	boundStart := bounds.Start
	block := test.NewBlockFromValues(bounds, values)
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

	c, sink = executor.NewControllerWithSink(parser.NodeID(1))
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

	c, sink = executor.NewControllerWithSink(parser.NodeID(1))
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

func TestBaseWithSecondBlock(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	boundStart := bounds.Start
	block1 := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	baseOp := baseOp{
		operatorType: "dummy",
		duration:     5 * time.Minute,
		processorFn:  dummyProcessor,
	}

	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: boundStart.Add(-1 * bounds.Duration),
			End:   bounds.End(),
			Step:  time.Second,
		},
	})
	bNode := node.(*baseNode)
	err := node.Process(parser.NodeID(0), block1)
	require.NoError(t, err)
	assert.Len(t, sink.Values, 0, "nothing processed yet")
	_, exists := bNode.cache.get(boundStart)
	assert.True(t, exists, "block cached for future")

	block2 := test.NewBlockFromValues(block.Bounds{
		Start:    bounds.Start.Add(-1 * bounds.Duration),
		Duration: bounds.Duration,
		StepSize: bounds.StepSize,
	}, values)

	err = node.Process(parser.NodeID(0), block2)
	require.NoError(t, err)
	assert.Len(t, sink.Values, 4, "output from both blocks")
	_, exists = bNode.cache.get(bounds.Previous(1).Start)
	assert.False(t, exists, "block removed from cache")
	_, exists = bNode.cache.get(bounds.Start)
	assert.False(t, exists, "block not cached")
	blks, err := bNode.cache.multiGet(bounds.Previous(1), 2, false)
	require.NoError(t, err)
	assert.Len(t, blks, 0)
}

// B3 [0,1] -> B1 [-2, -1] -> B2 [-1,0]
func TestBaseWithThreeBlocks(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	boundStart := bounds.Start
	block3 := test.NewBlockFromValues(bounds, values)
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
	err := node.Process(parser.NodeID(0), block3)
	require.NoError(t, err)
	assert.Len(t, sink.Values, 0, "nothing processed yet")
	_, exists := bNode.cache.get(boundStart)
	assert.True(t, exists, "block cached for future")

	block1 := test.NewBlockFromValues(block.Bounds{
		Start:    bounds.Start.Add(-2 * bounds.Duration),
		Duration: bounds.Duration,
		StepSize: bounds.StepSize,
	}, values)

	err = node.Process(parser.NodeID(0), block1)
	require.NoError(t, err)
	assert.Len(t, sink.Values, 2, "output from first block only")
	_, exists = bNode.cache.get(boundStart)
	assert.True(t, exists, "block still cached")
	_, exists = bNode.cache.get(boundStart.Add(-1 * bounds.Duration))
	assert.False(t, exists, "block cached")

	block2 := test.NewBlockFromValues(block.Bounds{
		Start:    bounds.Start.Add(-1 * bounds.Duration),
		Duration: bounds.Duration,
		StepSize: bounds.StepSize,
	}, values)

	err = node.Process(parser.NodeID(0), block2)
	require.NoError(t, err)
	assert.Len(t, sink.Values, 6, "output from all 3 blocks")
	_, exists = bNode.cache.get(bounds.Previous(2).Start)
	assert.False(t, exists, "block removed from cache")
	_, exists = bNode.cache.get(bounds.Previous(1).Start)
	assert.False(t, exists, "block not cached")
	_, exists = bNode.cache.get(bounds.Start)
	assert.False(t, exists, "block removed from cache")
	blks, err := bNode.cache.multiGet(bounds.Previous(2), 3, false)
	require.NoError(t, err)
	assert.Len(t, blks, 0)

}
