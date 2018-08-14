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
	"math"
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

// B1 has NaN in first series, first position
func TestCountWithThreeBlocks(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	boundStart := bounds.Start
	block3 := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))

	baseOp, err := NewCountOp([]interface{}{5 * time.Minute})
	require.NoError(t, err)
	node := baseOp.Node(c, transform.Options{
		TimeSpec: transform.TimeSpec{
			Start: boundStart.Add(-2 * bounds.Duration),
			End:   bounds.End(),
			Step:  time.Second,
		},
	})
	bNode := node.(*baseNode)
	err = node.Process(parser.NodeID(0), block3)
	require.NoError(t, err)
	assert.Len(t, sink.Values, 0, "nothing processed yet")
	_, exists := bNode.cache.get(boundStart)
	assert.True(t, exists, "block cached for future")

	original := values[0][0]
	values[0][0] = math.NaN()
	block1 := test.NewBlockFromValues(block.Bounds{
		Start:    bounds.Start.Add(-2 * bounds.Duration),
		Duration: bounds.Duration,
		StepSize: bounds.StepSize,
	}, values)

	values[0][0] = original
	err = node.Process(parser.NodeID(0), block1)
	require.NoError(t, err)
	assert.Len(t, sink.Values, 2, "output from first block only")
	test.EqualsWithNans(t, []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 4}, sink.Values[0])
	test.EqualsWithNans(t, []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 5}, sink.Values[1])
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
	test.EqualsWithNans(t, []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 4}, sink.Values[0])
	test.EqualsWithNans(t, []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 5}, sink.Values[1])
	expected := []float64{5, 5, 5, 5, 5}
	test.EqualsWithNans(t, expected, sink.Values[2])
	test.EqualsWithNans(t, expected, sink.Values[3])
	cachedBlocks, err := bNode.cache.multiGet(bounds.Previous(2), 3, false)
	require.NoError(t, err)
	assert.Nil(t, cachedBlocks[0], "block removed from cache")
	assert.Nil(t, cachedBlocks[1], "block not cached")
	assert.Nil(t, cachedBlocks[2], "block removed from cache")
}
