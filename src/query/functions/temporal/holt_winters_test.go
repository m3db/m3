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

	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var holtWintersTestCases = []testCase{
	{
		name: "holt_winters",
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 4},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 9},
		},
		afterAllBlocks: [][]float64{
			{4, 3.64, 3.1824, -4.8224, 4},
			{9, 8.64, 8.1824, 0.1776, 9},
		},
	},
}

var holtWintersTestCasesNaNs = []testCase{
	{
		name: "holt_winters all NaNs",
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		},
		afterAllBlocks: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		},
	},
}

func TestHoltWintersBlocks(t *testing.T) {
	v := [][]float64{
		{0, 1, 2, 3, 4},
		{5, 6, 7, 8, 9},
	}
	testHoltWinters(t, holtWintersTestCases, v)

	v = [][]float64{
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
	}
	testHoltWinters(t, holtWintersTestCasesNaNs, v)
}

// B1 has NaN in first series, first position
func testHoltWinters(t *testing.T, testCases []testCase, vals [][]float64) {
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			values, bounds := test.GenerateValuesAndBounds(vals, nil)
			boundStart := bounds.Start
			block3 := test.NewUnconsolidatedBlockFromDatapoints(bounds, values)
			c, sink := executor.NewControllerWithSink(parser.NodeID(1))

			baseOp, err := NewHoltWintersOp([]interface{}{5 * time.Minute, 0.2, 0.7})
			require.NoError(t, err)
			node := baseOp.Node(c, transform.Options{
				TimeSpec: transform.TimeSpec{
					Start: boundStart.Add(-2 * bounds.Duration),
					End:   bounds.End(),
					Step:  time.Second,
				},
			})
			bNode := node.(*baseNode)
			err = node.Process(models.NoopQueryContext(), parser.NodeID(0), block3)
			require.NoError(t, err)
			assert.Len(t, sink.Values, 0, "nothing processed yet")
			b, exists := bNode.cache.get(boundStart)
			assert.True(t, exists, "block cached for future")
			_, err = b.StepIter()
			assert.NoError(t, err)

			original := values[0][0]
			values[0][0] = math.NaN()
			block1 := test.NewUnconsolidatedBlockFromDatapoints(models.Bounds{
				Start:    bounds.Start.Add(-2 * bounds.Duration),
				Duration: bounds.Duration,
				StepSize: bounds.StepSize,
			}, values)

			values[0][0] = original
			err = node.Process(models.NoopQueryContext(), parser.NodeID(0), block1)
			require.NoError(t, err)
			assert.Len(t, sink.Values, 2, "output from first block only")
			test.EqualsWithNansWithDelta(t, tt.afterBlockOne[0], sink.Values[0], 0.0001)
			test.EqualsWithNansWithDelta(t, tt.afterBlockOne[1], sink.Values[1], 0.0001)
			_, exists = bNode.cache.get(boundStart)
			assert.True(t, exists, "block still cached")
			_, exists = bNode.cache.get(boundStart.Add(-1 * bounds.Duration))
			assert.False(t, exists, "block cached")

			block2 := test.NewUnconsolidatedBlockFromDatapoints(models.Bounds{
				Start:    bounds.Start.Add(-1 * bounds.Duration),
				Duration: bounds.Duration,
				StepSize: bounds.StepSize,
			}, values)

			err = node.Process(models.NoopQueryContext(), parser.NodeID(0), block2)
			require.NoError(t, err)
			assert.Len(t, sink.Values, 6, "output from all 3 blocks")
			test.EqualsWithNansWithDelta(t, tt.afterBlockOne[0], sink.Values[0], 0.0001)
			test.EqualsWithNansWithDelta(t, tt.afterBlockOne[1], sink.Values[1], 0.0001)
			expectedOne := tt.afterAllBlocks[0]
			expectedTwo := tt.afterAllBlocks[1]
			test.EqualsWithNansWithDelta(t, expectedOne, sink.Values[2], 0.0001)
			test.EqualsWithNansWithDelta(t, expectedTwo, sink.Values[3], 0.0001)
			_, exists = bNode.cache.get(bounds.Previous(2).Start)
			assert.False(t, exists, "block removed from cache")
			_, exists = bNode.cache.get(bounds.Previous(1).Start)
			assert.False(t, exists, "block not cached")
			_, exists = bNode.cache.get(bounds.Start)
			assert.False(t, exists, "block removed from cache")
			blks, err := bNode.cache.multiGet(bounds.Previous(2), 3, false)
			require.NoError(t, err)
			assert.Len(t, blks, 0)
		})
	}
}

func TestHoltWinters(t *testing.T) {
	holtWintersFn := makeHoltWintersFn(0.2, 0.6)
	val := holtWintersFn([]float64{math.NaN(), 1, math.NaN(), 5, 10, 15, math.NaN(), math.NaN()})

	test.EqualsWithNansWithDelta(t, 13.6559, val, 0.0001)
}
