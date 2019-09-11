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
	"github.com/m3db/m3/src/query/test/transformtest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testRateCase struct {
	name           string
	vals           [][]float64
	opType         string
	afterBlockOne  [][]float64
	afterAllBlocks [][]float64
}

var testRateCases = []testRateCase{
	{
		name:   "irate",
		opType: IRateType,
		vals: [][]float64{
			{678758, 680986, 683214, 685442, 687670},
			{1987036, 1988988, 1990940, 1992892, 1994844},
		},
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), 37.1333, 37.1333, 37.1333},
			{math.NaN(), 32.5333, 32.5333, 32.5333, 32.5333},
		},
		afterAllBlocks: [][]float64{
			{11312.6333, 37.1333, 37.1333, 37.1333, 37.1333},
			{33117.2666, 32.5333, 32.5333, 32.5333, 32.5333},
		},
	},
	{
		name:   "irate with some NaNs",
		opType: IRateType,
		vals: [][]float64{
			{1987036, 1988988, 1990940, math.NaN(), 1994844},
			{1987036, 1988988, 1990940, math.NaN(), math.NaN()},
		},
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), 32.5333, 32.5333, 32.5333},
			{math.NaN(), 32.5333, 32.5333, 32.5333, 32.5333},
		},
		afterAllBlocks: [][]float64{
			{33117.2666, 32.5333, 32.5333, 32.5333, 32.5333},
			{11039.0888, 32.5333, 32.5333, 32.5333, 32.5333},
		},
	},
	{
		name:   "irate with all NaNs",
		opType: IRateType,
		vals: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		},
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		},
		afterAllBlocks: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		},
	},
	{
		name:   "rate",
		opType: RateType,
		vals: [][]float64{
			{61108224, 61943808, 61943808, 61943808, 62205952},
			{1987036, 1988988, 1990940, 1992892, 1994844},
		},
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), 0, 0, 1165.0844},
			{math.NaN(), 13.01333, 19.52, 26.0266, 32.5333},
		},
		afterAllBlocks: [][]float64{
			{255709.8666, 259191.4666, 259191.4666, 258099.2, 4573.8666},
			{8303.7166, 8303.7166, 8303.7166, 8303.7166, 32.5333},
		},
	},
	{
		name:   "rate with some NaNs",
		opType: RateType,
		vals: [][]float64{
			{61108224, 61943808, 61943808, 62205952, math.NaN()},
			{1987036, 1988988, 1990940, math.NaN(), 1994844},
		},
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), 0, 1310.72, 1310.72},
			{math.NaN(), 13.01333, 19.52, 19.52, 32.5333},
		},
		afterAllBlocks: [][]float64{
			{255709.8666, 259191.4666, 258099.2, 4878.7911, 4878.7911},
			{8303.7166, 8303.7166, 8848.6222, 8848.6222, 32.5333},
		},
	},
	{
		name:   "rate with all NaNs",
		opType: RateType,
		vals: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		},
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

func TestRate(t *testing.T) {
	testRate(t, testRateCases)
}

var testDeltaCases = []testRateCase{
	{
		name:   "idelta",
		opType: IDeltaType,
		vals: [][]float64{
			{863682, 865910, 868138, 870366, 872594},
			{1987036, 1988988, 1990940, 1992892, 1994844},
		},
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), 2228, 2228, 2228},
			{math.NaN(), 1952, 1952, 1952, 1952},
		},
		afterAllBlocks: [][]float64{
			{-8912, 2228, 2228, 2228, 2228},
			{-7808, 1952, 1952, 1952, 1952},
		},
	},
	{
		name:   "idelta with some NaNs",
		opType: IDeltaType,
		vals: [][]float64{
			{1987036, 1988988, 1990940, math.NaN(), 1994844},
			{1987036, 1988988, 1990940, math.NaN(), math.NaN()},
		},
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), 1952, 1952, 3904},
			{math.NaN(), 1952, 1952, 1952, 1952},
		},
		afterAllBlocks: [][]float64{
			{-7808, 1952, 1952, 1952, 3904},
			{-3904, 1952, 1952, 1952, 1952},
		},
	},
	{
		name:   "idelta with all NaNs",
		opType: IDeltaType,
		vals: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		},
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		},
		afterAllBlocks: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		},
	},
	{
		name:   "delta",
		opType: DeltaType,
		vals: [][]float64{
			{678758, 680986, 683214, 685442, 687670},
			{2299, 2299, 2299, 2787, 2787},
		},
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), 4456, 6684, 8912},
			{math.NaN(), 0, 0, 650.6666, 610},
		},
		afterAllBlocks: [][]float64{
			{-2785, -2785, -2785, -2785, 11140},
			{0, 0, -610, 0, 610},
		},
	},
	{
		name:   "delta with some NaNs",
		opType: DeltaType,
		vals: [][]float64{
			{678758, 680986, 683214, 685442, math.NaN()},
			{2299, 2299, 2299, math.NaN(), 2787},
		},
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), 4456, 6684, 6684},
			{math.NaN(), 0, 0, 0, 610},
		},
		afterAllBlocks: [][]float64{
			{-2785, -2785, -2785, 8912, 8912},
			{0, 0, -650.6666, -650.6666, 610},
		},
	},
	{
		name:   "delta with all NaNs",
		opType: DeltaType,
		vals: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		},
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

func TestDelta(t *testing.T) {
	testRate(t, testDeltaCases)
}

var testIncreaseCases = []testRateCase{
	{
		name:   "increase",
		opType: IncreaseType,
		vals: [][]float64{
			{872594, 865910, 868138, 870366, 872594},
			{1987036, 1988988, 1990940, 1992892, 1994844},
		},
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), 4456, 6684, 8912},
			{math.NaN(), 3904, 5856, 7808, 9760},
		},
		afterAllBlocks: [][]float64{
			{8355, 1087957.5, 1087957.5, 1087957.5, 1090742.5},
			{2491115, 2491115, 2491115, 2491115, 9760},
		},
	},
	{
		name:   "increase with some NaNs",
		opType: IncreaseType,
		vals: [][]float64{
			{872594, 865910, 868138, 872694, math.NaN()},
			{1987036, 1988988, 1990940, math.NaN(), 1994844},
		},
		afterBlockOne: [][]float64{
			{math.NaN(), math.NaN(), 4456, 10176, 10176},
			{math.NaN(), 3904, 5856, 5856, 9760},
		},
		afterAllBlocks: [][]float64{
			{1099222.5, 2178825, 2175915, 1163592, 1163592},
			{2491115, 2491115, 2654586.6666, 2654586.6666, 9760},
		},
	},
	{
		name:   "increase with all NaNs",
		opType: IncreaseType,
		vals: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		},
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

func TestIncrease(t *testing.T) {
	testRate(t, testIncreaseCases)
}

// B1 has NaN in first series, first position
func testRate(t *testing.T, testCases []testRateCase) {
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			values, bounds := test.GenerateValuesAndBounds(tt.vals, nil)
			boundStart := bounds.Start
			block3 := test.NewUnconsolidatedBlockFromDatapoints(bounds, values)
			c, sink := executor.NewControllerWithSink(parser.NodeID(1))

			baseOp, err := NewRateOp([]interface{}{5 * time.Minute}, tt.opType)
			require.NoError(t, err)
			node := baseOp.Node(c, transformtest.Options(t, transform.OptionsParams{
				TimeSpec: transform.TimeSpec{
					Start: boundStart.Add(-2 * bounds.Duration),
					End:   bounds.End(),
					Step:  time.Minute,
				},
			}))
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

func TestUnknownRate(t *testing.T) {
	_, err := NewRateOp([]interface{}{5 * time.Minute}, "unknown_rate_func")
	require.Error(t, err)
}
