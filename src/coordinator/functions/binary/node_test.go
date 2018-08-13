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

package binary

import (
	"math"
	"testing"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/parser"
	"github.com/m3db/m3db/src/coordinator/test"
	"github.com/m3db/m3db/src/coordinator/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var scalarTests = []struct {
	name     string
	lVal     float64
	opType   string
	rVal     float64
	expected float64
}{
	/* Arithmetic */
	// +
	{"1 + 3 = 4", 1, PlusType, 3, 4},
	// -
	{"1 - 3 = -2", 1, MinusType, 3, -2},
	{"3 - 1 = 2", 3, MinusType, 1, 2},
	// *
	{"7 * 3 = 21", 7, MultiplyType, 3, 21},
	// /
	{"4 / 8 = 0.5", 4, DivType, 8, 0.5},
	{"4 / 8 = 2", 8, DivType, 4, 2},
	{"8 / 8 = 1", 8, DivType, 8, 1},
	{"8 / 0 = +infinity", 8, DivType, 0, math.Inf(1)},
	{"-8/ 0 = -infinity", -8, DivType, 0, math.Inf(-1)},
	{"0 / 0 = NaN", 0, DivType, 0, math.NaN()},
	// ^
	{"0 ^ 0 = 1", 0, ExpType, 0, 1},
	{"x ^ 0 = 1", 2, ExpType, 0, 1},
	{"0 ^ x = 0", 0, ExpType, 2, 0},
	// %
	{"8 % 0 = NaN", 8, ModType, 0, math.NaN()},
	{"8 % 3 = 2", 8, ModType, 3, 2},
	{"8 % 2 = 0", 8, ModType, 2, 0},
	{"8 % 1.5 = 1", 8, ModType, 1.5, 0.5},
	/* Comparison */
	// ==
	{"2 == 1 = 0", 2, EqType, 1, 0},
	{"2 == 2 = 1", 2, EqType, 2, 1},
	{"2 == 3 = 0", 2, EqType, 3, 0},
	// !=
	{"2 != 1 = 1", 2, NotEqType, 1, 1},
	{"2 != 2 = 0", 2, NotEqType, 2, 0},
	{"2 != 3 = 1", 2, NotEqType, 3, 1},
	// >
	{"2 > 1 = 1", 2, GreaterType, 1, 1},
	{"2 > 2 = 0", 2, GreaterType, 2, 0},
	{"2 > 3 = 0", 2, GreaterType, 3, 0},
	// <
	{"2 < 1 = 0", 2, LesserType, 1, 0},
	{"2 < 2 = 0", 2, LesserType, 2, 0},
	{"2 < 3 = 1", 2, LesserType, 3, 1},
	// >=
	{"2 >= 1 = 1", 2, GreaterEqType, 1, 1},
	{"2 >= 2 = 1", 2, GreaterEqType, 2, 1},
	{"2 >= 3 = 0", 2, GreaterEqType, 3, 0},
	// <=
	{"2 <= 1 = 0", 2, LesserEqType, 1, 0},
	{"2 <= 2 = 1", 2, LesserEqType, 2, 1},
	{"2 <= 3 = 1", 2, LesserEqType, 3, 1},
}

func TestScalars(t *testing.T) {
	_, bounds := test.GenerateValuesAndBounds(nil, nil)

	for _, tt := range scalarTests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewBinaryOp(
				tt.opType,
				NodeInformation{
					parser.NodeID(0),
					parser.NodeID(1),
					true, true,
					true, nil},
			)
			require.NoError(t, err)

			c, sink := executor.NewControllerWithSink(parser.NodeID(2))
			node := op.(binaryOp).Node(c)

			err = node.Process(parser.NodeID(0), block.NewScalarBlock(tt.lVal, bounds))
			require.NoError(t, err)

			err = node.Process(parser.NodeID(1), block.NewScalarBlock(tt.rVal, bounds))
			require.NoError(t, err)

			expected := [][]float64{{
				tt.expected, tt.expected, tt.expected,
				tt.expected, tt.expected, tt.expected,
			}}
			test.EqualsWithNans(t, expected, sink.Values)

			assert.Equal(t, bounds, sink.Meta.Bounds)
			assert.Len(t, sink.Meta.Tags, 0)

			assert.Len(t, sink.Metas, 1)
			assert.Equal(t, "", sink.Metas[0].Name)
			assert.Len(t, sink.Metas[0].Tags, 0)
		})
	}
}

type singleSeriesTest struct {
	name         string
	seriesValues [][]float64
	scalarVal    float64
	opType       string
	seriesLeft   bool
	expected     [][]float64
}

var singleSeriesTests = []singleSeriesTest{
	/* Arithmetic */
	// +
	{
		name:         "series + scalar",
		seriesValues: [][]float64{{1, math.NaN(), 3}, {4, 5, 6}},
		opType:       PlusType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{11, math.NaN(), 13}, {14, 15, 16}},
	},
	{
		name:         "scalar + series",
		scalarVal:    10,
		opType:       PlusType,
		seriesValues: [][]float64{{1, 2, 3}, {4, 5, math.NaN()}},
		seriesLeft:   false,
		expected:     [][]float64{{11, 12, 13}, {14, 15, math.NaN()}},
	},
	// -
	{
		name:         "series - scalar",
		seriesValues: [][]float64{{1, 2, 3}, {4, 5, 6}},
		opType:       MinusType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{-9, -8, -7}, {-6, -5, -4}},
	},
	{
		name:         "scalar - series",
		scalarVal:    10,
		opType:       MinusType,
		seriesValues: [][]float64{{1, 2, 3}, {4, 5, 6}},
		seriesLeft:   false,
		expected:     [][]float64{{9, 8, 7}, {6, 5, 4}},
	},
	// *
	{
		name:         "series * scalar",
		seriesValues: [][]float64{{-1, 0, math.NaN()}, {math.MaxFloat64 - 1, -1 * (math.MaxFloat64 - 1), 1}},
		opType:       MultiplyType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{-10, 0, math.NaN()}, {math.Inf(1), math.Inf(-1), 10}},
	},
	{
		name:         "scalar * series",
		scalarVal:    10,
		opType:       MultiplyType,
		seriesValues: [][]float64{{-1, 0, math.NaN(), math.MaxFloat64 - 1, -1 * (math.MaxFloat64 - 1), 1}},
		seriesLeft:   false,
		expected:     [][]float64{{-10, 0, math.NaN(), math.Inf(1), math.Inf(-1), 10}},
	},
	// /
	{
		name:         "series / scalar",
		seriesValues: [][]float64{{10, 0, 5, math.Inf(1), math.Inf(-1), math.NaN()}},
		opType:       DivType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{1, 0, 0.5, math.Inf(1), math.Inf(-1), math.NaN()}},
	},
	{
		name:         "scalar / series",
		scalarVal:    10,
		opType:       DivType,
		seriesValues: [][]float64{{10, 0, 5, math.Inf(1), math.Inf(-1), math.NaN()}},
		seriesLeft:   false,
		expected:     [][]float64{{1, math.Inf(1), 2, 0, 0, math.NaN()}},
	},
	{
		name:         "series / 0",
		seriesValues: [][]float64{{1, -2}, {3, -4}, {0, math.NaN()}},
		opType:       DivType,
		scalarVal:    0,
		seriesLeft:   true,
		expected:     [][]float64{{math.Inf(1), math.Inf(-1)}, {math.Inf(1), math.Inf(-1)}, {math.NaN(), math.NaN()}},
	},
	// ^
	{
		name:         "series ^ scalar",
		seriesValues: [][]float64{{1, 2, 3}, {4, math.NaN(), math.MaxFloat64}},
		opType:       ExpType,
		scalarVal:    2,
		seriesLeft:   true,
		expected:     [][]float64{{1, 4, 9}, {16, math.NaN(), math.Inf(1)}},
	},
	{
		name:         "scalar ^ series",
		scalarVal:    10,
		opType:       ExpType,
		seriesValues: [][]float64{{1, 2, 3}, {4, 5, 6}},
		seriesLeft:   false,
		expected:     [][]float64{{10, 100, 1000}, {10000, 100000, 1000000}},
	},
	{
		name:         "series ^ 0",
		seriesValues: [][]float64{{1, 2, 3}, {1, math.NaN(), math.MaxFloat64}},
		opType:       ExpType,
		scalarVal:    0,
		seriesLeft:   true,
		expected:     [][]float64{{1, 1, 1}, {1, 1, 1}},
	},
	{
		name:         "series ^ 0.5",
		seriesValues: [][]float64{{1, 4, 9}},
		opType:       ExpType,
		scalarVal:    0.5,
		seriesLeft:   true,
		expected:     [][]float64{{1, 2, 3}},
	},
	{
		name:         "series ^ -1",
		seriesValues: [][]float64{{1, 2, 4}},
		opType:       ExpType,
		scalarVal:    -1,
		seriesLeft:   true,
		expected:     [][]float64{{1, .5, .25}},
	},
	// %
	{
		name:         "series % scalar",
		seriesValues: [][]float64{{1, 2, 3}, {14, -105, 60}},
		opType:       ModType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{1, 2, 3}, {4, -5, 0}},
	},
	{
		name:         "scalar % series",
		scalarVal:    10,
		opType:       ModType,
		seriesValues: [][]float64{{1, 2, 3}, {4, 5, 6}},
		seriesLeft:   false,
		expected:     [][]float64{{0, 0, 1}, {2, 0, 4}},
	},
	/* Comparison */
	// ==
	{
		name:         "series == scalar",
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		opType:       EqType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{0, 0, 0, 0, 1}, {0, 0, 0, 0, 0}},
	},
	{
		name:         "scalar == series",
		scalarVal:    10,
		opType:       EqType,
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		seriesLeft:   false,
		expected:     [][]float64{{0, 0, 0, 0, 1}, {0, 0, 0, 0, 0}},
	},
	// !=
	{
		name:         "series != scalar",
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		opType:       NotEqType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{1, 1, 1, 1, 0}, {1, 1, 1, 1, 1}},
	},
	{
		name:         "scalar != series",
		scalarVal:    10,
		opType:       NotEqType,
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		seriesLeft:   false,
		expected:     [][]float64{{1, 1, 1, 1, 0}, {1, 1, 1, 1, 1}},
	},
	// >
	{
		name:         "series > scalar",
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		opType:       GreaterType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{0, 0, 0, 0, 0}, {1, 1, 0, 1, 0}},
	},
	{
		name:         "scalar > series",
		scalarVal:    10,
		opType:       GreaterType,
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		seriesLeft:   false,
		expected:     [][]float64{{1, 1, 1, 1, 0}, {0, 0, 0, 0, 1}},
	},
	// >
	{
		name:         "series < scalar",
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		opType:       LesserType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{1, 1, 1, 1, 0}, {0, 0, 0, 0, 1}},
	},
	{
		name:         "scalar < series",
		scalarVal:    10,
		opType:       LesserType,
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		seriesLeft:   false,
		expected:     [][]float64{{0, 0, 0, 0, 0}, {1, 1, 0, 1, 0}},
	},
	// >=
	{
		name:         "series >= scalar",
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		opType:       GreaterEqType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{0, 0, 0, 0, 1}, {1, 1, 0, 1, 0}},
	},
	{
		name:         "scalar >= series",
		scalarVal:    10,
		opType:       GreaterEqType,
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		seriesLeft:   false,
		expected:     [][]float64{{1, 1, 1, 1, 1}, {0, 0, 0, 0, 1}},
	},
	// <=
	{
		name:         "series <= scalar",
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		opType:       LesserEqType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{1, 1, 1, 1, 1}, {0, 0, 0, 0, 1}},
	},
	{
		name:         "scalar <= series",
		scalarVal:    10,
		opType:       LesserEqType,
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		seriesLeft:   false,
		expected:     [][]float64{{0, 0, 0, 0, 1}, {1, 1, 0, 1, 0}},
	},
}

func TestSingleSeriesReturnBool(t *testing.T) {
	returnBool := true
	_, bounds := test.GenerateValuesAndBounds(nil, nil)

	for _, tt := range singleSeriesTests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewBinaryOp(
				tt.opType,
				NodeInformation{
					parser.NodeID(0),
					parser.NodeID(1),
					!tt.seriesLeft, tt.seriesLeft,
					returnBool, nil},
			)
			require.NoError(t, err)

			c, sink := executor.NewControllerWithSink(parser.NodeID(2))
			node := op.(binaryOp).Node(c)

			seriesValues := tt.seriesValues
			metas := test.NewSeriesMeta("a", len(seriesValues))
			series := test.NewBlockFromValuesWithSeriesMeta(bounds, metas, seriesValues)
			// Set the series and scalar blocks on the correct sides
			if tt.seriesLeft {
				err = node.Process(parser.NodeID(0), series)
				require.NoError(t, err)

				err = node.Process(parser.NodeID(1), block.NewScalarBlock(tt.scalarVal, bounds))
				require.NoError(t, err)
			} else {
				err = node.Process(parser.NodeID(0), block.NewScalarBlock(tt.scalarVal, bounds))
				require.NoError(t, err)

				err = node.Process(parser.NodeID(1), series)
				require.NoError(t, err)
			}

			test.EqualsWithNans(t, tt.expected, sink.Values)

			assert.Equal(t, bounds, sink.Meta.Bounds)
			assert.Len(t, sink.Meta.Tags, 0)

			assert.Equal(t, metas, sink.Metas)
		})
	}
}

func TestSingleSeriesReturnValues(t *testing.T) {
	returnBool := false
	_, bounds := test.GenerateValuesAndBounds(nil, nil)

	comparisonTests := make([]singleSeriesTest, 0, 10)
	for _, test := range singleSeriesTests {
		if isComparison(test.opType) {
			comparisonTests = append(comparisonTests, test)
		}
	}

	for _, tt := range comparisonTests {
		t.Run(tt.name, func(t *testing.T) {
			// So as to not re-generate test case for ReturnBool == true
			// construct the expected results here
			expected := make([][]float64, len(tt.expected))
			for i := range expected {
				expected[i] = make([]float64, len(tt.expected[i]))

				for ii := range expected[i] {
					takeValue := tt.expected[i][ii] == 1
					if takeValue {
						expected[i][ii] = tt.seriesValues[i][ii]
					} else {
						expected[i][ii] = math.NaN()
					}
				}
			}

			op, err := NewBinaryOp(
				tt.opType,
				NodeInformation{
					parser.NodeID(0),
					parser.NodeID(1),
					!tt.seriesLeft, tt.seriesLeft,
					returnBool, nil},
			)
			require.NoError(t, err)

			c, sink := executor.NewControllerWithSink(parser.NodeID(2))
			node := op.(binaryOp).Node(c)

			seriesValues := tt.seriesValues
			metas := test.NewSeriesMeta("a", len(seriesValues))
			series := test.NewBlockFromValuesWithSeriesMeta(bounds, metas, seriesValues)
			// Set the series and scalar blocks on the correct sides
			if tt.seriesLeft {
				err = node.Process(parser.NodeID(0), series)
				require.NoError(t, err)

				err = node.Process(parser.NodeID(1), block.NewScalarBlock(tt.scalarVal, bounds))
				require.NoError(t, err)
			} else {
				err = node.Process(parser.NodeID(0), block.NewScalarBlock(tt.scalarVal, bounds))
				require.NoError(t, err)

				err = node.Process(parser.NodeID(1), series)
				require.NoError(t, err)
			}

			test.EqualsWithNans(t, expected, sink.Values)

			assert.Equal(t, bounds, sink.Meta.Bounds)
			assert.Len(t, sink.Meta.Tags, 0)

			assert.Equal(t, metas, sink.Metas)
		})
	}
}
