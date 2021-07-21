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
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/compare"
	"github.com/m3db/m3/src/query/test/executor"
	xtime "github.com/m3db/m3/src/x/time"

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
			op, err := NewOp(
				tt.opType,
				NodeParams{
					LNode:                parser.NodeID(rune(0)),
					RNode:                parser.NodeID(rune(1)),
					ReturnBool:           true,
					VectorMatcherBuilder: emptyVectorMatcherBuilder,
				},
			)
			require.NoError(t, err)

			c, sink := executor.NewControllerWithSink(parser.NodeID(rune(2)))
			node := op.(baseOp).Node(c, transform.Options{})

			err = node.Process(
				models.NoopQueryContext(),
				parser.NodeID(rune(0)),
				block.NewScalar(tt.lVal, block.Metadata{
					Bounds: bounds,
					Tags:   models.EmptyTags(),
				}),
			)

			require.NoError(t, err)
			err = node.Process(
				models.NoopQueryContext(),
				parser.NodeID(rune(1)),
				block.NewScalar(tt.rVal, block.Metadata{
					Bounds: bounds,
					Tags:   models.EmptyTags(),
				}),
			)

			expected := [][]float64{{
				tt.expected, tt.expected, tt.expected,
				tt.expected, tt.expected,
			}}

			compare.EqualsWithNans(t, expected, sink.Values)

			assert.Equal(t, bounds, sink.Meta.Bounds)
			assert.Equal(t, 0, sink.Meta.Tags.Len())

			assert.Len(t, sink.Metas, 1)
			assert.Equal(t, []byte(nil), sink.Metas[0].Name)
			assert.Equal(t, 0, sink.Metas[0].Tags.Len())
		})
	}
}

func TestScalarsReturnBoolFalse(t *testing.T) {
	_, bounds := test.GenerateValuesAndBounds(nil, nil)

	for _, tt := range scalarTests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewOp(
				tt.opType,
				NodeParams{
					LNode:                parser.NodeID(rune(0)),
					RNode:                parser.NodeID(rune(1)),
					ReturnBool:           false,
					VectorMatcherBuilder: emptyVectorMatcherBuilder,
				},
			)
			require.NoError(t, err)

			c, sink := executor.NewControllerWithSink(parser.NodeID(rune(2)))
			node := op.(baseOp).Node(c, transform.Options{})

			err = node.Process(
				models.NoopQueryContext(),
				parser.NodeID(rune(0)),
				block.NewScalar(tt.lVal, block.Metadata{
					Bounds: bounds,
					Tags:   models.EmptyTags(),
				}),
			)

			require.NoError(t, err)
			err = node.Process(
				models.NoopQueryContext(),
				parser.NodeID(rune(1)),
				block.NewScalar(tt.rVal, block.Metadata{
					Bounds: bounds,
					Tags:   models.EmptyTags(),
				}),
			)

			if tt.opType == EqType || tt.opType == NotEqType ||
				tt.opType == GreaterType || tt.opType == LesserType ||
				tt.opType == GreaterEqType || tt.opType == LesserEqType {
				require.Error(t, err, "scalar comparisons must fail without returnBool")
				return
			}

			require.NoError(t, err, "scalar maths must succeed without returnBool")

			expected := [][]float64{{
				tt.expected, tt.expected, tt.expected,
				tt.expected, tt.expected,
			}}

			compare.EqualsWithNans(t, expected, sink.Values)

			assert.Equal(t, bounds, sink.Meta.Bounds)
			assert.Equal(t, 0, sink.Meta.Tags.Len())

			assert.Len(t, sink.Metas, 1)
			assert.Equal(t, []byte(nil), sink.Metas[0].Name)
			assert.Equal(t, 0, sink.Metas[0].Tags.Len())
		})
	}
}

var singleSeriesTests = []struct {
	name         string
	seriesValues [][]float64
	scalarVal    float64
	opType       string
	seriesLeft   bool
	expected     [][]float64
	expectedBool [][]float64
}{
	/* Arithmetic */
	// +
	{
		name:         "series + scalar",
		seriesValues: [][]float64{{1, math.NaN(), 3}, {4, 5, 6}},
		opType:       PlusType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{11, math.NaN(), 13}, {14, 15, 16}},
		expectedBool: [][]float64{{11, math.NaN(), 13}, {14, 15, 16}},
	},
	{
		name:         "scalar + series",
		scalarVal:    10,
		opType:       PlusType,
		seriesValues: [][]float64{{1, 2, 3}, {4, 5, math.NaN()}},
		seriesLeft:   false,
		expected:     [][]float64{{11, 12, 13}, {14, 15, math.NaN()}},
		expectedBool: [][]float64{{11, 12, 13}, {14, 15, math.NaN()}},
	},
	// -
	{
		name:         "series - scalar",
		seriesValues: [][]float64{{1, 2, 3}, {4, 5, 6}},
		opType:       MinusType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{-9, -8, -7}, {-6, -5, -4}},
		expectedBool: [][]float64{{-9, -8, -7}, {-6, -5, -4}},
	},
	{
		name:         "scalar - series",
		scalarVal:    10,
		opType:       MinusType,
		seriesValues: [][]float64{{1, 2, 3}, {4, 5, 6}},
		seriesLeft:   false,
		expected:     [][]float64{{9, 8, 7}, {6, 5, 4}},
		expectedBool: [][]float64{{9, 8, 7}, {6, 5, 4}},
	},
	// *
	{
		name: "series * scalar",
		seriesValues: [][]float64{
			{-1, 0, math.NaN()},
			{math.MaxFloat64 - 1, -1 * (math.MaxFloat64 - 1), 1},
		},
		opType:       MultiplyType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{-10, 0, math.NaN()}, {math.Inf(1), math.Inf(-1), 10}},
		expectedBool: [][]float64{{-10, 0, math.NaN()}, {math.Inf(1), math.Inf(-1), 10}},
	},
	{
		name:         "scalar * series",
		scalarVal:    10,
		opType:       MultiplyType,
		seriesValues: [][]float64{{-1, 0, math.NaN(), math.MaxFloat64 - 1, -1 * (math.MaxFloat64 - 1), 1}},
		seriesLeft:   false,
		expected:     [][]float64{{-10, 0, math.NaN(), math.Inf(1), math.Inf(-1), 10}},
		expectedBool: [][]float64{{-10, 0, math.NaN(), math.Inf(1), math.Inf(-1), 10}},
	},
	// /
	{
		name:         "series / scalar",
		seriesValues: [][]float64{{10, 0, 5, math.Inf(1), math.Inf(-1), math.NaN()}},
		opType:       DivType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{1, 0, 0.5, math.Inf(1), math.Inf(-1), math.NaN()}},
		expectedBool: [][]float64{{1, 0, 0.5, math.Inf(1), math.Inf(-1), math.NaN()}},
	},
	{
		name:         "scalar / series",
		scalarVal:    10,
		opType:       DivType,
		seriesValues: [][]float64{{10, 0, 5, math.Inf(1), math.Inf(-1), math.NaN()}},
		seriesLeft:   false,
		expected:     [][]float64{{1, math.Inf(1), 2, 0, 0, math.NaN()}},
		expectedBool: [][]float64{{1, math.Inf(1), 2, 0, 0, math.NaN()}},
	},
	{
		name:         "series / 0",
		seriesValues: [][]float64{{1, -2}, {3, -4}, {0, math.NaN()}},
		opType:       DivType,
		scalarVal:    0,
		seriesLeft:   true,
		expected:     [][]float64{{math.Inf(1), math.Inf(-1)}, {math.Inf(1), math.Inf(-1)}, {math.NaN(), math.NaN()}},
		expectedBool: [][]float64{{math.Inf(1), math.Inf(-1)}, {math.Inf(1), math.Inf(-1)}, {math.NaN(), math.NaN()}},
	},
	// ^
	{
		name:         "series ^ scalar",
		seriesValues: [][]float64{{1, 2, 3}, {4, math.NaN(), math.MaxFloat64}},
		opType:       ExpType,
		scalarVal:    2,
		seriesLeft:   true,
		expected:     [][]float64{{1, 4, 9}, {16, math.NaN(), math.Inf(1)}},
		expectedBool: [][]float64{{1, 4, 9}, {16, math.NaN(), math.Inf(1)}},
	},
	{
		name:         "scalar ^ series",
		scalarVal:    10,
		opType:       ExpType,
		seriesValues: [][]float64{{1, 2, 3}, {4, 5, 6}},
		seriesLeft:   false,
		expected:     [][]float64{{10, 100, 1000}, {10000, 100000, 1000000}},
		expectedBool: [][]float64{{10, 100, 1000}, {10000, 100000, 1000000}},
	},
	{
		name:         "series ^ 0",
		seriesValues: [][]float64{{1, 2, 3}, {1, math.NaN(), math.MaxFloat64}},
		opType:       ExpType,
		scalarVal:    0,
		seriesLeft:   true,
		expected:     [][]float64{{1, 1, 1}, {1, 1, 1}},
		expectedBool: [][]float64{{1, 1, 1}, {1, 1, 1}},
	},
	{
		name:         "series ^ 0.5",
		seriesValues: [][]float64{{1, 4, 9}},
		opType:       ExpType,
		scalarVal:    0.5,
		seriesLeft:   true,
		expected:     [][]float64{{1, 2, 3}},
		expectedBool: [][]float64{{1, 2, 3}},
	},
	{
		name:         "series ^ -1",
		seriesValues: [][]float64{{1, 2, 4}},
		opType:       ExpType,
		scalarVal:    -1,
		seriesLeft:   true,
		expected:     [][]float64{{1, .5, .25}},
		expectedBool: [][]float64{{1, .5, .25}},
	},
	// %
	{
		name:         "series % scalar",
		seriesValues: [][]float64{{1, 2, 3}, {14, -105, 60}},
		opType:       ModType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{1, 2, 3}, {4, -5, 0}},
		expectedBool: [][]float64{{1, 2, 3}, {4, -5, 0}},
	},
	{
		name:         "scalar % series",
		scalarVal:    10,
		opType:       ModType,
		seriesValues: [][]float64{{1, 2, 3}, {4, 5, 6}},
		seriesLeft:   false,
		expected:     [][]float64{{0, 0, 1}, {2, 0, 4}},
		expectedBool: [][]float64{{0, 0, 1}, {2, 0, 4}},
	},
	/* Comparison */
	// ==
	{
		name: "series == scalar",
		seriesValues: [][]float64{{-10, 0, 1, 9, 10},
			{11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		opType:     EqType,
		scalarVal:  10,
		seriesLeft: true,
		expected: [][]float64{{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 10},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()}},
		expectedBool: [][]float64{{0, 0, 0, 0, 1}, {0, 0, 0, 0, 0}},
	},
	{
		name:      "scalar == series",
		scalarVal: 10,
		opType:    EqType,
		seriesValues: [][]float64{{-10, 0, 1, 9, 10},
			{11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		seriesLeft: false,
		expected: [][]float64{{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 10},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()}},
		expectedBool: [][]float64{{0, 0, 0, 0, 1}, {0, 0, 0, 0, 0}},
	},
	// !=
	{
		name:         "series != scalar",
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		opType:       NotEqType,
		scalarVal:    10,
		seriesLeft:   true,
		expected:     [][]float64{{-10, 0, 1, 9, math.NaN()}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		expectedBool: [][]float64{{1, 1, 1, 1, 0}, {1, 1, 1, 1, 1}},
	},
	{
		name:         "scalar != series",
		scalarVal:    10,
		opType:       NotEqType,
		seriesValues: [][]float64{{-10, 0, 1, 9, 10}, {11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		seriesLeft:   false,
		expected:     [][]float64{{10, 10, 10, 10, math.NaN()}, {10, 10, 10, 10, 10}},
		expectedBool: [][]float64{{1, 1, 1, 1, 0}, {1, 1, 1, 1, 1}},
	},
	// >
	{
		name: "series > scalar",
		seriesValues: [][]float64{{-10, 0, 1, 9, 10},
			{11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		opType:     GreaterType,
		scalarVal:  10,
		seriesLeft: true,
		expected: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			{11, math.MaxFloat64, math.NaN(), math.Inf(1), math.NaN()},
		},
		expectedBool: [][]float64{{0, 0, 0, 0, 0}, {1, 1, 0, 1, 0}},
	},
	{
		name:      "scalar > series",
		scalarVal: 10,
		opType:    GreaterType,
		seriesValues: [][]float64{
			{-10, 0, 1, 9, 10},
			{11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)},
		},
		seriesLeft: false,
		expected: [][]float64{
			{10, 10, 10, 10, math.NaN()},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 10},
		},
		expectedBool: [][]float64{{1, 1, 1, 1, 0}, {0, 0, 0, 0, 1}},
	},
	// >
	{
		name: "series < scalar",
		seriesValues: [][]float64{
			{-10, 0, 1, 9, 10},
			{11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)},
		},
		opType:     LesserType,
		scalarVal:  10,
		seriesLeft: true,
		expected: [][]float64{
			{-10, 0, 1, 9, math.NaN()},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.Inf(-1)},
		},
		expectedBool: [][]float64{{1, 1, 1, 1, 0}, {0, 0, 0, 0, 1}},
	},
	{
		name:      "scalar < series",
		scalarVal: 10,
		opType:    LesserType,
		seriesValues: [][]float64{{-10, 0, 1, 9, 10},
			{11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		seriesLeft: false,
		expected: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
			{10, 10, math.NaN(), 10, math.NaN()},
		},
		expectedBool: [][]float64{{0, 0, 0, 0, 0}, {1, 1, 0, 1, 0}},
	},
	// >=
	{
		name: "series >= scalar",
		seriesValues: [][]float64{{-10, 0, 1, 9, 10},
			{11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)}},
		opType:     GreaterEqType,
		scalarVal:  10,
		seriesLeft: true,
		expected: [][]float64{{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 10},
			{11, math.MaxFloat64, math.NaN(), math.Inf(1), math.NaN()}},
		expectedBool: [][]float64{{0, 0, 0, 0, 1}, {1, 1, 0, 1, 0}},
	},
	{
		name:      "scalar >= series",
		scalarVal: 10,
		opType:    GreaterEqType,
		seriesValues: [][]float64{
			{-10, 0, 1, 9, 10},
			{11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)},
		},
		seriesLeft: false,
		expected: [][]float64{
			{10, 10, 10, 10, 10},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 10},
		},
		expectedBool: [][]float64{{1, 1, 1, 1, 1}, {0, 0, 0, 0, 1}},
	},
	// <=
	{
		name: "series <= scalar",
		seriesValues: [][]float64{
			{-10, 0, 1, 9, 10},
			{11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)},
		},
		opType:     LesserEqType,
		scalarVal:  10,
		seriesLeft: true,
		expected: [][]float64{
			{-10, 0, 1, 9, 10},
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.Inf(-1)},
		},
		expectedBool: [][]float64{{1, 1, 1, 1, 1}, {0, 0, 0, 0, 1}},
	},
	{
		name:      "scalar <= series",
		scalarVal: 10,
		opType:    LesserEqType,
		seriesValues: [][]float64{
			{-10, 0, 1, 9, 10},
			{11, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)},
		},
		seriesLeft: false,
		expected: [][]float64{
			{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 10},
			{10, 10, math.NaN(), 10, math.NaN()},
		},
		expectedBool: [][]float64{{0, 0, 0, 0, 1}, {1, 1, 0, 1, 0}},
	},
}

func TestSingleSeriesReturnBool(t *testing.T) {
	now := xtime.Now()

	for _, tt := range singleSeriesTests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewOp(
				tt.opType,
				NodeParams{
					LNode:                parser.NodeID(rune(0)),
					RNode:                parser.NodeID(rune(1)),
					ReturnBool:           true,
					VectorMatcherBuilder: emptyVectorMatcherBuilder,
				},
			)
			require.NoError(t, err)

			c, sink := executor.NewControllerWithSink(parser.NodeID(rune(2)))
			node := op.(baseOp).Node(c, transform.Options{})

			seriesValues := tt.seriesValues
			metas := test.NewSeriesMeta("a", len(seriesValues))
			bounds := models.Bounds{
				Start:    now,
				Duration: time.Minute * time.Duration(len(seriesValues[0])),
				StepSize: time.Minute,
			}

			series := test.NewBlockFromValuesWithSeriesMeta(bounds, metas, seriesValues)
			// Set the series and scalar blocks on the correct sides
			if tt.seriesLeft {
				err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), series)
				require.NoError(t, err)

				err = node.Process(
					models.NoopQueryContext(),
					parser.NodeID(rune(1)),
					block.NewScalar(tt.scalarVal, block.Metadata{
						Bounds: bounds,
						Tags:   models.EmptyTags(),
					}),
				)

				require.NoError(t, err)
			} else {
				err = node.Process(
					models.NoopQueryContext(),
					parser.NodeID(rune(0)),
					block.NewScalar(tt.scalarVal, block.Metadata{
						Bounds: bounds,
						Tags:   models.EmptyTags(),
					}),
				)

				require.NoError(t, err)
				err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(1)), series)
				require.NoError(t, err)
			}

			compare.EqualsWithNans(t, tt.expectedBool, sink.Values)

			assert.Equal(t, bounds, sink.Meta.Bounds)
			assert.Equal(t, 0, sink.Meta.Tags.Len())

			assert.Equal(t, metas, sink.Metas)
		})
	}
}

func TestSingleSeriesReturnValues(t *testing.T) {
	now := xtime.Now()

	for _, tt := range singleSeriesTests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewOp(
				tt.opType,
				NodeParams{
					LNode:                parser.NodeID(rune(0)),
					RNode:                parser.NodeID(rune(1)),
					ReturnBool:           false,
					VectorMatcherBuilder: emptyVectorMatcherBuilder,
				},
			)

			require.NoError(t, err)
			c, sink := executor.NewControllerWithSink(parser.NodeID(rune(2)))
			node := op.(baseOp).Node(c, transform.Options{})

			seriesValues := tt.seriesValues
			metas := test.NewSeriesMeta("a", len(seriesValues))
			bounds := models.Bounds{
				Start:    now,
				Duration: time.Minute * time.Duration(len(seriesValues[0])),
				StepSize: time.Minute,
			}

			series := test.NewBlockFromValuesWithSeriesMeta(bounds, metas, seriesValues)
			// Set the series and scalar blocks on the correct sides
			if tt.seriesLeft {
				err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), series)
				require.NoError(t, err)

				err = node.Process(
					models.NoopQueryContext(),
					parser.NodeID(rune(1)),
					block.NewScalar(tt.scalarVal, block.Metadata{
						Bounds: bounds,
						Tags:   models.EmptyTags(),
					}),
				)

				require.NoError(t, err)
			} else {
				err = node.Process(
					models.NoopQueryContext(),
					parser.NodeID(rune(0)),
					block.NewScalar(tt.scalarVal, block.Metadata{
						Bounds: bounds,
						Tags:   models.EmptyTags(),
					}),
				)

				require.NoError(t, err)
				err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(1)), series)
				require.NoError(t, err)
			}

			compare.EqualsWithNans(t, tt.expected, sink.Values)

			assert.Equal(t, bounds, sink.Meta.Bounds)
			assert.Equal(t, 0, sink.Meta.Tags.Len())

			assert.Equal(t, metas, sink.Metas)
		})
	}
}

var bothSeriesTests = []struct {
	name          string
	opType        string
	lhsMeta       []block.SeriesMeta
	lhs           [][]float64
	rhsMeta       []block.SeriesMeta
	rhs           [][]float64
	returnBool    bool
	expectedMetas []block.SeriesMeta
	expected      [][]float64
}{
	/* Arithmetic */
	{
		"+, second series matches first",
		PlusType,
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2, 3}, {4, 5, 6}},
		test.NewSeriesMeta("a", 3)[1:],
		[][]float64{{10, 20, 30}, {40, 50, 60}},
		true,
		test.NewSeriesMeta("a", 2)[1:],
		[][]float64{{14, 25, 36}},
	},
	{
		"-, first two series on lhs match",
		MinusType,
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2, 3}, {4, 5, 6}},
		test.NewSeriesMeta("a", 3),
		[][]float64{{10, 20, 30}, {40, 50, 60}, {700, 800, 900}},
		true,
		test.NewSeriesMeta("a", 2),
		[][]float64{{-9, -18, -27}, {-36, -45, -54}},
	},
	{
		"*, last two series on lhs match",
		MultiplyType,
		test.NewSeriesMeta("a", 3),
		[][]float64{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
		test.NewSeriesMeta("a", 3)[1:],
		[][]float64{{10, 20, 30}, {40, 50, 60}},
		true,
		test.NewSeriesMeta("a", 3)[1:],
		[][]float64{{40, 100, 180}, {280, 400, 540}},
	},
	{
		"/, both series match",
		DivType,
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2, 3}, {40, 50, 60}},
		test.NewSeriesMeta("a", 2),
		[][]float64{{10, 20, 30}, {4, 5, 6}},
		true,
		test.NewSeriesMeta("a", 2),
		[][]float64{{0.1, 0.1, 0.1}, {10, 10, 10}},
	},
	{
		"^, single matching series",
		ExpType,
		test.NewSeriesMeta("a", 1),
		[][]float64{{10, math.NaN(), -1, 9, 10, 4}},
		test.NewSeriesMeta("a", 1),
		[][]float64{{2, 2, 0.5, 0.5, -1, -0.5}},
		true,
		test.NewSeriesMeta("a", 1),
		[][]float64{{100, math.NaN(), math.NaN(), 3, 0.1, 0.5}},
	},
	{
		"%, single matching series",
		ModType,
		test.NewSeriesMeta("a", 1),
		[][]float64{{10, 11, 12, 13, 14, 15}},
		test.NewSeriesMeta("a", 1),
		[][]float64{{2, 3, -5, 1.5, 1.5, -1.5}},
		true,
		test.NewSeriesMeta("a", 1),
		[][]float64{{0, 2, 2, 1, 0.5, 0}},
	},
	/* Comparison */
	{
		"==, second series matches first",
		EqType,
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2, 3}, {3, 6, 9}},
		test.NewSeriesMeta("a", 3)[1:],
		[][]float64{{10, 6, 30}, {40, 50, 60}},
		false,
		test.NewSeriesMeta("a", 2)[1:],
		[][]float64{{math.NaN(), 6, math.NaN()}},
	},
	{
		"== BOOL, second series matches first",
		EqType,
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2, 3}, {3, 6, 9}},
		test.NewSeriesMeta("a", 3)[1:],
		[][]float64{{10, 6, 30}, {40, 50, 60}},
		true,
		test.NewSeriesMeta("a", 2)[1:],
		[][]float64{{0, 1, 0}},
	},
	{
		"=! BOOL, both series match",
		NotEqType,
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2, 3}, {3, 6, 9}},
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 20, 3}, {40, 6, 60}},
		true,
		test.NewSeriesMeta("a", 2),
		[][]float64{{0, 1, 0}, {1, 0, 1}},
	},
	{
		"=!, both series match",
		NotEqType,
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2, 3}, {3, 6, 9}},
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 20, 3}, {40, 6, 60}},
		false,
		test.NewSeriesMeta("a", 2),
		[][]float64{{math.NaN(), 2, math.NaN()}, {3, math.NaN(), 9}},
	},
	{
		"> BOOL, last two series of rhs match",
		GreaterType,
		test.NewSeriesMeta("a", 3)[1:],
		[][]float64{{1, 2, 3}, {3, 6, 9}},
		test.NewSeriesMeta("a", 3),
		[][]float64{{10, 10, 10}, {1, 20, -100}, {2, 4, 10}},
		true,
		test.NewSeriesMeta("a", 3)[1:],
		[][]float64{{0, 0, 1}, {1, 1, 0}},
	},
	{
		">, last two series of rhs match",
		GreaterType,
		test.NewSeriesMeta("a", 3)[1:],
		[][]float64{{1, 2, 3}, {3, 6, 9}},
		test.NewSeriesMeta("a", 3),
		[][]float64{{10, 10, 10}, {1, 20, -100}, {2, 4, 10}},
		false,
		test.NewSeriesMeta("a", 3)[1:],
		[][]float64{{math.NaN(), math.NaN(), 3}, {3, 6, math.NaN()}},
	},
	{
		"< BOOL, single series matches",
		LesserType,
		test.NewSeriesMeta("a", 1),
		[][]float64{{1, 2, 3}},
		test.NewSeriesMeta("a", 1),
		[][]float64{{-1, 2, 5}},
		true,
		test.NewSeriesMeta("a", 1),
		[][]float64{{0, 0, 1}},
	},
	{
		"<, single series matches",
		LesserType,
		test.NewSeriesMeta("a", 1),
		[][]float64{{1, 2, 3}},
		test.NewSeriesMeta("a", 1),
		[][]float64{{-1, 2, 5}},
		false,
		test.NewSeriesMeta("a", 1),
		[][]float64{{math.NaN(), math.NaN(), 3}},
	},
	{
		">= BOOL, single series matches",
		GreaterEqType,
		test.NewSeriesMeta("a", 1),
		[][]float64{{1, 2, 3}},
		test.NewSeriesMeta("a", 1),
		[][]float64{{-1, 2, 5}},
		true,
		test.NewSeriesMeta("a", 1),
		[][]float64{{1, 1, 0}},
	},
	{
		">=, single series matches",
		GreaterEqType,
		test.NewSeriesMeta("a", 1),
		[][]float64{{1, 2, 3}},
		test.NewSeriesMeta("a", 1),
		[][]float64{{-1, 2, 5}},
		false,
		test.NewSeriesMeta("a", 1),
		[][]float64{{1, 2, math.NaN()}},
	},
	{
		"<= BOOL, single series matches",
		LesserEqType,
		test.NewSeriesMeta("a", 1),
		[][]float64{{1, 2, 3}},
		test.NewSeriesMeta("a", 1),
		[][]float64{{-1, 2, 5}},
		true,
		test.NewSeriesMeta("a", 1),
		[][]float64{{0, 1, 1}},
	},
	{
		"<=, single series matches",
		LesserEqType,
		test.NewSeriesMeta("a", 1),
		[][]float64{{1, 2, 3}},
		test.NewSeriesMeta("a", 1),
		[][]float64{{-1, 2, 5}},
		false,
		test.NewSeriesMeta("a", 1),
		[][]float64{{math.NaN(), 2, 3}},
	},
}

func TestBothSeries(t *testing.T) {
	now := xtime.Now()

	for _, tt := range bothSeriesTests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewOp(
				tt.opType,
				NodeParams{
					LNode:                parser.NodeID(rune(0)),
					RNode:                parser.NodeID(rune(1)),
					ReturnBool:           tt.returnBool,
					VectorMatcherBuilder: emptyVectorMatcherBuilder,
				},
			)
			require.NoError(t, err)

			c, sink := executor.NewControllerWithSink(parser.NodeID(rune(2)))
			node := op.(baseOp).Node(c, transform.Options{})
			bounds := models.Bounds{
				Start:    now,
				Duration: time.Minute * time.Duration(len(tt.lhs[0])),
				StepSize: time.Minute,
			}

			err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)),
				test.NewBlockFromValuesWithSeriesMeta(bounds, tt.lhsMeta, tt.lhs))
			require.NoError(t, err)

			err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(1)),
				test.NewBlockFromValuesWithSeriesMeta(bounds, tt.rhsMeta, tt.rhs))
			require.NoError(t, err)

			compare.EqualsWithNans(t, tt.expected, sink.Values)

			// Extract duped expected metas
			expectedMeta := block.Metadata{
				Bounds:         bounds,
				ResultMetadata: block.NewResultMetadata(),
			}
			var expectedMetas []block.SeriesMeta
			expectedMeta.Tags, expectedMetas = utils.DedupeMetadata(
				tt.expectedMetas, models.NewTagOptions())
			expectedMeta, expectedMetas = removeNameTags(expectedMeta, expectedMetas)
			assert.Equal(t, expectedMeta, sink.Meta)
			assert.Equal(t, expectedMetas, sink.Metas)
		})
	}
}

func TestBinaryFunctionWithDifferentNames(t *testing.T) {
	now := xtime.Now()

	meta := func(bounds models.Bounds, name string, m block.ResultMetadata) block.Metadata {
		return block.Metadata{
			Bounds:         bounds,
			Tags:           models.NewTags(1, models.NewTagOptions()).SetName([]byte(name)),
			ResultMetadata: m,
		}
	}

	var (
		bounds = models.Bounds{
			Start:    now,
			Duration: time.Minute * 3,
			StepSize: time.Minute,
		}

		lhsResultMeta = block.ResultMetadata{
			LocalOnly:  true,
			Exhaustive: false,
			Warnings:   []block.Warning{},
		}

		lhsMeta  = meta(bounds, "left", lhsResultMeta)
		lhsMetas = test.NewSeriesMeta("a", 2)
		lhs      = [][]float64{{1, 2, 3}, {4, 5, 6}}
		left     = test.NewBlockFromValuesWithMetaAndSeriesMeta(
			lhsMeta, lhsMetas, lhs,
		)

		rhsResultMeta = block.ResultMetadata{
			LocalOnly:  false,
			Exhaustive: true,
			Warnings:   []block.Warning{block.Warning{Name: "foo", Message: "bar"}},
		}

		rhsMeta  = meta(bounds, "right", rhsResultMeta)
		rhsMetas = test.NewSeriesMeta("a", 3)[1:]
		rhs      = [][]float64{{10, 20, 30}, {40, 50, 60}}
		right    = test.NewBlockFromValuesWithMetaAndSeriesMeta(
			rhsMeta, rhsMetas, rhs,
		)

		expected = [][]float64{{14, 25, 36}}
	)

	op, err := NewOp(
		PlusType,
		NodeParams{
			LNode:                parser.NodeID(rune(0)),
			RNode:                parser.NodeID(rune(1)),
			VectorMatcherBuilder: emptyVectorMatcherBuilder,
		},
	)
	require.NoError(t, err)

	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(2)))
	node := op.(baseOp).Node(c, transform.Options{})

	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), left)
	require.NoError(t, err)

	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(1)), right)
	require.NoError(t, err)

	compare.EqualsWithNans(t, expected, sink.Values)

	exResultMeta := block.ResultMetadata{
		LocalOnly:  false,
		Exhaustive: false,
		Warnings:   []block.Warning{block.Warning{Name: "foo", Message: "bar"}},
	}

	// Extract duped expected metas
	expectedMeta := block.Metadata{
		Bounds:         bounds,
		Tags:           models.NewTags(1, models.NewTagOptions()).AddTag(toTag("a1", "a1")),
		ResultMetadata: exResultMeta,
	}

	expectedMetas := []block.SeriesMeta{
		block.SeriesMeta{
			Name: []byte("a1"),
			Tags: models.EmptyTags(),
		},
	}

	assert.Equal(t, expectedMeta, sink.Meta)
	assert.Equal(t, expectedMetas, sink.Metas)
}

func TestOneToOneMatcher(t *testing.T) {
	now := xtime.Now()

	meta := func(bounds models.Bounds, name string, m block.ResultMetadata) block.Metadata {
		return block.Metadata{
			Bounds:         bounds,
			Tags:           models.NewTags(1, models.NewTagOptions()).SetName([]byte(name)),
			ResultMetadata: m,
		}
	}

	var (
		bounds = models.Bounds{
			Start:    now,
			Duration: time.Minute * 3,
			StepSize: time.Minute,
		}

		lhsResultMeta = block.ResultMetadata{
			LocalOnly:  true,
			Exhaustive: false,
			Warnings:   []block.Warning{},
		}

		lhsMeta  = meta(bounds, "left", lhsResultMeta)
		lhsMetas = test.NewSeriesMeta("a", 2)
		lhs      = [][]float64{{1, 2, 3}, {4, 5, 6}}
		left     = test.NewBlockFromValuesWithMetaAndSeriesMeta(
			lhsMeta, lhsMetas, lhs,
		)

		rhsResultMeta = block.ResultMetadata{
			LocalOnly:  false,
			Exhaustive: true,
			Warnings:   []block.Warning{{Name: "foo", Message: "bar"}},
		}

		rhsMeta  = meta(bounds, "right", rhsResultMeta)
		rhsMetas = test.NewSeriesMeta("a", 3)[1:]
		rhs      = [][]float64{{10, 20, 30}, {40, 50, 60}}
		right    = test.NewBlockFromValuesWithMetaAndSeriesMeta(
			rhsMeta, rhsMetas, rhs,
		)

		expected = [][]float64{{41, 52, 63}, {14, 25, 36}}
	)

	op, err := NewOp(
		PlusType,
		NodeParams{
			LNode:                parser.NodeID(rune(0)),
			RNode:                parser.NodeID(rune(1)),
			VectorMatcherBuilder: oneToOneVectorMatchingBuilder,
		},
	)
	require.NoError(t, err)

	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(2)))
	node := op.(baseOp).Node(c, transform.Options{})

	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), left)
	require.NoError(t, err)

	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(1)), right)
	require.NoError(t, err)

	compare.EqualsWithNans(t, expected, sink.Values)

	expectedMetas := []block.SeriesMeta{
		{
			Name: []byte("a0"),
			Tags: models.EmptyTags(),
		},
		{
			Name: []byte("a1"),
			Tags: models.NewTags(1, models.NewTagOptions()).AddTag(toTag("a1", "a1")),
		},
	}

	assert.Equal(t, expectedMetas, sink.Metas)
}

func oneToOneVectorMatchingBuilder(_, _ block.Block) VectorMatching {
	return VectorMatching{
		Set:            true,
		Card:           CardOneToOne,
		On:             true,
		MatchingLabels: [][]byte{[]byte("a1")},
	}
}
