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

package linear

import (
	"math"
	"testing"

	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/compare"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func expectedMathVals(values [][]float64, fn func(x float64) float64) [][]float64 {
	expected := make([][]float64, 0, len(values))
	for _, val := range values {
		v := make([]float64, len(val))
		for i, ev := range val {
			v[i] = fn(ev)
		}

		expected = append(expected, v)
	}
	return expected
}

func TestAbsWithAllValues(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	values[0][0] = -values[0][0]
	values[1][1] = -values[1][1]

	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	mathOp, err := NewMathOp(AbsType)
	require.NoError(t, err)

	op, ok := mathOp.(transform.Params)
	require.True(t, ok)
	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedMathVals(values, math.Abs)
	assert.Len(t, sink.Values, 2)
	assert.Equal(t, expected, sink.Values)
}

func TestAbsWithSomeValues(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	mathOp, err := NewMathOp(AbsType)
	require.NoError(t, err)

	op, ok := mathOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedMathVals(values, math.Abs)
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestLn(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)

	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	mathOp, err := NewMathOp(LnType)
	require.NoError(t, err)

	op, ok := mathOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedMathVals(values, math.Log)
	assert.Len(t, sink.Values, 2)
	assert.Equal(t, expected, sink.Values)
}

func TestLog10WithNoValues(t *testing.T) {
	v := [][]float64{
		{nan, nan, nan, nan, nan},
		{nan, nan, nan, nan, nan},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	mathOp, err := NewMathOp(Log10Type)
	require.NoError(t, err)

	op, ok := mathOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedMathVals(values, math.Log10)
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestLog2WithSomeValues(t *testing.T) {
	v := [][]float64{
		{nan, 1, 2, 3, 3},
		{nan, 4, 5, 6, 6},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	mathOp, err := NewMathOp(Log2Type)
	require.NoError(t, err)

	op, ok := mathOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedMathVals(values, math.Log2)
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestFloorWithSomeValues(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2.2, 3.3, 4},
		{math.NaN(), 6, 7.77, 8, 9.9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	mathOp, err := NewMathOp(FloorType)
	require.NoError(t, err)

	op, ok := mathOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedMathVals(values, math.Floor)
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestCeilWithSomeValues(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2.2, 3.3, 4},
		{math.NaN(), 6, 7.77, 8, 9.9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	mathOp, err := NewMathOp(CeilType)
	require.NoError(t, err)

	op, ok := mathOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedMathVals(values, math.Ceil)
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}
func TestExpWithSomeValues(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	mathOp, err := NewMathOp(ExpType)
	require.NoError(t, err)

	op, ok := mathOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedMathVals(values, math.Exp)
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestSqrtWithSomeValues(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	mathOp, err := NewMathOp(SqrtType)
	require.NoError(t, err)

	op, ok := mathOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedMathVals(values, math.Sqrt)
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestNonExistentFunc(t *testing.T) {
	_, err := NewMathOp("nonexistent_func")
	require.Error(t, err)
}
