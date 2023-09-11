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
	"time"

	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/compare"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nolint
func expectedClampVals(values [][]float64, num float64, fn func(x, y float64) float64) [][]float64 {
	expected := make([][]float64, 0, len(values))
	for _, val := range values {
		v := make([]float64, len(val))
		for i, ev := range val {
			v[i] = fn(ev, num)
		}

		expected = append(expected, v)
	}
	return expected
}

func TestClampMin(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	values[0][0] = math.NaN()

	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	clampOp, err := NewClampOp([]interface{}{3.0}, ClampMinType)
	require.NoError(t, err)

	op, ok := clampOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedClampVals(values, 3.0, math.Max)
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestClampMax(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	values[0][0] = math.NaN()

	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	clampOp, err := NewClampOp([]interface{}{3.0}, ClampMaxType)
	require.NoError(t, err)

	op, ok := clampOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedClampVals(values, 3.0, math.Min)
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestClampFailsParse(t *testing.T) {
	_, err := NewClampOp([]interface{}{}, "bad")
	assert.Error(t, err)
}

func runClamp(t *testing.T, args []interface{},
	opType string, vals []float64) []float64 {
	bounds := models.Bounds{
		StepSize: step,
		Duration: step * time.Duration(len(vals)),
	}

	v := [][]float64{vals}
	block := test.NewBlockFromValues(bounds, v)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	roundOp, err := NewClampOp(args, opType)
	require.NoError(t, err)

	op, ok := roundOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	require.Len(t, sink.Values, 1)

	return sink.Values[0]
}

func TestClampWithArgs(t *testing.T) {
	var (
		v     = []float64{math.NaN(), 0, 1, 2, 3, math.Inf(1), math.Inf(-1)}
		exMax = []float64{math.NaN(), 0, 1, 2, 2, 2, math.Inf(-1)}
		exMin = []float64{math.NaN(), 2, 2, 2, 3, math.Inf(1), 2}
	)

	max := runClamp(t, toArgs(2), ClampMaxType, v)
	compare.EqualsWithNans(t, exMax, max)

	min := runClamp(t, toArgs(2), ClampMinType, v)
	compare.EqualsWithNans(t, exMin, min)
}
