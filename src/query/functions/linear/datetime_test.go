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

var withArg = []interface{}{struct{}{}}

func expectedDateVals(values [][]float64, fn func(t time.Time) float64) [][]float64 {
	expected := make([][]float64, 0, len(values))
	for _, val := range values {
		v := make([]float64, len(val))
		for i, ev := range val {
			if math.IsNaN(ev) {
				v[i] = math.NaN()
				continue
			}
			t := time.Unix(int64(ev), 0).UTC()
			v[i] = fn(t)
		}

		expected = append(expected, v)
	}
	return expected
}

func TestDayOfMonth(t *testing.T) {
	v := [][]float64{
		{1493712846039, math.NaN(), 1493712846139, 1493712846239, 1493712846339},
		{math.NaN(), 1493712846439, 1493712846539, 1493712846639, 1493712846739},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	timeOp, err := NewDateOp(DayOfMonthType, true)
	require.NoError(t, err)

	op, ok := timeOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedDateVals(values, datetimeFuncs[DayOfMonthType])
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestDayOfWeek(t *testing.T) {
	v := [][]float64{
		{1493712846039, math.NaN(), 1493712846139, 1493712846239, 1493712846339},
		{math.NaN(), 1493712846439, 1493712846539, 1493712846639, 1493712846739},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	timeOp, err := NewDateOp(DayOfWeekType, true)
	require.NoError(t, err)

	op, ok := timeOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedDateVals(values, datetimeFuncs[DayOfWeekType])
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestDaysInMonth(t *testing.T) {
	v := [][]float64{
		{1493712846039, math.NaN(), 1493712846139, 1493712846239, 1493712846339},
		{math.NaN(), 1493712846439, 1493712846539, 1493712846639, 1493712846739},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	timeOp, err := NewDateOp(DaysInMonthType, true)
	require.NoError(t, err)

	op, ok := timeOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedDateVals(values, datetimeFuncs[DaysInMonthType])
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestHour(t *testing.T) {
	v := [][]float64{
		{1493712846039, math.NaN(), 1493712846139, 1493712846239, 1493712846339},
		{math.NaN(), 1493712846439, 1493712846539, 1493712846639, 1493712846739},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	timeOp, err := NewDateOp(HourType, true)
	require.NoError(t, err)

	op, ok := timeOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedDateVals(values, datetimeFuncs[HourType])
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestMinute(t *testing.T) {
	v := [][]float64{
		{1493712846039, math.NaN(), 1493712846139, 1493712846239, 1493712846339},
		{math.NaN(), 1493712846439, 1493712846539, 1493712846639, 1493712846739},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	timeOp, err := NewDateOp(MinuteType, true)
	require.NoError(t, err)

	op, ok := timeOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedDateVals(values, datetimeFuncs[MinuteType])
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestMonth(t *testing.T) {
	v := [][]float64{
		{1493712846039, math.NaN(), 1493712846139, 1493712846239, 1493712846339},
		{math.NaN(), 1493712846439, 1493712846539, 1493712846639, 1493712846739},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	timeOp, err := NewDateOp(MonthType, true)
	require.NoError(t, err)

	op, ok := timeOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedDateVals(values, datetimeFuncs[MonthType])
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestYear(t *testing.T) {
	v := [][]float64{
		{1493712846039, math.NaN(), 1493712846139, 1493712846239, 1493712846339},
		{math.NaN(), 1493712846439, 1493712846539, 1493712846639, 1493712846739},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	timeOp, err := NewDateOp(YearType, true)
	require.NoError(t, err)

	op, ok := timeOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	expected := expectedDateVals(values, datetimeFuncs[YearType])
	assert.Len(t, sink.Values, 2)
	compare.EqualsWithNans(t, expected, sink.Values)
}

func TestNonExistentDateFunc(t *testing.T) {
	_, err := NewDateOp("nonexistent_func", true)
	require.Error(t, err)
}

func TestWithoutArgs(t *testing.T) {
	v := [][]float64{
		{1493712846039, math.NaN(), 1493712846139, 1493712846239, 1493712846339},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	timeOp, err := NewDateOp(YearType, false)
	require.NoError(t, err)

	op, ok := timeOp.(transform.Params)
	require.True(t, ok)

	node := op.Node(c, transform.Options{})
	err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
	require.NoError(t, err)
	assert.Len(t, sink.Values, 1)
	ex := float64(time.Now().Year())
	assert.Equal(t, []float64{ex, ex, ex, ex, ex}, sink.Values[0])
}
