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

package datetime

import (
	"math"
	"testing"
	"time"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/parser"
	"github.com/m3db/m3db/src/coordinator/test"
	"github.com/m3db/m3db/src/coordinator/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func expectedDateVals(values [][]time.Time, opType string) [][]float64 {
	expected := make([][]float64, 0, len(values))
	for _, val := range values {
		v := make([]float64, len(val))
		for i, t := range val {
			v[i] = datetimeFuncs[opType](t)
		}
		expected = append(expected, v)
	}

	return expected
}

func getTimes(values [][]float64, bounds block.Bounds) [][]time.Time {
	times := make([][]time.Time, 0, len(values))
	for _, val := range values {
		v := make([]time.Time, len(val))
		for i := 0; i < len(val); i++ {
			v[i] = bounds.Start.Add(time.Duration(i) * bounds.StepSize)
		}
		times = append(times, v)
	}

	return times
}

func TestDayOfMonthType(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	times := getTimes(v, bounds)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	op, err := NewDateOp(DayOfMonthType)
	require.NoError(t, err)
	node := op.Node(c)
	err = node.Process(parser.NodeID(0), block)
	require.NoError(t, err)
	expected := expectedDateVals(times, op.operatorType)
	assert.Len(t, sink.Values, 2)
	test.EqualsWithNans(t, expected, sink.Values)
}

func TestDayOfWeekType(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	times := getTimes(values, bounds)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	op, err := NewDateOp(DayOfWeekType)
	require.NoError(t, err)
	node := op.Node(c)
	err = node.Process(parser.NodeID(0), block)
	require.NoError(t, err)
	expected := expectedDateVals(times, op.operatorType)
	assert.Len(t, sink.Values, 2)
	test.EqualsWithNans(t, expected, sink.Values)
}

func TestDaysInMonthType(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	times := getTimes(values, bounds)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	op, err := NewDateOp(DaysInMonthType)
	require.NoError(t, err)
	node := op.Node(c)
	err = node.Process(parser.NodeID(0), block)
	require.NoError(t, err)
	expected := expectedDateVals(times, op.operatorType)
	assert.Len(t, sink.Values, 2)
	test.EqualsWithNans(t, expected, sink.Values)
}

func TestHourType(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	times := getTimes(values, bounds)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	op, err := NewDateOp(HourType)
	require.NoError(t, err)
	node := op.Node(c)
	err = node.Process(parser.NodeID(0), block)
	require.NoError(t, err)
	expected := expectedDateVals(times, op.operatorType)
	assert.Len(t, sink.Values, 2)
	test.EqualsWithNans(t, expected, sink.Values)
}
func TestMinuteType(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	times := getTimes(values, bounds)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	op, err := NewDateOp(MinuteType)
	require.NoError(t, err)
	node := op.Node(c)
	err = node.Process(parser.NodeID(0), block)
	require.NoError(t, err)
	expected := expectedDateVals(times, op.operatorType)
	assert.Len(t, sink.Values, 2)
	test.EqualsWithNans(t, expected, sink.Values)
}

func TestMonthType(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	times := getTimes(values, bounds)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	op, err := NewDateOp(MonthType)
	require.NoError(t, err)
	node := op.Node(c)
	err = node.Process(parser.NodeID(0), block)
	require.NoError(t, err)
	expected := expectedDateVals(times, op.operatorType)
	assert.Len(t, sink.Values, 2)
	test.EqualsWithNans(t, expected, sink.Values)
}
func TestYearType(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	times := getTimes(values, bounds)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	op, err := NewDateOp(YearType)
	require.NoError(t, err)
	node := op.Node(c)
	err = node.Process(parser.NodeID(0), block)
	require.NoError(t, err)
	expected := expectedDateVals(times, op.operatorType)
	assert.Len(t, sink.Values, 2)
	test.EqualsWithNans(t, expected, sink.Values)
}

func TestTimeType(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	times := getTimes(values, bounds)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	op, err := NewDateOp(TimeType)
	require.NoError(t, err)
	node := op.Node(c)
	err = node.Process(parser.NodeID(0), block)
	require.NoError(t, err)
	expected := expectedDateVals(times, op.operatorType)
	assert.Len(t, sink.Values, 2)
	test.EqualsWithNans(t, expected, sink.Values)
}
