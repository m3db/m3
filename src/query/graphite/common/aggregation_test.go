// Copyright (c) 2019 Uber Technologies, Inc.
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

package common

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/require"
)

func TestRangeOfSeries(t *testing.T) {
	ctx, input := NewConsolidationTestSeries(consolidationStartTime, consolidationEndTime, 30*time.Second)
	defer ctx.Close()

	in := ts.SeriesList{Values: input}

	expectedStart := ctx.StartTime.Add(-30 * time.Second)
	expectedStep := 10000
	rangeSeries, err := Range(ctx, in, func(series ts.SeriesList) string {
		return "woot"
	})
	require.Nil(t, err)

	expected := TestSeries{
		Name: "woot",
		Data: []float64{0, 0, 0, 12, 12, 12, 14, 14, 14, 0, 0, 0},
	}

	CompareOutputsAndExpected(t, expectedStep, expectedStart, []TestSeries{expected}, []*ts.Series{rangeSeries})
}

func TestAggregationFunctions(t *testing.T) {
	type input struct {
		functionName string
		values       []float64
	}

	type output struct {
		aggregatedValue float64
		nans            int
	}

	tests := []struct {
		input  input
		output output
	}{
		{
			input: input{
				"sum",
				[]float64{1, 2, 3, math.NaN()},
			},
			output: output{
				6,
				1,
			},
		},
		{
			input: input{
				"avg",
				[]float64{1, 2, 3, math.NaN()},
			},
			output: output{
				2,
				1,
			},
		},
		{
			input: input{
				"max",
				[]float64{1, 2, 3, math.NaN()},
			},
			output: output{
				3,
				1,
			},
		},
		{
			input: input{
				"min",
				[]float64{1, 2, 3, math.NaN()},
			},
			output: output{
				1,
				1,
			},
		},
		{
			input: input{
				"median",
				[]float64{1, 2, 3, math.NaN()},
			},
			output: output{
				2,
				1,
			},
		},
		{
			input: input{
				"diff",
				[]float64{1, 2, 3, math.NaN()},
			},
			output: output{
				-4,
				1,
			},
		},
		{
			input: input{
				"stddev",
				[]float64{1, 2, 3, math.NaN()},
			},
			output: output{
				math.Sqrt(float64(2) / float64(3)),
				1,
			},
		},
		{
			input: input{
				"range",
				[]float64{1, 2, 3, math.NaN()},
			},
			output: output{
				2,
				1,
			},
		},
		{
			input: input{
				"multiply",
				[]float64{1, 2, 3, math.NaN()},
			},
			output: output{
				1 * 2 * 3,
				1,
			},
		},
		{
			input: input{
				"last",
				[]float64{1, 2, 3, math.NaN()},
			},
			output: output{
				3,
				1,
			},
		},
	}

	for _, test := range tests {
		safeAggFn, ok := SafeAggregationFns[test.input.functionName]
		require.True(t, ok)
		aggregatedValue, nans, ok := safeAggFn(test.input.values)
		require.True(t, ok)
		require.Equal(t, test.output.aggregatedValue, aggregatedValue, fmt.Sprintf("aggregation result for %v should be equal", test.input.functionName))
		require.Equal(t, test.output.nans, nans)
	}
}
