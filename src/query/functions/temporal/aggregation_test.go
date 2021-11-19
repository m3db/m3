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
	"testing"
	"time"

	"github.com/m3db/m3/src/query/executor/transform"

	"github.com/stretchr/testify/require"
)

var aggregationTestCases = []testCase{
	{
		name:   "avg_over_time",
		opType: AvgType,
		vals: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 9},
		},
		expected: [][]float64{
			{nan, 1, 1.5, 2, 2.5, 2, 2, 2, 2, 2},
			{5, 5.5, 6, 6.5, 7, 7, 7, 7, 7, 7},
		},
	},
	{
		name:   "avg_over_time with warning",
		opType: AvgType,
		vals: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 9},
		},
		expected: [][]float64{
			{nan, 1, 1.5, 2, 2.5, 2, 2, 2, 2, 2},
			{5, 5.5, 6, 6.5, 7, 7, 7, 7, 7, 7},
		},
		withWarning: true,
	},
	{
		name:   "avg_over_time all NaNs",
		opType: AvgType,
		vals: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
		expected: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
	},
	{
		name:   "count_over_time",
		opType: CountType,
		vals: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 9},
		},
		expected: [][]float64{
			{nan, 1, 2, 3, 4, 5, 5, 5, 5, 5},
			{1, 2, 3, 4, 5, 5, 5, 5, 5, 5},
		},
	},
	{
		name:   "count_over_time all NaNs",
		opType: CountType,
		vals: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
		expected: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
	},
	{
		name:   "min_over_time",
		opType: MinType,
		vals: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 1},
		},
		expected: [][]float64{
			{nan, 1, 1, 1, 1, 0, 0, 0, 0, 0},
			{5, 5, 5, 5, 5, 5, 5, 5, 5, 1},
		},
	},
	{
		name:   "min_over_time all NaNs",
		opType: MinType,
		vals: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
		expected: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
	},
	{
		name:   "max_over_time",
		opType: MaxType,
		vals: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 111},
		},
		expected: [][]float64{
			{nan, 1, 2, 3, 4, 4, 4, 4, 4, 4},
			{5, 6, 7, 8, 9, 9, 9, 9, 9, 111},
		},
	},
	{
		name:   "max_over_time all NaNs",
		opType: MaxType,
		vals: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
		expected: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
	},
	{
		name:   "sum_over_time",
		opType: SumType,
		vals: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 9},
		},
		expected: [][]float64{
			{nan, 1, 3, 6, 10, 10, 10, 10, 10, 10},
			{5, 11, 18, 26, 35, 35, 35, 35, 35, 35},
		},
	},
	{
		name:   "sum_over_time all NaNs",
		opType: SumType,
		vals: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
		expected: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
	},
	{
		name:   "stddev_over_time",
		opType: StdDevType,
		vals: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 9},
		},
		expected: [][]float64{
			{nan, nan, 0.5, 0.81649, 1.1180,
				1.4142, 1.4142, 1.4142, 1.4142, 1.4142},
			{nan, 0.5, 0.81649, 1.11803, 1.4142,
				1.4142, 1.4142, 1.4142, 1.4142, 1.4142},
		},
	},
	{
		name:   "stddev_over_time all NaNs",
		opType: StdDevType,
		vals: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
		expected: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
	},
	{
		name:   "stdvar_over_time",
		opType: StdVarType,
		vals: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 9},
		},
		expected: [][]float64{
			{nan, nan, 0.25, 0.666666, 1.25, 2, 2, 2, 2, 2},
			{nan, 0.25, 0.66666, 1.25, 2, 2, 2, 2, 2, 2},
		},
	},
	{
		name:   "stdvar_over_time all NaNs",
		opType: StdVarType,
		vals: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
		expected: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
	},
	{
		name:   "last_over_time",
		opType: LastType,
		vals: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 9},
		},
		expected: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 9},
		},
	},
	{
		name:   "last_over_time leading NaNs",
		opType: LastType,
		vals: [][]float64{
			{nan, 1, nan, 3, nan, nan, 2, nan, nan, nan},
			{5, nan, nan, nan, nan, nan, nan, 7, nan, nan},
		},
		expected: [][]float64{
			{nan, 1, nan, 3, nan, nan, 2, nan, nan, nan},
			{5, nan, nan, nan, nan, nan, nan, 7, nan, nan},
		},
	},
	{
		name:   "last_over_time all NaNs",
		opType: LastType,
		vals: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
		expected: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
	},
	{
		name:   "quantile_over_time",
		opType: QuantileType,
		vals: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 9},
		},
		expected: [][]float64{
			{nan, 1, 1.2, 1.4, 1.6, 0.8, 0.8, 0.8, 0.8, 0.8},
			{5, 5.2, 5.4, 5.6, 5.8, 5.8, 5.8, 5.8, 5.8, 5.8},
		},
	},
	{
		name:   "quantile_over_time all NaNs",
		opType: QuantileType,
		vals: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
		expected: [][]float64{
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
	},
}

func TestAggregation(t *testing.T) {
	opGen := func(t *testing.T, tc testCase) transform.Params {
		if tc.opType == QuantileType {
			args := []interface{}{0.2, 5 * time.Minute}
			baseOp, err := NewQuantileOp(args, tc.opType)
			require.NoError(t, err)

			return baseOp
		}

		args := []interface{}{5 * time.Minute}
		baseOp, err := NewAggOp(args, tc.opType)
		require.NoError(t, err)
		return baseOp
	}

	testTemporalFunc(t, opGen, aggregationTestCases)
}

func TestUnknownAggregation(t *testing.T) {
	_, err := NewAggOp([]interface{}{5 * time.Minute}, "unknown_agg_func")
	require.Error(t, err)
}
