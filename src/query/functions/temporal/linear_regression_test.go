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

var linearRegressionTestCases = []testCase{
	{
		name:   "predict_linear",
		opType: PredictLinearType,
		vals: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 9},
		},
		expected: [][]float64{
			{nan, nan, 3.6666, 4.6666, 5.6666, 2, 0.1666, 0.1666, 2, 5.6666},
			{nan, 7.6666, 8.6666, 9.6666, 10.6666, 7, 5.1666, 5.1666, 7, 10.6666},
		},
	},
	{
		name:   "deriv",
		opType: DerivType,
		vals: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 9},
		},
		expected: [][]float64{
			{nan, nan, 0.0166, 0.0166, 0.0166, 0, -0.0083, -0.0083, 0, 0.0166},
			{nan, 0.0166, 0.0166, 0.0166, 0.0166, 0, -0.0083, -0.0083, 0, 0.0166},
		},
	},
	{
		name:   "predict_linear some NaNs",
		opType: PredictLinearType,
		vals: [][]float64{
			{nan, 1, 2, 3, nan, nan, 1, 2, 3, nan},
			{5, 6, nan, 8, 9, 5, 6, nan, 8, 9},
		},
		expected: [][]float64{
			{nan, nan, 3.6666, 4.6666, 5.6666, 6.6666, 0.6153, 0.8461, 4.6666, 5.6666},
			{nan, 7.6666, 8.6666, 9.6666, 10.6666, 7, 3.8333, 2.8333, 7, 10.6666},
		},
	},
	{
		name:   "deriv some NaNs",
		opType: DerivType,
		vals: [][]float64{
			{nan, 1, 2, 3, nan, nan, 1, 2, 3, nan},
			{5, 6, nan, 8, 9, 5, 6, nan, 8, 9},
		},
		expected: [][]float64{
			{nan, nan, 0.0166, 0.0166, 0.0166, 0.0166, -0.0058, -0.0058, 0.0166, 0.0166},
			{nan, 0.0166, 0.0166, 0.0166, 0.0166, 0, -0.0166, -0.0166, 0, 0.0166},
		},
	},
	{
		name:   "predict_linear NaNs",
		opType: PredictLinearType,
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
		name:   "deriv NaNs",
		opType: DerivType,
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

func TestLinearRegressionBlocks(t *testing.T) {
	opGen := func(t *testing.T, tc testCase) transform.Params {
		if tc.opType == PredictLinearType {
			baseOp, err := NewLinearRegressionOp(
				[]interface{}{5 * time.Minute, 100.0}, tc.opType)
			require.NoError(t, err)
			return baseOp
		}

		baseOp, err := NewLinearRegressionOp(
			[]interface{}{5 * time.Minute}, tc.opType)
		require.NoError(t, err)
		return baseOp
	}

	testTemporalFunc(t, opGen, linearRegressionTestCases)
}

func TestUnknownLinearRegression(t *testing.T) {
	_, err := NewLinearRegressionOp(
		[]interface{}{5 * time.Minute},
		"unknown_linear_regression_func")
	require.Error(t, err)
}
