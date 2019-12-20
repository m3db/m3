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

var temporalFunctionTestCases = []testCase{
	{
		name:   "resets",
		opType: ResetsType,
		vals: [][]float64{
			{nan, 0, 2, nan, 1, 1, 0, 2, nan, 1},
			{6, 4, 4, 2, 5, 6, 4, 4, 2, 5},
		},
		expected: [][]float64{
			{nan, 0, 0, 0, 1, 1, 2, 1, 1, 2},
			{nan, 1, 1, 2, 2, 1, 2, 1, 2, 2},
		},
	},
	{
		name:   "changes",
		opType: ChangesType,
		vals: [][]float64{
			{nan, 0, 2, nan, 1, 1, 0, 2, nan, 1},
			{6, 4, 4, 2, 5, 6, 4, 4, 2, 5},
		},
		expected: [][]float64{
			{nan, 0, 1, 1, 2, 2, 2, 2, 2, 3},
			{nan, 1, 1, 2, 3, 3, 4, 3, 3, 3},
		},
	},
	{
		name:   "resets all NaNs",
		opType: ResetsType,
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
		name:   "changes all NaNs",
		opType: ChangesType,
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
		name:   "resets first and last NaN",
		opType: ResetsType,
		vals: [][]float64{
			{nan, 0, 2, 1, nan, nan, 0, 2, 1, nan},
			{nan, 4, 4, 2, nan, nan, 4, 4, 2, nan},
		},
		expected: [][]float64{
			{nan, 0, 0, 1, 1, 1, 2, 1, 1, 1},
			{nan, 0, 0, 1, 1, 1, 1, 0, 1, 1},
		},
	},
	{
		name:   "changes first and last NaN",
		opType: ChangesType,
		vals: [][]float64{
			{nan, 0, 2, 5, nan, nan, 0, 2, 5, nan},
			{nan, 4, 4, 2, nan, nan, 4, 4, 2, nan},
		},
		expected: [][]float64{
			{nan, 0, 1, 2, 2, 2, 2, 2, 2, 2},
			{nan, 0, 0, 1, 1, 1, 2, 1, 1, 1},
		},
	},
}

func TestTemporalFunctionProcess(t *testing.T) {
	opGen := func(t *testing.T, tc testCase) transform.Params {
		op, err := NewFunctionOp([]interface{}{5 * time.Minute}, tc.opType)
		require.NoError(t, err)
		return op
	}

	testTemporalFunc(t, opGen, temporalFunctionTestCases)
}

func TestUnknownFunction(t *testing.T) {
	_, err := NewFunctionOp([]interface{}{5 * time.Minute}, "unknown_func")
	require.Error(t, err)
}
