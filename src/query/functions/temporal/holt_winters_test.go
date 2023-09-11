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
	"github.com/m3db/m3/src/query/test/compare"

	"github.com/stretchr/testify/require"
)

var holtWintersTestCases = []testCase{
	{
		name: "holt_winters",
		vals: [][]float64{
			{nan, 1, 2, 3, 4, 0, 1, 2, 3, 4},
			{5, 6, 7, 8, 9, 5, 6, 7, 8, 9},
		},
		expected: [][]float64{
			{nan, nan, 2, 3, 4, 4, 3.64, 3.1824, -4.8224, 4},
			{nan, 6, 7, 8, 9, 9, 8.64, 8.1824, 0.1776, 9},
		},
	},
	{
		name: "holt_winters all NaNs",
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

func TestHoltWintersProcess(t *testing.T) {
	opGen := func(t *testing.T, _ testCase) transform.Params {
		op, err := NewHoltWintersOp([]interface{}{5 * time.Minute, 0.2, 0.7})
		require.NoError(t, err)
		return op
	}

	testTemporalFunc(t, opGen, holtWintersTestCases)
}

func TestHoltWintersFn(t *testing.T) {
	holtWintersFn := makeHoltWintersFn(0.2, 0.6)
	testData := []float64{nan, 1, nan, 5, 10, 15, nan, nan}
	val := holtWintersFn(testData)
	compare.EqualsWithNansWithDelta(t, 13.6559, val, 0.0001)
}
