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

var rateTestCases = []testCase{
	{
		name:   "irate",
		opType: IRateType,
		vals: [][]float64{
			{nan, 680986, 683214, 685442, 687670,
				678758, 680986, 683214, 685442, 687670},
			{1987036, 1988988, 1990940, 1992892, 1994844,
				1987036, 1988988, 1990940, 1992892, 1994844},
		},
		expected: [][]float64{
			{nan, nan, 37.1333, 37.1333, 37.1333,
				11312.6333, 37.1333, 37.1333, 37.1333, 37.1333},
			{nan, 32.5333, 32.5333, 32.5333, 32.5333,
				33117.2666, 32.5333, 32.5333, 32.5333, 32.5333},
		},
	},
	{
		name:   "irate with some NaNs",
		opType: IRateType,
		vals: [][]float64{
			{nan, 1988988, 1990940, nan, 1994844,
				1987036, 1988988, 1990940, nan, 1994844},
			{1987036, 1988988, 1990940, nan, nan,
				1987036, 1988988, 1990940, nan, nan},
		},
		expected: [][]float64{
			{nan, nan, 32.5333, 32.5333, 32.5333,
				33117.2666, 32.5333, 32.5333, 32.5333, 32.5333},
			{nan, 32.5333, 32.5333, 32.5333, 32.5333,
				11039.0888, 32.5333, 32.5333, 32.5333, 32.5333},
		},
	},
	{
		name:   "irate with all NaNs",
		opType: IRateType,
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
		name:   "rate",
		opType: RateType,
		vals: [][]float64{
			{nan, 61943808, 61943808, 61943808, 62205952,
				61108224, 61943808, 61943808, 61943808, 62205952},
			{1987036, 1988988, 1990940, 1992892, 1994844,
				1987036, 1988988, 1990940, 1992892, 1994844},
		},
		expected: [][]float64{
			{nan, nan, 0, 0, 1019.44889,
				255709.8666, 259191.4666, 259191.4666, 258099.2, 4573.8666},
			{nan, 9.760000, 16.26666, 22.77333, 32.53333,
				8303.7166, 8303.7166, 8303.7166, 8303.7166, 32.53333},
		},
	},
	{
		name:   "rate with some NaNs",
		opType: RateType,
		vals: [][]float64{
			{nan, 61943808, 61943808, 62205952, nan,
				61108224, 61943808, 61943808, 62205952, nan},
			{1987036, 1988988, 1990940, nan, 1994844,
				1987036, 1988988, 1990940, nan, 1994844},
		},
		expected: [][]float64{
			{nan, nan, 0, 1092.266673, 1529.17334,
				255709.8666, 259191.4666, 258099.2, 4268.9422, 6098.48888},
			{nan, 9.760000, 16.26666, 22.77333, 32.533333,
				8303.7166, 8303.7166, 7742.5444, 11060.7777, 32.5333},
		},
	},
	{
		name:   "rate with all NaNs",
		opType: RateType,
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

func TestRate(t *testing.T) {
	testRate(t, rateTestCases)
}

var deltaTestCases = []testCase{
	{
		name:   "idelta",
		opType: IDeltaType,
		vals: [][]float64{
			{nan, 865910, 868138, 870366, 872594,
				863682, 865910, 868138, 870366, 872594},
			{1987036, 1988988, 1990940, 1992892, 1994844,
				1987036, 1988988, 1990940, 1992892, 1994844},
		},
		expected: [][]float64{
			{nan, nan, 2228, 2228, 2228,
				-8912, 2228, 2228, 2228, 2228},
			{nan, 1952, 1952, 1952, 1952,
				-7808, 1952, 1952, 1952, 1952},
		},
	},
	{
		name:   "idelta with some NaNs",
		opType: IDeltaType,
		vals: [][]float64{
			{nan, 1988988, 1990940, nan, 1994844,
				1987036, 1988988, 1990940, nan, 1994844},
			{1987036, 1988988, 1990940, nan, nan,
				1987036, 1988988, 1990940, nan, nan},
		},
		expected: [][]float64{
			{nan, nan, 1952, 1952, 3904,
				-7808, 1952, 1952, 1952, 3904},
			{nan, 1952, 1952, 1952, 1952,
				-3904, 1952, 1952, 1952, 1952},
		},
	},
	{
		name:   "idelta with all NaNs",
		opType: IDeltaType,
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
		name:   "delta",
		opType: DeltaType,
		vals: [][]float64{
			{nan, 680986, 683214, 685442, 687670,
				678758, 680986, 683214, 685442, 687670},
			{2299, 2299, 2299, 2787, 2787,
				2299, 2299, 2299, 2787, 2787},
		},
		expected: [][]float64{
			{nan, nan, 3342.000037, 5570.000037, 7798.00003,
				-2785, -2785, -2785, -2785, 11140},
			{nan, 0, 0, 569.33333, 610,
				0, 0, -610, 0, 610},
		},
	},
	{
		name:   "delta with some NaNs",
		opType: DeltaType,
		vals: [][]float64{
			{nan, 680986, 683214, 685442, nan,
				678758, 680986, 683214, 685442, nan},
			{2299, 2299, 2299, nan, 2787,
				2299, 2299, 2299, nan, 2787},
		},
		expected: [][]float64{
			{nan, nan, 3342.000037, 5570.000037, 7798.00003,
				-2785, -2785, -2785, 7798.000037, 11140},
			{nan, 0, 0, 0, 610,
				0, 0, -569.33333, -813.33333, 610},
		},
	},
	{
		name:   "delta with all NaNs",
		opType: DeltaType,
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

func TestDelta(t *testing.T) {
	testRate(t, deltaTestCases)
}

var increaseTestCases = []testCase{
	{
		name:   "increase",
		opType: IncreaseType,
		vals: [][]float64{
			{nan, 865910, 868138, 870366, 872594,
				872594, 865910, 868138, 870366, 872594},
			{1987036, 1988988, 1990940, 1992892, 1994844,
				1987036, 1988988, 1990940, 1992892, 1994844},
		},
		expected: [][]float64{
			{nan, nan, 3342, 5570, 7798,
				8355, 1087957.5, 1087957.5, 1087957.5, 1090742.5},
			{nan, 2928, 4880, 6832, 9760,
				2491115, 2491115, 2491115, 2491115, 9760},
		},
	},
	{
		name:   "increase with some NaNs",
		opType: IncreaseType,
		vals: [][]float64{
			{nan, 865910, 868138, 872694, nan,
				872594, 865910, 868138, 872694, nan},
			{1987036, 1988988, 1990940, nan, 1994844,
				1987036, 1988988, 1990940, nan, 1994844},
		},
		expected: [][]float64{
			{nan, nan, 3342.000037, 8480, 11872,
				1099222.5, 2178825, 2175915, 1018143.00484, 1454490},
			{nan, 2928, 4880, 6832, 9760,
				2491115, 2491115, 2322763.34439, 3318233.3333, 9760},
		},
	},
	{
		name:   "increase with all NaNs",
		opType: IncreaseType,
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

func TestIncrease(t *testing.T) {
	testRate(t, increaseTestCases)
}

func testRate(t *testing.T, testCases []testCase) {
	opGen := func(t *testing.T, tc testCase) transform.Params {
		op, err := NewRateOp([]interface{}{5 * time.Minute}, tc.opType)
		require.NoError(t, err)
		return op
	}

	testTemporalFunc(t, opGen, testCases)
}

func TestUnknownRate(t *testing.T) {
	_, err := NewRateOp([]interface{}{5 * time.Minute}, "unknown_rate_func")
	require.Error(t, err)
}
