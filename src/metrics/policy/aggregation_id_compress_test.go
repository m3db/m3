// Copyright (c) 2017 Uber Technologies, Inc.
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

package policy

import (
	"testing"

	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func TestAggregationIDCompressRoundTrip(t *testing.T) {
	testcases := []struct {
		input     AggregationTypes
		result    AggregationTypes
		expectErr bool
	}{
		{DefaultAggregationTypes, AggregationTypes{}, false},
		{[]AggregationType{Unknown}, DefaultAggregationTypes, true},
		{[]AggregationType{Lower, Upper}, []AggregationType{Lower, Upper}, false},
		{[]AggregationType{Last}, []AggregationType{Last}, false},
		{[]AggregationType{P999, P9999}, []AggregationType{P999, P9999}, false},
		{[]AggregationType{1, 5, 9, 3, 2}, []AggregationType{1, 2, 3, 5, 9}, false},
		// 20 is an Unknown aggregation type.
		{[]AggregationType{10, 20}, DefaultAggregationTypes, true},
	}

	p := NewAggregationTypesPool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init(func() AggregationTypes {
		return make(AggregationTypes, 0, totalAggregationTypes)
	})
	compressor, decompressor := NewAggregationIDCompressor(), NewPooledAggregationIDDecompressor(p)
	for _, test := range testcases {
		codes, err := compressor.Compress(test.input)
		if test.expectErr {
			require.Error(t, err)
			continue
		}
		res, err := decompressor.Decompress(codes)
		require.NoError(t, err)
		require.Equal(t, test.result, res)
	}
}

func TestAggregationIDDecompressError(t *testing.T) {
	compressor, decompressor := NewAggregationIDCompressor(), NewAggregationIDDecompressor()
	_, err := decompressor.Decompress([AggregationIDLen]uint64{1}) // aggregation type: Unknown.
	require.Error(t, err)

	max, err := compressor.Compress([]AggregationType{Last, Lower, Upper, Mean, Median, Count, Sum, SumSq, Stdev, P95, P99, P999, P9999})
	require.NoError(t, err)

	max[0] = max[0] << 1
	_, err = decompressor.Decompress(max)
	require.Error(t, err)
}
