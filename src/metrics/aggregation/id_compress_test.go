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

package aggregation

import (
	"testing"

	"github.com/m3db/m3/src/x/pool"

	"github.com/stretchr/testify/require"
)

func TestIDCompressRoundTrip(t *testing.T) {
	testcases := []struct {
		input     Types
		result    Types
		expectErr bool
	}{
		{DefaultTypes, DefaultTypes, false},
		{[]Type{UnknownType}, DefaultTypes, true},
		{[]Type{Min, Max}, []Type{Min, Max}, false},
		{[]Type{Last}, []Type{Last}, false},
		{[]Type{P999, P9999}, []Type{P999, P9999}, false},
		{[]Type{1, 5, 9, 3, 2}, []Type{1, 2, 3, 5, 9}, false},
		// 50 is an unknown aggregation type.
		{[]Type{10, 50}, DefaultTypes, true},
	}

	p := NewTypesPool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init(func() Types {
		return make(Types, 0, maxTypeID)
	})
	compressor, decompressor := NewIDCompressor(), NewPooledIDDecompressor(p)
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

func TestIDDecompressError(t *testing.T) {
	compressor, decompressor := NewIDCompressor(), NewIDDecompressor()
	_, err := decompressor.Decompress([IDLen]uint64{1})
	require.Error(t, err)

	max, err := compressor.Compress(
		[]Type{Last, Min, Max, Mean, Median, Count, Sum, SumSq, Stdev, P95, P99, P999, P9999, P25, P75})
	require.NoError(t, err)

	max[0] = max[0] << 1
	_, err = decompressor.Decompress(max)
	require.Error(t, err)
}

func TestIDMustDecompress(t *testing.T) {
	compressor, decompressor := NewIDCompressor(), NewIDDecompressor()
	inputs := []struct {
		id          ID
		shouldPanic bool
	}{
		{
			id:          compressor.MustCompress([]Type{Last, Min, Max, Mean, Median, Count, Sum, SumSq}),
			shouldPanic: false,
		},
		{
			id:          [IDLen]uint64{1},
			shouldPanic: true,
		},
	}

	for _, input := range inputs {
		if input.shouldPanic {
			require.Panics(t, func() { decompressor.MustDecompress(input.id) })
		} else {
			require.NotPanics(t, func() { decompressor.MustDecompress(input.id) })
		}
	}

}
