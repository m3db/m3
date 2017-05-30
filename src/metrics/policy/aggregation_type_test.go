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

	"github.com/stretchr/testify/require"
)

func TestAggregationTypeIsValid(t *testing.T) {
	require.True(t, P9999.IsValid())
	require.False(t, AggregationType(int(P9999)+1).IsValid())
}

func TestAggregationTypesIsDefault(t *testing.T) {
	require.True(t, DefaultAggregationTypes.IsDefault())

	require.False(t, AggregationTypes{Upper}.IsDefault())
}

func TestCompressedAggregationTypesIsDefault(t *testing.T) {
	var id AggregationID
	require.True(t, id.IsDefault())

	id[0] = 8
	require.False(t, id.IsDefault())

	id[0] = 0
	require.True(t, id.IsDefault())
}

func TestParseParseAggregationTypes(t *testing.T) {
	inputs := []struct {
		str      string
		expected AggregationTypes
	}{
		{
			str:      "Lower",
			expected: AggregationTypes{Lower},
		},
		{
			str:      "Lower,Upper",
			expected: AggregationTypes{Lower, Upper},
		},
	}
	for _, input := range inputs {
		res, err := ParseAggregationTypes(input.str)
		require.NoError(t, err)
		require.Equal(t, input.expected, res)
	}
}

func TestCompressedAggregationTypesMerge(t *testing.T) {
	testcases := []struct {
		input  AggregationID
		other  AggregationID
		result AggregationID
		merged bool
	}{
		{DefaultAggregationID, DefaultAggregationID, DefaultAggregationID, false},
		{mustCompress(Mean), DefaultAggregationID, mustCompress(Mean), false},
		{DefaultAggregationID, mustCompress(Mean), mustCompress(Mean), true},
		{mustCompress(Lower), mustCompress(Upper), mustCompress(Lower, Upper), true},
		{mustCompress(Lower), mustCompress(Lower, Upper), mustCompress(Lower, Upper), true},
		{mustCompress(Lower, Upper), mustCompress(Lower), mustCompress(Lower, Upper), false},
	}

	for _, test := range testcases {
		res, merged := test.input.Merge(test.other)
		require.Equal(t, test.result, res)
		require.Equal(t, test.merged, merged)
	}
}

func mustCompress(aggTypes ...AggregationType) AggregationID {
	res, err := NewAggregationIDCompressor().Compress(aggTypes)
	if err != nil {
		panic(err.Error())
	}
	return res
}
