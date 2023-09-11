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
	"testing"

	"github.com/stretchr/testify/assert"
)

type percentileTestParams struct {
	interpolate bool
	percentile  float64
	input       []float64
	expected    float64
}

func TestGetPercentile(t *testing.T) {
	tests := []percentileTestParams{
		{
			false,
			0,
			[]float64{1, 2, 3, 4, 5},
			1,
		},
		{
			false,
			10,
			[]float64{1, 2, 3, 4, 5},
			1,
		},
		{
			false,
			50,
			[]float64{1, 2, 3, 4, 5},
			3,
		},
		{
			true,
			50,
			[]float64{1, 2, 3, 4, 5},
			3,
		},
		{
			false,
			50,
			[]float64{1, 2, 3, 4, 5, 6},
			4,
		},
		{
			true,
			50,
			[]float64{1, 2, 3, 4, 5, 6},
			3.5,
		},
		{
			false,
			90,
			[]float64{1, 2, 3, 4, 5},
			5,
		},
		{
			false,
			50,
			[]float64{1},
			1,
		},
		{
			false,
			50,
			[]float64{1, 2},
			2,
		},
		{
			true,
			30,
			[]float64{32, 34, 62, 73, 75},
			33.6,
		},
		{
			true,
			33,
			[]float64{32, 34, 73, 75},
			33.3,
		},
	}

	for _, test := range tests {
		testGetPercentile(t, test)
	}
}

func testGetPercentile(t *testing.T, test percentileTestParams) {
	actual := GetPercentile(test.input, test.percentile, test.interpolate)
	assert.Equal(t, test.expected, actual)
}
