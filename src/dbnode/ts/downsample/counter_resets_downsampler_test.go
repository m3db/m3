// Copyright (c) 2020 Uber Technologies, Inc.
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

package downsample

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDownsampleCounterResets(t *testing.T) {
	tests := []struct {
		name  string
		given []float64
		want  []Value
	}{
		{
			name:  "empty",
			given: []float64{},
			want:  []Value{},
		},
		{
			name:  "one value",
			given: []float64{3.14},
			want:  []Value{{0, 3.14}},
		},
		{
			name:  "two different values",
			given: []float64{3, 5},
			want:  []Value{{0, 3}, {1, 5}},
		},
		{
			name:  "two identical values",
			given: []float64{5, 5},
			want:  []Value{{1, 5}},
		},
		{
			name:  "two values with reset",
			given: []float64{3, 1},
			want:  []Value{{0, 3}, {1, 1}},
		},
		{
			name:  "second value equal to first, then reset",
			given: []float64{2, 2, 1},
			want:  []Value{{0, 2}, {2, 1}},
		},
		{
			name:  "three values, no reset",
			given: []float64{3, 4, 5},
			want:  []Value{{0, 3}, {2, 5}},
		},
		{
			name:  "three identical values",
			given: []float64{5, 5, 5},
			want:  []Value{{2, 5}},
		},
		{
			name:  "three values, reset after first",
			given: []float64{3, 1, 5},
			want:  []Value{{0, 3}, {1, 1}, {2, 5}},
		},
		{
			name:  "three values, reset after second",
			given: []float64{3, 5, 1},
			want:  []Value{{0, 3}, {1, 5}, {2, 1}},
		},
		{
			name:  "three values, two resets",
			given: []float64{5, 3, 2},
			want:  []Value{{0, 5}, {1, 8}, {2, 2}},
		},
		{
			name:  "four values, reset after first",
			given: []float64{3, 1, 4, 5},
			want:  []Value{{0, 3}, {1, 1}, {3, 5}},
		},
		{
			name:  "four values, reset after second (A)",
			given: []float64{3, 4, 1, 5},
			want:  []Value{{0, 3}, {1, 4}, {2, 1}, {3, 5}},
		},
		{
			name:  "four values, reset after second (B)",
			given: []float64{3, 4, 1, 4},
			want:  []Value{{0, 3}, {1, 4}, {2, 1}, {3, 4}},
		},
		{
			name:  "four values, reset after second (C)",
			given: []float64{3, 4, 1, 2},
			want:  []Value{{0, 3}, {1, 4}, {3, 2}},
		},
		{
			name:  "four values, reset after third",
			given: []float64{3, 4, 5, 1},
			want:  []Value{{0, 3}, {2, 5}, {3, 1}},
		},
		{
			name:  "four values, two resets (A)",
			given: []float64{3, 1, 5, 4},
			want:  []Value{{0, 3}, {2, 8}, {3, 4}},
		},
		{
			name:  "four values, two resets (B)",
			given: []float64{5, 2, 1, 4},
			want:  []Value{{0, 5}, {1, 7}, {3, 4}},
		},
		{
			name:  "four values, two resets (C)",
			given: []float64{5, 2, 2, 1},
			want:  []Value{{0, 5}, {2, 7}, {3, 1}},
		},
		{
			name:  "four values, two resets (D)",
			given: []float64{3, 5, 2, 1},
			want:  []Value{{0, 3}, {2, 7}, {3, 1}},
		},
		{
			name:  "reset between two equal values",
			given: []float64{4, 3, 4},
			want:  []Value{{0, 4}, {1, 3}, {2, 4}},
		},
		{
			name:  "four values, three resets",
			given: []float64{9, 7, 4, 1},
			want:  []Value{{0, 9}, {2, 20}, {3, 1}},
		},
		{
			name:  "five values, two resets (A)",
			given: []float64{3, 1, 2, 5, 4},
			want:  []Value{{0, 3}, {3, 8}, {4, 4}},
		},
		{
			name:  "five equal values",
			given: []float64{1, 1, 1, 1, 1},
			want:  []Value{{4, 1}},
		},
		{
			name:  "five increasing values",
			given: []float64{1, 2, 3, 4, 5},
			want:  []Value{{0, 1}, {4, 5}},
		},
		{
			name:  "five decreasing values",
			given: []float64{5, 4, 3, 2, 1},
			want:  []Value{{0, 5}, {3, 5 + 4 + 3 + 2}, {4, 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := downsample(math.NaN(), tt.given)
			assert.Equal(t, tt.want, results)
		})
	}
}

func TestDownsampleCounterResetsWithPrevFrameLastValue(t *testing.T) {
	tests := []struct {
		name                    string
		givenPrevFrameLastValue float64
		givenFrame              []float64
		want                    []Value
	}{
		{
			name:                    "empty",
			givenPrevFrameLastValue: 3,
			givenFrame:              []float64{},
			want:                    []Value{},
		},
		{
			name:                    "one value equal to prev frame last",
			givenPrevFrameLastValue: 2,
			givenFrame:              []float64{2},
			want:                    []Value{{0, 2}},
		},
		{
			name:                    "one value less than prev frame last",
			givenPrevFrameLastValue: 3,
			givenFrame:              []float64{2},
			want:                    []Value{{0, 2}},
		},
		{
			name:                    "one value more than prev frame last",
			givenPrevFrameLastValue: 3,
			givenFrame:              []float64{4},
			want:                    []Value{{0, 4}},
		},
		{
			name:                    "two values, increasing",
			givenPrevFrameLastValue: 3,
			givenFrame:              []float64{4, 5},
			want:                    []Value{{1, 5}},
		},
		{
			name:                    "reset between frames",
			givenPrevFrameLastValue: 3,
			givenFrame:              []float64{2, 5},
			want:                    []Value{{0, 2}, {1, 5}},
		},
		{
			name:                    "reset between frames and within frame",
			givenPrevFrameLastValue: 4,
			givenFrame:              []float64{2, 1},
			want:                    []Value{{0, 2}, {1, 1}},
		},
		{
			name:                    "reset within frame",
			givenPrevFrameLastValue: 1,
			givenFrame:              []float64{4, 3},
			want:                    []Value{{0, 4}, {1, 3}},
		},
		{
			name:                    "all equal",
			givenPrevFrameLastValue: 1,
			givenFrame:              []float64{1, 1},
			want:                    []Value{{1, 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := downsample(tt.givenPrevFrameLastValue, tt.givenFrame)
			assert.Equal(t, tt.want, results)
		})
	}
}

func downsample(prevFrameLastValue float64, vals []float64) []Value {
	results := make([]Value, 0, 4)

	return DownsampleCounterResets(prevFrameLastValue, vals, results)
}
