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

package tile

import (
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestDownsampleCounterResets(t *testing.T) {
	tests := []struct {
		name        string
		givenValues []float64
		wantIndices []int
		wantValues  []float64
	}{
		{
			name:        "empty",
			givenValues: []float64{},
			wantIndices: []int{},
			wantValues:  []float64{},
		},
		{
			name:        "one value",
			givenValues: []float64{3.14},
			wantIndices: []int{0},
			wantValues:  []float64{3.14},
		},
		{
			name:        "two different values",
			givenValues: []float64{3, 5},
			wantIndices: []int{0, 1},
			wantValues:  []float64{3, 5},
		},
		{
			name:        "two identical values",
			givenValues: []float64{5, 5},
			wantIndices: []int{1},
			wantValues:  []float64{5},
		},
		{
			name:        "two values with reset",
			givenValues: []float64{3, 1},
			wantIndices: []int{0, 1},
			wantValues:  []float64{3, 1},
		},
		{
			name:        "second value equal to first, then reset",
			givenValues: []float64{2, 2, 1},
			wantIndices: []int{0, 2},
			wantValues:  []float64{2, 1},
		},
		{
			name:        "three values, no reset",
			givenValues: []float64{3, 4, 5},
			wantIndices: []int{0, 2},
			wantValues:  []float64{3, 5},
		},
		{
			name:        "three identical values",
			givenValues: []float64{5, 5, 5},
			wantIndices: []int{2},
			wantValues:  []float64{5},
		},
		{
			name:        "three values, reset after first",
			givenValues: []float64{3, 1, 5},
			wantIndices: []int{0, 1, 2},
			wantValues:  []float64{3, 1, 5}},
		{
			name:        "three values, reset after second",
			givenValues: []float64{3, 5, 1},
			wantIndices: []int{0, 1, 2},
			wantValues:  []float64{3, 5, 1},
		},
		{
			name:        "three values, two resets",
			givenValues: []float64{5, 3, 2},
			wantIndices: []int{0, 1, 2},
			wantValues:  []float64{5, 8, 2},
		},
		{
			name:        "four values, reset after first",
			givenValues: []float64{3, 1, 4, 5},
			wantIndices: []int{0, 1, 3},
			wantValues:  []float64{3, 1, 5},
		},
		{
			name:        "four values, reset after second (A)",
			givenValues: []float64{3, 4, 1, 5},
			wantIndices: []int{0, 1, 2, 3},
			wantValues:  []float64{3, 4, 1, 5},
		},
		{
			name:        "four values, reset after second (B)",
			givenValues: []float64{3, 4, 1, 4},
			wantIndices: []int{0, 1, 2, 3},
			wantValues:  []float64{3, 4, 1, 4},
		},
		{
			name:        "four values, reset after second (C)",
			givenValues: []float64{3, 4, 1, 2},
			wantIndices: []int{0, 1, 3},
			wantValues:  []float64{3, 4, 2},
		},
		{
			name:        "four values, reset after third",
			givenValues: []float64{3, 4, 5, 1},
			wantIndices: []int{0, 2, 3},
			wantValues:  []float64{3, 5, 1},
		},
		{
			name:        "four values, two resets (A)",
			givenValues: []float64{3, 1, 5, 4},
			wantIndices: []int{0, 2, 3},
			wantValues:  []float64{3, 8, 4},
		},
		{
			name:        "four values, two resets (B)",
			givenValues: []float64{5, 2, 1, 4},
			wantIndices: []int{0, 1, 3},
			wantValues:  []float64{5, 7, 4},
		},
		{
			name:        "four values, two resets (C)",
			givenValues: []float64{5, 2, 2, 1},
			wantIndices: []int{0, 2, 3},
			wantValues:  []float64{5, 7, 1},
		},
		{
			name:        "four values, two resets (D)",
			givenValues: []float64{3, 5, 2, 1},
			wantIndices: []int{0, 2, 3},
			wantValues:  []float64{3, 7, 1},
		},
		{
			name:        "reset between two identical values",
			givenValues: []float64{4, 3, 4},
			wantIndices: []int{0, 1, 2},
			wantValues:  []float64{4, 3, 4},
		},
		{
			name:        "four values, three resets",
			givenValues: []float64{9, 7, 4, 1},
			wantIndices: []int{0, 2, 3},
			wantValues:  []float64{9, 20, 1},
		},
		{
			name:        "five values, two resets (A)",
			givenValues: []float64{3, 1, 2, 5, 4},
			wantIndices: []int{0, 3, 4},
			wantValues:  []float64{3, 8, 4},
		},
		{
			name:        "five equal values",
			givenValues: []float64{1, 1, 1, 1, 1},
			wantIndices: []int{4},
			wantValues:  []float64{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			indices, results := downsample(math.NaN(), tt.givenValues)

			assert.Equal(t, tt.wantValues, results)
			assert.Equal(t, tt.wantIndices, indices)
		})
	}
}

func TestDownsampleCounterResetsWithPrevFrameLastValue(t *testing.T) {
	tests := []struct {
		name                    string
		givenPrevFrameLastValue float64
		givenValues             []float64
		wantIndices             []int
		wantValues              []float64
	}{
		{
			name:                    "empty",
			givenPrevFrameLastValue: 3,
			givenValues:             []float64{},
			wantIndices:             []int{},
			wantValues:              []float64{},
		},
		{
			name:                    "one value equal to prev frame last",
			givenPrevFrameLastValue: 2,
			givenValues:             []float64{2},
			wantIndices:             []int{0},
			wantValues:              []float64{2},
		},
		{
			name:                    "one value less than prev frame last",
			givenPrevFrameLastValue: 3,
			givenValues:             []float64{2},
			wantIndices:             []int{0},
			wantValues:              []float64{2},
		},
		{
			name:                    "one value more than prev frame last",
			givenPrevFrameLastValue: 3,
			givenValues:             []float64{4},
			wantIndices:             []int{0},
			wantValues:              []float64{4},
		},
		{
			name:                    "two values, increasing",
			givenPrevFrameLastValue: 3,
			givenValues:             []float64{4, 5},
			wantIndices:             []int{1},
			wantValues:              []float64{5},
		},
		{
			name:                    "reset between frames",
			givenPrevFrameLastValue: 3,
			givenValues:             []float64{2, 5},
			wantIndices:             []int{0, 1},
			wantValues:              []float64{2, 5},
		},
		{
			name:                    "reset between frames and within frame",
			givenPrevFrameLastValue: 4,
			givenValues:             []float64{2, 1},
			wantIndices:             []int{0, 1},
			wantValues:              []float64{2, 1},
		},
		{
			name:                    "reset within frame",
			givenPrevFrameLastValue: 1,
			givenValues:             []float64{4, 3},
			wantIndices:             []int{0, 1},
			wantValues:              []float64{4, 3},
		},
		{
			name:                    "all equal",
			givenPrevFrameLastValue: 1,
			givenValues:             []float64{1, 1},
			wantIndices:             []int{1},
			wantValues:              []float64{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			indices, results := downsample(tt.givenPrevFrameLastValue, tt.givenValues)

			assert.Equal(t, tt.wantValues, results)
			assert.Equal(t, tt.wantIndices, indices)
		})
	}
}

func TestDownsampleCounterResetsInvariants(t *testing.T) {
	testDownsampleCounterResetsInvariants(t, false)
}

func TestDownsampleCounterResetsInvariantsWithPrevFrameLastValue(t *testing.T) {
	testDownsampleCounterResetsInvariants(t, true)
}

func testDownsampleCounterResetsInvariants(t *testing.T, usePrevFrameLastValue bool) {
	params := gopter.DefaultTestParameters()
	params.MinSize = 1
	if usePrevFrameLastValue {
		params.MinSize++
	}
	params.MinSuccessfulTests = 10000
	params.MaxShrinkCount = 0
	properties := gopter.NewProperties(params)
	generator := gen.SliceOf(gen.Float64Range(0, 100))

	epsilon := 0.00001

	properties.Property("return consistent indices", prop.ForAll(
		func(v []float64) bool {
			indices, results := downsampleFromSlice(v, usePrevFrameLastValue)

			if len(indices) == 0 || len(indices) != len(results) {
				return false
			}

			for i := 1; i < len(indices); i++ {
				if indices[i] <= indices[i-1] {
					return false
				}
			}

			return true
		},
		generator,
	))

	properties.Property("shrink the result", prop.ForAll(
		func(v []float64) bool {
			maxResultLength := len(v)
			if maxResultLength > 4 {
				maxResultLength = 4
			}

			_, results := downsampleFromSlice(v, usePrevFrameLastValue)

			return len(results) <= maxResultLength
		},
		generator,
	))

	properties.Property("preserve the last value", prop.ForAll(
		func(v []float64) bool {
			indices, results := downsampleFromSlice(v, usePrevFrameLastValue)

			lastIndex := len(v) - 1
			if usePrevFrameLastValue {
				lastIndex--
			}

			return results[len(results)-1] == v[len(v)-1] &&
				indices[len(indices)-1] == lastIndex
		},
		generator,
	))

	properties.Property("preserve values after adjusting for counter resets", prop.ForAll(
		func(v []float64) bool {
			indices, results := downsampleFromSlice(v, usePrevFrameLastValue)
			if usePrevFrameLastValue {
				results = append([]float64{v[0]}, results...)
			}

			adjustedInput := applyCounterResetAdjustment(v)
			adjustedResults := applyCounterResetAdjustment(results)

			indexOffset := 0
			if usePrevFrameLastValue {
				indexOffset = 1
			}

			for resultPos, inputPos := range indices {
				if math.Abs(adjustedResults[resultPos+indexOffset]-adjustedInput[inputPos+indexOffset]) > epsilon {
					return false
				}
			}

			return true
		},
		generator,
	))

	properties.TestingRun(t)
}

func downsample(prevFrameLastValue float64, vals []float64) ([]int, []float64) {
	frame := SeriesBlockFrame{record: &record{vals: vals}}
	indices := make([]int, 0)
	results := make([]float64, 0)

	DownsampleCounterResets(prevFrameLastValue, frame, &indices, &results)

	return indices, results
}

func downsampleFromSlice(vals []float64, usePrevFrameLastValue bool) ([]int, []float64) {
	prevFrameLastValue := math.NaN()

	if usePrevFrameLastValue {
		prevFrameLastValue = vals[0]
		vals = vals[1:]
	}

	return downsample(prevFrameLastValue, vals)
}

func applyCounterResetAdjustment(vals []float64) []float64 {
	transformed := make([]float64, 0, len(vals))
	if len(vals) == 0 {
		return transformed
	}

	previous := vals[0]
	accumulated := previous
	transformed = append(transformed, previous)

	for i := 1; i < len(vals); i++ {
		current := vals[i]
		delta := current - previous
		if delta >= 0 {
			accumulated += delta
		} else { // a reset
			accumulated += current
		}
		transformed = append(transformed, accumulated)
		previous = current
	}

	return transformed
}
