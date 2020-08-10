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
		{"empty", []float64{}, nil, nil},
		{"one value", []float64{3.14}, []int{0}, []float64{3.14}},
		{"two different values", []float64{3, 5}, []int{0, 1}, []float64{3, 5}},
		{"two identical values", []float64{5, 5}, []int{1}, []float64{5}},
		{"two values with reset", []float64{3, 1}, []int{0, 1}, []float64{3, 1}},
		{"second value equal to first, then reset", []float64{2, 2, 1}, []int{0, 2}, []float64{2, 1}},
		{"three values, no reset", []float64{3, 4, 5}, []int{0, 2}, []float64{3, 5}},
		{"three identical values", []float64{5, 5, 5}, []int{2}, []float64{5}},
		{"three values, reset after first", []float64{3, 1, 5}, []int{0, 1, 2}, []float64{3, 1, 5}},
		{"three values, reset after second", []float64{3, 5, 1}, []int{0, 1, 2}, []float64{3, 5, 1}},
		{"three values, two resets", []float64{5, 3, 2}, []int{0, 1, 2}, []float64{5, 8, 2}},
		{"four values, reset after first", []float64{3, 1, 4, 5}, []int{0, 1, 3}, []float64{3, 1, 5}},
		{"four values, reset after second (A)", []float64{3, 4, 1, 5}, []int{0, 1, 2, 3}, []float64{3, 4, 1, 5}},
		{"four values, reset after second (B)", []float64{3, 4, 1, 4}, []int{0, 1, 2, 3}, []float64{3, 4, 1, 4}},
		{"four values, reset after second (C)", []float64{3, 4, 1, 2}, []int{0, 1, 3}, []float64{3, 4, 2}},
		{"four values, reset after third", []float64{3, 4, 5, 1}, []int{0, 2, 3}, []float64{3, 5, 1}},
		{"four values, two resets (A)", []float64{3, 1, 5, 4}, []int{0, 2, 3}, []float64{3, 8, 4}},
		{"four values, two resets (B)", []float64{5, 2, 1, 4}, []int{0, 1, 3}, []float64{5, 7, 4}},
		{"four values, two resets (C)", []float64{5, 2, 2, 1}, []int{0, 2, 3}, []float64{5, 7, 1}},
		{"four values, two resets (D)", []float64{3, 5, 2, 1}, []int{0, 2, 3}, []float64{3, 7, 1}},
		{"reset between two identical values", []float64{4, 3, 4}, []int{0, 1, 2}, []float64{4, 3, 4}},
		{"four values, three resets", []float64{9, 7, 4, 1}, []int{0, 2, 3}, []float64{9, 20, 1}},
		{"five values, two resets (A)", []float64{3, 1, 2, 5, 4}, []int{0, 3, 4}, []float64{3, 8, 4}},
		{"five equal values", []float64{1, 1, 1, 1, 1}, []int{4}, []float64{1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := &record{vals: tt.givenValues}
			frame := SeriesBlockFrame{record: record}
			indices, results := DownsampleCounterResets(math.NaN(), frame)
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
		{"empty", 3, []float64{}, nil, nil},
		{"one value equal to prev frame last", 2, []float64{2}, []int{0}, []float64{2}},
		{"one value less than prev frame last", 3, []float64{2}, []int{0}, []float64{2}},
		{"one value more than prev frame last", 3, []float64{4}, []int{0}, []float64{4}},
		{"two values, increasing", 3, []float64{4, 5}, []int{1}, []float64{5}},
		{"reset between frames", 3, []float64{2, 5}, []int{0, 1}, []float64{2, 5}},
		{"reset between frames and within frame", 4, []float64{2, 1}, []int{0, 1}, []float64{2, 1}},
		{"reset within frame", 1, []float64{4, 3}, []int{0, 1}, []float64{4, 3}},
		{"all equal", 1, []float64{1, 1}, []int{1}, []float64{1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := &record{vals: tt.givenValues}
			frame := SeriesBlockFrame{record: record}
			indices, results := DownsampleCounterResets(tt.givenPrevFrameLastValue, frame)
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
		params.MinSize = 2
	}
	params.MinSuccessfulTests = 10000
	params.MaxShrinkCount = 0
	properties := gopter.NewProperties(params)
	generator := gen.SliceOf(gen.Float64Range(0, 100))

	epsilon := 0.00001

	properties.Property("return consistent indices", prop.ForAll(
		func(v []float64) bool {
			indices, results := downsample(v, usePrevFrameLastValue)

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

			_, results := downsample(v, usePrevFrameLastValue)

			return len(results) <= maxResultLength
		},
		generator,
	))

	properties.Property("preserve the last value", prop.ForAll(
		func(v []float64) bool {
			indices, results := downsample(v, usePrevFrameLastValue)

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
			indices, results := downsample(v, usePrevFrameLastValue)
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

func downsample(vals []float64, usePrevFrameLastValue bool) ([]int, []float64) {
	prevFrameLastValue := math.NaN()

	if usePrevFrameLastValue {
		prevFrameLastValue = vals[0]
		vals = vals[1:]
	}

	frame := SeriesBlockFrame{record: &record{vals: vals}}

	return DownsampleCounterResets(prevFrameLastValue, frame)
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
