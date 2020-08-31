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
	"math"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/assert"
)

func TestDownsampleCounterResets(t *testing.T) {
	tests := []struct {
		name       string
		givenFrame []float64
		want       []DownsampledValue
	}{
		{
			name:       "empty",
			givenFrame: []float64{},
			want:       []DownsampledValue{},
		},
		{
			name:       "one value",
			givenFrame: []float64{3.14},
			want:       []DownsampledValue{{0, 3.14}},
		},
		{
			name:       "two different values",
			givenFrame: []float64{3, 5},
			want:       []DownsampledValue{{0, 3}, {1, 5}},
		},
		{
			name:       "two identical values",
			givenFrame: []float64{5, 5},
			want:       []DownsampledValue{{1, 5}},
		},
		{
			name:       "two values with reset",
			givenFrame: []float64{3, 1},
			want:       []DownsampledValue{{0, 3}, {1, 1}},
		},
		{
			name:       "second value equal to first, then reset",
			givenFrame: []float64{2, 2, 1},
			want:       []DownsampledValue{{0, 2}, {2, 1}},
		},
		{
			name:       "three values, no reset",
			givenFrame: []float64{3, 4, 5},
			want:       []DownsampledValue{{0, 3}, {2, 5}},
		},
		{
			name:       "three identical values",
			givenFrame: []float64{5, 5, 5},
			want:       []DownsampledValue{{2, 5}},
		},
		{
			name:       "three values, reset after first",
			givenFrame: []float64{3, 1, 5},
			want:       []DownsampledValue{{0, 3}, {1, 1}, {2, 5}},
		},
		{
			name:       "three values, reset after second",
			givenFrame: []float64{3, 5, 1},
			want:       []DownsampledValue{{0, 3}, {1, 5}, {2, 1}},
		},
		{
			name:       "three values, two resets",
			givenFrame: []float64{5, 3, 2},
			want:       []DownsampledValue{{0, 5}, {1, 8}, {2, 2}},
		},
		{
			name:       "four values, reset after first",
			givenFrame: []float64{3, 1, 4, 5},
			want:       []DownsampledValue{{0, 3}, {1, 1}, {3, 5}},
		},
		{
			name:       "four values, reset after second (A)",
			givenFrame: []float64{3, 4, 1, 5},
			want:       []DownsampledValue{{0, 3}, {1, 4}, {2, 1}, {3, 5}},
		},
		{
			name:       "four values, reset after second (B)",
			givenFrame: []float64{3, 4, 1, 4},
			want:       []DownsampledValue{{0, 3}, {1, 4}, {2, 1}, {3, 4}},
		},
		{
			name:       "four values, reset after second (C)",
			givenFrame: []float64{3, 4, 1, 2},
			want:       []DownsampledValue{{0, 3}, {1, 4}, {3, 2}},
		},
		{
			name:       "four values, reset after third",
			givenFrame: []float64{3, 4, 5, 1},
			want:       []DownsampledValue{{0, 3}, {2, 5}, {3, 1}},
		},
		{
			name:       "four values, two resets (A)",
			givenFrame: []float64{3, 1, 5, 4},
			want:       []DownsampledValue{{0, 3}, {2, 8}, {3, 4}},
		},
		{
			name:       "four values, two resets (B)",
			givenFrame: []float64{5, 2, 1, 4},
			want:       []DownsampledValue{{0, 5}, {1, 7}, {3, 4}},
		},
		{
			name:       "four values, two resets (C)",
			givenFrame: []float64{5, 2, 2, 1},
			want:       []DownsampledValue{{0, 5}, {2, 7}, {3, 1}},
		},
		{
			name:       "four values, two resets (D)",
			givenFrame: []float64{3, 5, 2, 1},
			want:       []DownsampledValue{{0, 3}, {2, 7}, {3, 1}},
		},
		{
			name:       "reset between two equal values",
			givenFrame: []float64{4, 3, 4},
			want:       []DownsampledValue{{0, 4}, {1, 3}, {2, 4}},
		},
		{
			name:       "four values, three resets",
			givenFrame: []float64{9, 7, 4, 1},
			want:       []DownsampledValue{{0, 9}, {2, 20}, {3, 1}},
		},
		{
			name:       "five values, two resets (A)",
			givenFrame: []float64{3, 1, 2, 5, 4},
			want:       []DownsampledValue{{0, 3}, {3, 8}, {4, 4}},
		},
		{
			name:       "five equal values",
			givenFrame: []float64{1, 1, 1, 1, 1},
			want:       []DownsampledValue{{4, 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			results := downsample(math.NaN(), tt.givenFrame)

			assert.Equal(t, tt.want, results)
		})
	}
}

func TestDownsampleCounterResetsWithPrevFrameLastValue(t *testing.T) {
	tests := []struct {
		name                    string
		givenPrevFrameLastValue float64
		givenFrame              []float64
		want                    []DownsampledValue
	}{
		{
			name:                    "empty",
			givenPrevFrameLastValue: 3,
			givenFrame:              []float64{},
			want:                    []DownsampledValue{},
		},
		{
			name:                    "one value equal to prev frame last",
			givenPrevFrameLastValue: 2,
			givenFrame:              []float64{2},
			want:                    []DownsampledValue{{0, 2}},
		},
		{
			name:                    "one value less than prev frame last",
			givenPrevFrameLastValue: 3,
			givenFrame:              []float64{2},
			want:                    []DownsampledValue{{0, 2}},
		},
		{
			name:                    "one value more than prev frame last",
			givenPrevFrameLastValue: 3,
			givenFrame:              []float64{4},
			want:                    []DownsampledValue{{0, 4}},
		},
		{
			name:                    "two values, increasing",
			givenPrevFrameLastValue: 3,
			givenFrame:              []float64{4, 5},
			want:                    []DownsampledValue{{1, 5}},
		},
		{
			name:                    "reset between frames",
			givenPrevFrameLastValue: 3,
			givenFrame:              []float64{2, 5},
			want:                    []DownsampledValue{{0, 2}, {1, 5}},
		},
		{
			name:                    "reset between frames and within frame",
			givenPrevFrameLastValue: 4,
			givenFrame:              []float64{2, 1},
			want:                    []DownsampledValue{{0, 2}, {1, 1}},
		},
		{
			name:                    "reset within frame",
			givenPrevFrameLastValue: 1,
			givenFrame:              []float64{4, 3},
			want:                    []DownsampledValue{{0, 4}, {1, 3}},
		},
		{
			name:                    "all equal",
			givenPrevFrameLastValue: 1,
			givenFrame:              []float64{1, 1},
			want:                    []DownsampledValue{{1, 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			results := downsample(tt.givenPrevFrameLastValue, tt.givenFrame)

			assert.Equal(t, tt.want, results)
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
			results := downsampleFromSlice(v, usePrevFrameLastValue)

			for i := 1; i < len(results); i++ {
				if results[i].FrameIndex <= results[i-1].FrameIndex {
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

			results := downsampleFromSlice(v, usePrevFrameLastValue)

			return len(results) <= maxResultLength
		},
		generator,
	))

	properties.Property("preserve the last value", prop.ForAll(
		func(v []float64) bool {
			results := downsampleFromSlice(v, usePrevFrameLastValue)

			lastIndex := len(v) - 1
			lastFrameValue := v[lastIndex]
			if usePrevFrameLastValue {
				lastIndex--
			}

			lastResult := results[len(results)-1]

			return lastResult.Value == lastFrameValue && lastResult.FrameIndex == lastIndex
		},
		generator,
	))

	properties.Property("preserve values after adjusting for counter resets", prop.ForAll(
		func(v []float64) bool {
			results := downsampleFromSlice(v, usePrevFrameLastValue)

			downsampledValues := make([]float64, 0, len(results))
			for _, result := range results {
				downsampledValues = append(downsampledValues, result.Value)
			}

			prevFrameLastValue := 0.0
			input := v
			if usePrevFrameLastValue {
				prevFrameLastValue = v[0]
				input = input[1:]
			}

			adjustedInput := applyCounterResetAdjustment(prevFrameLastValue, input)
			adjustedResults := applyCounterResetAdjustment(prevFrameLastValue, downsampledValues)

			for i, result := range results {
				if math.Abs(adjustedResults[i]-adjustedInput[result.FrameIndex]) > epsilon {
					return false
				}
			}

			return true
		},
		generator,
	))

	properties.TestingRun(t)
}

func downsample(prevFrameLastValue float64, vals []float64) []DownsampledValue {
	results := make([]DownsampledValue, 0, 4)

	DownsampleCounterResets(prevFrameLastValue, vals, &results)

	return results
}

func downsampleFromSlice(vals []float64, usePrevFrameLastValue bool) []DownsampledValue {
	prevFrameLastValue := math.NaN()

	if usePrevFrameLastValue {
		prevFrameLastValue = vals[0]
		vals = vals[1:]
	}

	return downsample(prevFrameLastValue, vals)
}

func applyCounterResetAdjustment(previousVal float64, vals []float64) []float64 {
	transformed := make([]float64, 0, len(vals))
	if len(vals) == 0 {
		return transformed
	}

	var (
		previous    = previousVal
		accumulated float64
	)

	for i := 0; i < len(vals); i++ {
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
