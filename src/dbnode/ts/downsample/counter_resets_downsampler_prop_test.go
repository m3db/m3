// +build big
//
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
	"time"

	"github.com/m3db/m3/src/x/instrument"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"go.uber.org/zap"
)

func TestDownsampleCounterResetsInvariants(t *testing.T) {
	testDownsampleCounterResetsInvariants(t, false)
}

func TestDownsampleCounterResetsInvariantsWithPrevFrameLastValue(t *testing.T) {
	testDownsampleCounterResetsInvariants(t, true)
}

func testDownsampleCounterResetsInvariants(t *testing.T, usePrevFrameLastValue bool) {
	seed := time.Now().UnixNano()
	params := gopter.DefaultTestParameters()
	params.MinSize = 1
	if usePrevFrameLastValue {
		params.MinSize++
	}
	params.MinSuccessfulTests = 10000
	params.MaxShrinkCount = 0
	params.Rng.Seed(seed)
	properties := gopter.NewProperties(params)
	generator := gen.SliceOf(gen.Float64Range(0, 100))

	epsilon := 0.00001

	// NB: capture seed to be able to replicate failed runs.
	logger := instrument.NewTestOptions(t).Logger()
	logger.Info("Running tests", zap.Int64("seed", seed), zap.Bool("usePrevFrameLastValue", usePrevFrameLastValue))

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

func downsampleFromSlice(vals []float64, usePrevFrameLastValue bool) []Value {
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
