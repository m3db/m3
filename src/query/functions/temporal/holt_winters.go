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
	"fmt"
	"math"

	"github.com/m3db/m3/src/query/executor/transform"
)

const (
	// HoltWintersType produces a smoothed value for time series based on the specified interval.
	// The algorithm used comes from https://en.wikipedia.org/wiki/Exponential_smoothing#Double_exponential_smoothing.
	// Holt-Winters should only be used with gauges.
	HoltWintersType = "holt_winters"
)

// NewHoltWintersOp creates a new base Holt-Winters transform with a specified node.
func NewHoltWintersOp(args []interface{}) (transform.Params, error) {
	// todo(braskin): move this logic to the parser.
	if len(args) != 3 {
		return emptyOp, fmt.Errorf("invalid number of args for %s: %d", HoltWintersType, len(args))
	}

	sf, ok := args[1].(float64)
	if !ok {
		return emptyOp, fmt.Errorf("unable to cast to scalar argument: %v for %s", args[0], HoltWintersType)
	}

	tf, ok := args[2].(float64)
	if !ok {
		return emptyOp, fmt.Errorf("unable to cast to scalar argument: %v for %s", args[0], HoltWintersType)
	}

	// Sanity check the input.
	if sf <= 0 || sf >= 1 {
		return emptyOp, fmt.Errorf("invalid smoothing factor. Expected: 0 < sf < 1, got: %f", sf)
	}

	if tf <= 0 || tf >= 1 {
		return emptyOp, fmt.Errorf("invalid trend factor. Expected: 0 < tf < 1, got: %f", tf)
	}

	aggregationFunc := makeHoltWintersFn(sf, tf)
	a := aggProcessor{
		aggFunc: aggregationFunc,
	}

	return newBaseOp(args, HoltWintersType, a)
}

func makeHoltWintersFn(sf, tf float64) aggFunc {
	return func(vals []float64) float64 {
		var (
			foundFirst, foundSecond         bool
			secondVal                       float64
			trendVal                        float64
			scaledSmoothVal, scaledTrendVal float64
			prev, curr                      float64
			idx                             int
		)

		for _, val := range vals {
			if math.IsNaN(val) {
				continue
			}

			if !foundFirst {
				foundFirst = true
				curr = val
				idx++
				continue
			}

			if !foundSecond {
				foundSecond = true
				secondVal = val
				trendVal = secondVal - curr
			}

			// scale the raw value against the smoothing factor.
			scaledSmoothVal = sf * val

			// scale the last smoothed value with the trend at this point.
			trendVal = calcTrendValue(idx-1, sf, tf, prev, curr, trendVal)
			scaledTrendVal = (1 - sf) * (curr + trendVal)

			prev, curr = curr, scaledSmoothVal+scaledTrendVal
			idx++
		}

		// need at least two values to apply a smoothing operation.
		if !foundSecond {
			return math.NaN()
		}

		return curr
	}
}

// Calculate the trend value at the given index i in raw data d.
// This is somewhat analogous to the slope of the trend at the given index.
// The argument "s" is the set of computed smoothed values.
// The argument "b" is the set of computed trend factors.
// The argument "d" is the set of raw input values.
func calcTrendValue(i int, sf, tf, s0, s1, b float64) float64 {
	if i == 0 {
		return b
	}

	x := tf * (s1 - s0)
	y := (1 - tf) * b

	return x + y
}
