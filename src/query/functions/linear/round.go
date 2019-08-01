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

package linear

import (
	"fmt"
	"math"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/functions/lazy"
	"github.com/m3db/m3/src/query/parser"
)

// RoundType rounds each datapoint in the series.
// Ties are resolved by rounding up. The optional to_nearest argument allows
// specifying the nearest multiple to which the timeseries values should be
// rounded (default=1). This variable may be a fraction.
// Special cases are: round(_, 0) = NaN
const RoundType = "round"

type roundOp struct {
	toNearest float64
}

func parseArgs(args []interface{}) (float64, error) {
	// NB: if no args passed; this should use default value of `1`.
	if len(args) == 0 {
		return 1, nil
	}

	if len(args) > 1 {
		return 0, fmt.Errorf("invalid number of args for round: %d", len(args))
	}

	// Attempt to parse a single arg.
	if nearest, ok := args[0].(float64); ok {
		return nearest, nil
	}

	return 0, fmt.Errorf("unable to cast to to_nearest argument: %v", args[0])
}

func roundFn(roundTo float64) block.ValueTransform {
	if roundTo == 0 {
		return func(float64) float64 { return math.NaN() }
	}

	roundToInverse := 1.0 / roundTo
	return func(v float64) float64 {
		return math.Floor(v*roundToInverse+0.5) / roundToInverse
	}
}

// NewRoundOp creates a new round op based on the type and arguments.
func NewRoundOp(args []interface{}) (parser.Params, error) {
	toNearest, err := parseArgs(args)
	if err != nil {
		return nil, err
	}

	lazyOpts := block.NewLazyOptions().SetValueTransform(roundFn(toNearest))
	return lazy.NewLazyOp(RoundType, lazyOpts)
}
