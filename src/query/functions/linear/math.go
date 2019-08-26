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

const (
	// AbsType takes absolute value of each datapoint in the series.
	AbsType = "abs"

	// CeilType rounds each value in the timeseries up to the nearest integer.
	CeilType = "ceil"

	// FloorType rounds each value in the timeseries down to the nearest integer.
	FloorType = "floor"

	// ExpType calculates the exponential function for all values.
	// Special cases are: Exp(+Inf) = +Inf and Exp(NaN) = NaN
	ExpType = "exp"

	// SqrtType calculates the square root for all values.
	SqrtType = "sqrt"

	// Special cases for all of the following include:
	// ln(+Inf) = +Inf
	// ln(0) = -Inf
	// ln(x < 0) = NaN
	// ln(NaN) = NaN

	// LnType calculates the natural logarithm for all values.
	LnType = "ln"

	// Log2Type calculates the binary logarithm for all values.
	Log2Type = "log2"

	// Log10Type calculates the decimal logarithm for values.
	Log10Type = "log10"
)

var (
	mathFuncs = map[string]block.ValueTransform{
		AbsType:   math.Abs,
		CeilType:  math.Ceil,
		FloorType: math.Floor,
		ExpType:   math.Exp,
		SqrtType:  math.Sqrt,
		LnType:    math.Log,
		Log2Type:  math.Log2,
		Log10Type: math.Log10,
	}
)

// NewMathOp creates a new math op based on the type.
func NewMathOp(opType string) (parser.Params, error) {
	if fn, ok := mathFuncs[opType]; ok {
		lazyOpts := block.NewLazyOptions().SetValueTransform(fn)
		return lazy.NewLazyOp(opType, lazyOpts)
	}

	return nil, fmt.Errorf("unknown math type: %s", opType)
}
