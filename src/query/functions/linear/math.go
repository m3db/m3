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

	"github.com/m3db/m3db/src/query/executor/transform"
)

const (
	// AbsType takes absolute value of each datapoint in the series
	AbsType = "abs"

	// CeilType rounds each value in the timeseries up to the nearest integer
	CeilType = "ceil"

	// FloorType rounds each value in the timeseries down to the nearest integer
	FloorType = "floor"

	// ExpType calculates the exponential function for all values in the timerseies
	// Special cases are: Exp(+Inf) = +Inf and Exp(NaN) = NaN
	ExpType = "exp"

	// SqrtType calculates the square root for all values in the timeseries
	SqrtType = "sqrt"

	// Special cases for all of the following include:
	// ln(+Inf) = +Inf
	// ln(0) = -Inf
	// ln(x < 0) = NaN
	// ln(NaN) = NaN

	// LnType calculates the natural logarithm for all values in the timeseries
	LnType = "ln"

	// Log2Type calculates the binary logarithm for all values in the timeseries
	Log2Type = "log2"

	// Log10Type calculates the decimal logarithm for all values in the timeseries
	Log10Type = "log10"
)

var (
	mathFuncs = map[string]func(x float64) float64{
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

// NewMathOp creates a new math op based on the type
func NewMathOp(optype string) (BaseOp, error) {
	if _, ok := mathFuncs[optype]; !ok {
		return emptyOp, fmt.Errorf("unknown math type: %s", optype)
	}

	return BaseOp{
		operatorType: optype,
		processorFn:  newMathNode,
	}, nil
}

func newMathNode(op BaseOp, controller *transform.Controller) Processor {
	return &mathNode{op: op, controller: controller, mathFn: mathFuncs[op.operatorType]}
}

type mathNode struct {
	op         BaseOp
	mathFn     func(x float64) float64
	controller *transform.Controller
}

func (m *mathNode) Process(values []float64) []float64 {
	for i := range values {
		values[i] = m.mathFn(values[i])
	}

	return values
}
