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

package binary

import (
	"math"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
)

const (
	// EqType checks that lhs is equal to rhs
	EqType = "=="

	// NotEqType checks that lhs is equal to rhs
	NotEqType = "!="

	// GreaterType checks that lhs is equal to rhs
	GreaterType = ">"

	// LesserType checks that lhs is equal to rhs
	LesserType = "<"

	// GreaterEqType checks that lhs is equal to rhs
	GreaterEqType = ">="

	// LesserEqType checks that lhs is equal to rhs
	LesserEqType = "<="

	// suffix to return bool values instead of lhs values
	returnBoolSuffix = "BOOL"
)

// convert true to 1, false to 0
func toFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// convert true to x, false to NaN
func toComparisonValue(b bool, x float64) float64 {
	if b {
		return x
	}
	return math.NaN()
}

var (
	comparisonFuncs = map[string]Function{
		EqType:        func(x, y float64) float64 { return toComparisonValue(x == y, x) },
		NotEqType:     func(x, y float64) float64 { return toComparisonValue(x != y, x) },
		GreaterType:   func(x, y float64) float64 { return toComparisonValue(x > y, x) },
		LesserType:    func(x, y float64) float64 { return toComparisonValue(x < y, x) },
		GreaterEqType: func(x, y float64) float64 { return toComparisonValue(x >= y, x) },
		LesserEqType:  func(x, y float64) float64 { return toComparisonValue(x <= y, x) },

		EqType + returnBoolSuffix:        func(x, y float64) float64 { return toFloat(x == y) },
		NotEqType + returnBoolSuffix:     func(x, y float64) float64 { return toFloat(x != y) },
		GreaterType + returnBoolSuffix:   func(x, y float64) float64 { return toFloat(x > y) },
		LesserType + returnBoolSuffix:    func(x, y float64) float64 { return toFloat(x < y) },
		GreaterEqType + returnBoolSuffix: func(x, y float64) float64 { return toFloat(x >= y) },
		LesserEqType + returnBoolSuffix:  func(x, y float64) float64 { return toFloat(x <= y) },
	}
)

// Builds a comparison processing function if able. If wrong opType supplied,
// returns no function and false
func buildComparisonFunction(
	opType string,
	params NodeParams,
) (processFunc, bool) {
	if params.ReturnBool {
		opType += returnBoolSuffix
	}
	fn, ok := comparisonFuncs[opType]
	if !ok {
		return nil, false
	}

	return func(queryCtx *models.QueryContext, lhs, rhs block.Block, controller *transform.Controller) (block.Block, error) {
		return processBinary(queryCtx, lhs, rhs, params, controller, true, fn)
	}, true
}
