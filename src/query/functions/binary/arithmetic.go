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
	// PlusType adds datapoints in both series
	PlusType = "+"

	// MinusType subtracts rhs from lhs
	MinusType = "-"

	// MultiplyType multiplies datapoints by series
	MultiplyType = "*"

	// DivType divides datapoints by series
	// Special cases are:
	// 	 X / 0 = +Inf
	// 	-X / 0 = -Inf
	// 	 0 / 0 =  NaN
	DivType = "/"

	// ExpType raises lhs to the power of rhs
	// NB: to keep consistency with prometheus (and go)
	//  0 ^ 0 = 1
	//  NaN ^ 0 = 1
	ExpType = "^"

	// ModType takes the modulo of lhs by rhs
	// Special cases are:
	// 	 X % 0 = NaN
	// 	 NaN % X = NaN
	// 	 X % NaN = NaN
	ModType = "%"
)

var (
	arithmeticFuncs = map[string]Function{
		PlusType:     func(x, y float64) float64 { return x + y },
		MinusType:    func(x, y float64) float64 { return x - y },
		MultiplyType: func(x, y float64) float64 { return x * y },
		DivType:      func(x, y float64) float64 { return x / y },
		ModType:      math.Mod,
		ExpType:      math.Pow,
	}
)

// Builds an arithmetic processing function if able. If wrong opType supplied,
// returns no function and false
func buildArithmeticFunction(
	opType string,
	params NodeParams,
) (processFunc, bool) {
	fn, ok := arithmeticFuncs[opType]
	if !ok {
		return nil, false
	}

	// Build the binary processing step
	return func(queryCtx *models.QueryContext, lhs, rhs block.Block, controller *transform.Controller) (block.Block, error) {
		return processBinary(queryCtx, lhs, rhs, params, controller, false, fn)
	}, true
}
