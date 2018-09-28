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

package promql

import (
	"fmt"
	"math"

	"github.com/m3db/m3/src/query/functions/binary"

	pql "github.com/prometheus/prometheus/promql"
)

var (
	errNilScalarArg         = fmt.Errorf("scalar expression is nil")
	errInvalidNestingFetch  = fmt.Errorf("invalid nesting for fetch")
	errInvalidNestingVector = fmt.Errorf("invalid nesting for vector conversion")
)

func resolveScalarArgument(expr pql.Expr) (float64, error) {
	value, nesting, err := resolveScalarArgumentWithNesting(expr, 0)
	// On a regular error, return error
	if err != nil {
		return 0, err
	}

	if nesting != 0 {
		return 0, fmt.Errorf("promql.resolveScalarArgument: invalid nesting %d", nesting)
	}

	return value, nil
}

// resolves an expression which should resolve to a scalar argument
func resolveScalarArgumentWithNesting(expr pql.Expr, nesting int) (float64, int, error) {
	if expr == nil {
		return 0, 0, errNilScalarArg
	}

	switch n := expr.(type) {
	case *pql.BinaryExpr:
		left, nestingLeft, err := resolveScalarArgumentWithNesting(n.LHS, nesting)
		if err != nil {
			return 0, 0, err
		}

		right, nestingRight, err := resolveScalarArgumentWithNesting(n.RHS, nesting)
		if err != nil {
			return 0, 0, err
		}

		if nestingLeft < nestingRight {
			nesting = nestingLeft
		} else {
			nesting = nestingRight
		}

		op := getBinaryOpType(n.Op)
		fn, err := binary.ArithmeticFunction(op, n.ReturnBool)
		if err != nil {
			return 0, 0, err
		}

		return fn(left, right), nesting, nil

	case *pql.VectorSelector:
		// during scalar argument resolution, prom does not expand vectors
		// and returns NaN as the value instead.
		if nesting < 1 {
			return 0, 0, errInvalidNestingFetch
		}

		return math.NaN(), nesting - 1, nil

	case *pql.Call:
		// TODO: once these functions exist, use those constants here
		// If the function called is `scalar`, evaluate inside and insure a scalar
		if n.Func.Name == "scalar" {
			return resolveScalarArgumentWithNesting(n.Args[0], nesting+1)
		} else if n.Func.Name == "vector" {
			// If the function called is `vector`, evaluate inside and insure a vector
			if nesting < 1 {
				return 0, 0, errInvalidNestingVector
			}

			return resolveScalarArgumentWithNesting(n.Args[0], nesting-1)
		}

		return 0, 0, nil

	case *pql.NumberLiteral:
		return n.Val, 0, nil

	case *pql.ParenExpr:
		// Evaluate inside of paren expressions
		return resolveScalarArgumentWithNesting(n.Expr, nesting)
	}

	return 0, 0, fmt.Errorf("resolveScalarArgument: unhandled node type %T, %v", expr, expr)
}
