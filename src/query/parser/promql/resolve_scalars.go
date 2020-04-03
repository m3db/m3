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
	"github.com/m3db/m3/src/query/functions/scalar"

	pql "github.com/prometheus/prometheus/promql"
)

var (
	errNilScalarArg         = fmt.Errorf("scalar expression is nil")
	errInvalidNestingFetch  = fmt.Errorf("invalid nesting for fetch")
	errInvalidNestingVector = fmt.Errorf("invalid nesting for vector conversion")
)

func resolveScalarArgument(expr pql.Expr) (scalar.Value, error) {
	value, nesting, err := resolveScalarArgumentWithNesting(expr, 0)
	if err != nil {
		return scalar.Value{}, err
	}

	if nesting != 0 {
		return scalar.Value{},
			fmt.Errorf("promql.resolveScalarArgument: invalid nesting %d", nesting)
	}

	return value, nil
}

// resolves an expression which should resolve to a scalar argument
func resolveScalarArgumentWithNesting(
	expr pql.Expr,
	nesting int,
) (scalar.Value, int, error) {
	if expr == nil {
		return scalar.Value{}, 0, errNilScalarArg
	}

	switch n := expr.(type) {
	case *pql.BinaryExpr:
		left, nestingLeft, err := resolveScalarArgumentWithNesting(n.LHS, nesting)
		if err != nil {
			return scalar.Value{}, 0, err
		}

		right, nestingRight, err := resolveScalarArgumentWithNesting(n.RHS, nesting)
		if err != nil {
			return scalar.Value{}, 0, err
		}

		if nestingLeft < nestingRight {
			nesting = nestingLeft
		} else {
			nesting = nestingRight
		}

		op := getBinaryOpType(n.Op)
		fn, err := binary.ArithmeticFunction(op, n.ReturnBool)
		if err != nil {
			return scalar.Value{}, 0, err
		}

		applied := scalar.ApplyFunc(left, right, fn)
		return applied, nesting, nil

	case *pql.VectorSelector:
		// during scalar argument resolution, prom does not expand vectors
		// and returns NaN as the value instead.
		if nesting < 1 {
			return scalar.Value{}, 0, errInvalidNestingFetch
		}

		return scalar.Value{Scalar: math.NaN()}, nesting - 1, nil

	case *pql.Call:
		// If the function called is `scalar`, evaluate and ensure a scalar.
		if n.Func.Name == scalar.ScalarType {
			return resolveScalarArgumentWithNesting(n.Args[0], nesting+1)
		} else if n.Func.Name == scalar.VectorType {
			// If the function called is `vector`, evaluate and ensure a vector.
			if nesting < 1 {
				return scalar.Value{}, 0, errInvalidNestingVector
			}

			return resolveScalarArgumentWithNesting(n.Args[0], nesting-1)
		} else if n.Func.Name == scalar.TimeType {
			return scalar.Value{
				HasTimeValues: true,
				TimeValueFn: func(generator scalar.TimeValueGenerator) []float64 {
					return generator()
				},
			}, 0, nil
		}

		return scalar.Value{}, 0, fmt.Errorf("unknown call: %s", n.String())

	case *pql.NumberLiteral:
		return scalar.Value{Scalar: n.Val}, 0, nil

	case *pql.UnaryExpr:
		if n.Op.String() == binary.PlusType {
			return resolveScalarArgumentWithNesting(n.Expr, nesting)
		}

		val, nesting, err := resolveScalarArgumentWithNesting(n.Expr, nesting)
		if err != nil {
			return scalar.Value{}, 0, err
		}

		return scalar.ApplyFunc(val, scalar.Value{}, func(x, _ float64) float64 {
			return x * -1
		}), nesting, err

	case *pql.ParenExpr:
		// Evaluate inside of paren expressions
		return resolveScalarArgumentWithNesting(n.Expr, nesting)
	}

	return scalar.Value{}, 0,
		fmt.Errorf("resolveScalarArgument: unhandled node type %T, %v", expr, expr)
}
