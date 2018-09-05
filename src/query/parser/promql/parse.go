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

	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/functions/binary"
	"github.com/m3db/m3/src/query/parser"

	pql "github.com/prometheus/prometheus/promql"
)

type promParser struct {
	expr pql.Expr
}

// Parse takes a promQL string and converts parses it into a DAG
func Parse(q string) (parser.Parser, error) {
	expr, err := pql.ParseExpr(q)
	if err != nil {
		return nil, err
	}

	return &promParser{
		expr: expr,
	}, nil
}

func (p *promParser) DAG() (parser.Nodes, parser.Edges, error) {
	state := &parseState{}
	err := state.walk(p.expr)
	if err != nil {
		return nil, nil, err
	}

	return state.transforms, state.edges, nil
}

func (p *promParser) String() string {
	return p.expr.String()
}

type parseState struct {
	edges      parser.Edges
	transforms parser.Nodes
}

func (p *parseState) lastTransformID() parser.NodeID {
	if len(p.transforms) == 0 {
		return parser.NodeID(-1)
	}

	return p.transforms[len(p.transforms)-1].ID
}

func (p *parseState) transformLen() int {
	return len(p.transforms)
}

func (p *parseState) walk(node pql.Node) error {
	if node == nil {
		return nil
	}
	fmt.Printf("Node type %T, %v\n", node, node)
	switch n := node.(type) {
	case *pql.AggregateExpr:
		err := p.walk(n.Expr)
		if err != nil {
			return err
		}

		val, err := p.resolveScalarArgument(n.Param)
		if err != nil {
			return err
		}

		fmt.Println(val, "expr", n.Expr.String(), "params", n.Param.String())
		op, err := NewAggregationOperator(n)
		if err != nil {
			return err
		}

		opTransform := parser.NewTransformFromOperation(op, p.transformLen())
		p.edges = append(p.edges, parser.Edge{
			ParentID: p.lastTransformID(),
			ChildID:  opTransform.ID,
		})
		p.transforms = append(p.transforms, opTransform)
		// TODO: handle labels, params
		return nil

	case *pql.MatrixSelector:
		operation, err := NewSelectorFromMatrix(n)
		if err != nil {
			return err
		}

		p.transforms = append(p.transforms, parser.NewTransformFromOperation(operation, p.transformLen()))
		return nil

	case *pql.VectorSelector:
		operation, err := NewSelectorFromVector(n)
		if err != nil {
			return err
		}

		p.transforms = append(p.transforms, parser.NewTransformFromOperation(operation, p.transformLen()))
		return nil

	case *pql.Call:
		expressions := n.Args
		argTypes := n.Func.ArgTypes
		argValues := make([]interface{}, 0, len(expressions))
		for i, expr := range expressions {
			if argTypes[i] == pql.ValueTypeScalar {
				val, err := p.resolveScalarArgument(expr)
				if err != nil {
					return err
				}

				argValues = append(argValues, val)
			} else {

				// switch e := expr.(type) {
				// case *pql.NumberLiteral:
				// 	argValues = append(argValues, e.Val)
				// 	continue
				// case *pql.MatrixSelector:
				// 	argValues = append(argValues, e.Range)
				// }

				err := p.walk(expr)
				if err != nil {
					return err
				}
			}

		}

		op, err := NewFunctionExpr(n.Func.Name, argValues)
		if err != nil {
			return err
		}

		opTransform := parser.NewTransformFromOperation(op, p.transformLen())
		p.edges = append(p.edges, parser.Edge{
			ParentID: p.lastTransformID(),
			ChildID:  opTransform.ID,
		})
		p.transforms = append(p.transforms, opTransform)
		return nil

	case *pql.BinaryExpr:
		err := p.walk(n.LHS)
		if err != nil {
			return err
		}

		lhsID := p.lastTransformID()
		err = p.walk(n.RHS)
		if err != nil {
			return err
		}

		rhsID := p.lastTransformID()
		op, err := NewBinaryOperator(n, lhsID, rhsID)
		if err != nil {
			return err
		}

		opTransform := parser.NewTransformFromOperation(op, p.transformLen())
		p.edges = append(p.edges, parser.Edge{
			ParentID: lhsID,
			ChildID:  opTransform.ID,
		})
		p.edges = append(p.edges, parser.Edge{
			ParentID: rhsID,
			ChildID:  opTransform.ID,
		})
		p.transforms = append(p.transforms, opTransform)
		return nil

	case *pql.NumberLiteral:
		op := NewScalarOperator(n)
		opTransform := parser.NewTransformFromOperation(op, p.transformLen())
		p.transforms = append(p.transforms, opTransform)
		return nil

	case *pql.ParenExpr:
		// Evaluate inside of paren expressions
		return p.walk(n.Expr)

	default:
		return fmt.Errorf("promql.Walk: unhandled node type %T, %v", node, node)
	}

	// TODO: This should go away once all cases have been implemented
	return errors.ErrNotImplemented
}

var (
	errNilScalarArg         = fmt.Errorf("scalar expression is nil")
	errInvalidNestingFetch  = fmt.Errorf("invalid nesting for fetch")
	errInvalidNestingVector = fmt.Errorf("invalid nesting for vector conversion")
)

func (p *parseState) resolveScalarArgument(expr pql.Expr) (float64, error) {
	nesting := 0
	value, err := resolveScalarArgument(expr, &nesting)
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
func resolveScalarArgument(expr pql.Expr, nesting *int) (float64, error) {
	if expr == nil {
		return 0, errNilScalarArg
	}

	switch n := expr.(type) {
	case *pql.BinaryExpr:
		left, err := resolveScalarArgument(n.LHS, nesting)
		if err != nil {
			return 0, err
		}

		right, err := resolveScalarArgument(n.RHS, nesting)
		if err != nil {
			return 0, err
		}

		op := getBinaryOpType(n.Op)

		fn, err := binary.ArithmeticFunction(op, n.ReturnBool)
		if err != nil {
			return 0, err
		}

		return fn(left, right), nil

	case *pql.VectorSelector:
		// during scalar argument resolution, prom does not expand vectors
		// and returns NaN as the value instead.
		if *nesting < 1 {
			return 0, errInvalidNestingFetch
		}

		*nesting = *nesting - 1
		return math.NaN(), nil

	case *pql.Call:
		// TODO: once these functions exist, use those constants here
		// If the function called is `scalar`, evaluate inside and insure a scalar
		if n.Func.Name == "scalar" {
			*nesting = *nesting + 1
			return resolveScalarArgument(n.Args[0], nesting)
		} else if n.Func.Name == "vector" {
			// If the function called is `vector`, evaluate inside and insure a vector
			if *nesting < 1 {
				return 0, errInvalidNestingFetch
			}

			*nesting = *nesting - 1
			return resolveScalarArgument(n.Args[0], nesting)
		}

		fmt.Println(n.Type(), n.String(), n.Func.Name, n.Func.ArgTypes, n.Func.Variadic, n.Func.ReturnType)
		return 0, nil

	case *pql.NumberLiteral:
		return n.Val, nil

	case *pql.ParenExpr:
		// Evaluate inside of paren expressions
		return resolveScalarArgument(n.Expr, nesting)
	}

	return 0, fmt.Errorf("resolveScalarArgument: unhandled node type %T, %v", expr, expr)
}
