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
	"time"

	pql "github.com/prometheus/prometheus/promql/parser"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/functions/binary"
	"github.com/m3db/m3/src/query/functions/lazy"
	"github.com/m3db/m3/src/query/functions/scalar"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	xtime "github.com/m3db/m3/src/x/time"
)

type promParser struct {
	stepSize          time.Duration
	expr              pql.Expr
	tagOpts           models.TagOptions
	parseFunctionExpr ParseFunctionExpr
}

// Parse takes a promQL string and converts parses it into a DAG.
func Parse(
	q string,
	stepSize time.Duration,
	tagOpts models.TagOptions,
	parseOptions ParseOptions,
) (parser.Parser, error) {
	fn := parseOptions.ParseFn()
	expr, err := fn(q)
	if err != nil {
		return nil, err
	}

	return &promParser{
		expr:              expr,
		stepSize:          stepSize,
		tagOpts:           tagOpts,
		parseFunctionExpr: parseOptions.FunctionParseExpr(),
	}, nil
}

func (p *promParser) DAG() (parser.Nodes, parser.Edges, error) {
	state := &parseState{
		stepSize:          p.stepSize,
		tagOpts:           p.tagOpts,
		parseFunctionExpr: p.parseFunctionExpr,
	}

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
	stepSize          time.Duration
	edges             parser.Edges
	transforms        parser.Nodes
	tagOpts           models.TagOptions
	parseFunctionExpr ParseFunctionExpr
}

func (p *parseState) lastTransformID() parser.NodeID {
	if len(p.transforms) == 0 {
		return parser.NodeID(rune(-1))
	}

	return p.transforms[len(p.transforms)-1].ID
}

func (p *parseState) transformLen() int {
	return len(p.transforms)
}

func (p *parseState) addLazyUnaryTransform(unaryOp string) error {
	// NB: if unary type is "+", we do not apply any offsets.
	if unaryOp == binary.PlusType {
		return nil
	}

	vt := func(val float64) float64 { return val * -1.0 }
	lazyOpts := block.NewLazyOptions().SetValueTransform(vt)

	op, err := lazy.NewLazyOp(lazy.UnaryType, lazyOpts)
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
}

func (p *parseState) addLazyOffsetTransform(offset time.Duration) error {
	// NB: if offset is <= 0, we do not apply any offsets.
	if offset == 0 {
		return nil
	} else if offset < 0 {
		return fmt.Errorf("offset must be positive, received: %v", offset)
	}

	var (
		tt = func(t xtime.UnixNano) xtime.UnixNano { return t.Add(offset) }
		mt = func(meta block.Metadata) block.Metadata {
			meta.Bounds.Start = meta.Bounds.Start.Add(offset)
			return meta
		}
	)

	lazyOpts := block.NewLazyOptions().
		SetTimeTransform(tt).
		SetMetaTransform(mt)

	op, err := lazy.NewLazyOp(lazy.OffsetType, lazyOpts)
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
}

func adjustOffset(offset time.Duration, step time.Duration) time.Duration {
	// handles case where offset is 0 too.
	align := offset % step
	if align == 0 {
		return offset
	}

	// NB: Prometheus rounds offsets up to step size, e.g. a 61 second offset with
	// a 1 minute stepsize gets rounded to a 2 minute offset.
	return offset + step - align
}

func (p *parseState) walk(node pql.Node) error {
	if node == nil {
		return nil
	}


	switch n := node.(type) {
	case *pql.AggregateExpr:
		fmt.Printf("AggregateExpr node2 %v \n", node)
		err := p.walk(n.Expr)
		if err != nil {
			return err
		}

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
		fmt.Printf("MatrixSelector node2 %v \n", node)
		// Align offset to stepSize.
		vectorSelector := n.VectorSelector.(*pql.VectorSelector)
		vectorSelector.Offset = adjustOffset(vectorSelector.OriginalOffset, p.stepSize)
		operation, err := NewSelectorFromMatrix(n, p.tagOpts)
		if err != nil {
			return err
		}

		p.transforms = append(
			p.transforms,
			parser.NewTransformFromOperation(operation, p.transformLen()),
		)
		return p.addLazyOffsetTransform(vectorSelector.OriginalOffset)

	case *pql.VectorSelector:
		fmt.Printf("VectorSelector node2 %v \n", node)
		// Align offset to stepSize.
		n.Offset = adjustOffset(n.OriginalOffset, p.stepSize)
		operation, err := NewSelectorFromVector(n, p.tagOpts)
		if err != nil {
			return err
		}

		p.transforms = append(
			p.transforms,
			parser.NewTransformFromOperation(operation, p.transformLen()),
		)

		return p.addLazyOffsetTransform(n.OriginalOffset)

	case *pql.Call:
		if n.Func.Name == scalar.VectorType {
			if len(n.Args) != 1 {
				return fmt.Errorf(
					"vector() operation must be called with 1 argument, got %d",
					len(n.Args),
				)
			}

			val, err := resolveScalarArgument(n.Args[0])
			if err != nil {
				return err
			}

			op, err := scalar.NewScalarOp(val, p.tagOpts)
			if err != nil {
				return err
			}

			opTransform := parser.NewTransformFromOperation(op, p.transformLen())
			p.transforms = append(p.transforms, opTransform)
			return nil
		}

		for i, expr := range n.Args {
			n.Args[i] = unwrapParenExpr(expr)
		}

		var (
			// argTypes describes Prom's expected argument types for this call.
			argTypes = n.Func.ArgTypes
			// expressions describes the actual arguments for this call.
			expressions       = n.Args
			argCount          = len(argTypes)
			exprCount         = len(expressions)
			numExpectedValues = argCount
			variadic          = n.Func.Variadic
			hasValue          = false
		)

		if variadic == 0 {
			if argCount != exprCount {
				return fmt.Errorf("incorrect number of expressions(%d) for %q, "+
					"received %d", exprCount, n.Func.Name, argCount)
			}
		} else {
			hasValue = exprCount > 0
			if argCount-1 > exprCount {
				return fmt.Errorf("incorrect number of expressions(%d) for variadic "+
					"function %q, received %d", exprCount, n.Func.Name, argCount)
			}

			if argCount != exprCount {
				numExpectedValues--
			}
		}

		argValues := make([]interface{}, 0, exprCount)
		stringValues := make([]string, 0, exprCount)
		for i := 0; i < numExpectedValues; i++ {
			argType := argTypes[i]
			expr := expressions[i]
			if argType == pql.ValueTypeScalar {
				val, err := resolveScalarArgument(expr)
				if err != nil {
					return err
				}

				argValues = append(argValues, val)
			} else if argType == pql.ValueTypeString {
				stringValues = append(stringValues, expr.(*pql.StringLiteral).Val)
			} else {
				if e, ok := expr.(*pql.MatrixSelector); ok {
					argValues = append(argValues, e.Range)
				}

				if err := p.walk(expr); err != nil {
					return err
				}
			}
		}

		// NB: Variadic function with additional args that are appended to the end
		// of the arg list.
		if variadic != 0 && exprCount > numExpectedValues {
			for _, expr := range expressions[numExpectedValues:] {
				if argTypes[argCount-1] == pql.ValueTypeString {
					stringValues = append(stringValues, expr.(*pql.StringLiteral).Val)
				} else {
					s, err := resolveScalarArgument(expr)
					if err != nil {
						return err
					}

					argValues = append(argValues, s)
				}
			}
		}

		op, ok, err := p.parseFunctionExpr(n.Func.Name, argValues,
			stringValues, hasValue, n.Args.String(), p.tagOpts)
		if err != nil {
			return err
		}
		fmt.Printf("parseFunctionExpr: %v \n", op)

		if !ok {
			return nil
		}

		opTransform := parser.NewTransformFromOperation(op, p.transformLen())
		if op.OpType() != scalar.TimeType {
			p.edges = append(p.edges, parser.Edge{
				ParentID: p.lastTransformID(),
				ChildID:  opTransform.ID,
			})
		}

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
		op, err := newScalarOperator(n, p.tagOpts)
		if err != nil {
			return err
		}

		opTransform := parser.NewTransformFromOperation(op, p.transformLen())
		p.transforms = append(p.transforms, opTransform)
		return nil

	case *pql.ParenExpr:
		// Evaluate inside of paren expressions
		return p.walk(n.Expr)

	case *pql.UnaryExpr:
		err := p.walk(n.Expr)
		if err != nil {
			return err
		}

		unaryOp, err := getUnaryOpType(n.Op)
		if err != nil {
			return err
		}

		return p.addLazyUnaryTransform(unaryOp)

	default:
		return fmt.Errorf("promql.Walk: unhandled node type %T, %v", node, node)
	}
}
