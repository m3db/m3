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

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/functions/binary"
	"github.com/m3db/m3/src/query/functions/lazy"
	"github.com/m3db/m3/src/query/functions/scalar"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"

	pql "github.com/prometheus/prometheus/promql"
)

type promParser struct {
	expr    pql.Expr
	tagOpts models.TagOptions
}

// Parse takes a promQL string and converts parses it into a DAG
func Parse(q string, tagOpts models.TagOptions) (parser.Parser, error) {
	expr, err := pql.ParseExpr(q)
	if err != nil {
		return nil, err
	}

	return &promParser{
		expr:    expr,
		tagOpts: tagOpts,
	}, nil
}

func (p *promParser) DAG() (parser.Nodes, parser.Edges, error) {
	state := &parseState{tagOpts: p.tagOpts}
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
	tagOpts    models.TagOptions
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

func validOffset(offset time.Duration) error {

	return nil
}

func (p *parseState) addLazyUnaryTransform(unaryOp string) error {
	// NB: if unary type is "+", we do not apply any offsets.
	if unaryOp == binary.PlusType {
		return nil
	}

	vt := func(val float64) float64 { return val * -1.0 }
	lazyOpts := block.NewLazyOpts().SetValueTransform(vt)

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
		tt = func(t time.Time) time.Time { return t.Add(offset) }
		mt = func(meta block.Metadata) block.Metadata {
			meta.Bounds.Start = meta.Bounds.Start.Add(offset)
			return meta
		}
	)

	lazyOpts := block.NewLazyOpts().
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

func (p *parseState) walk(node pql.Node) error {
	if node == nil {
		return nil
	}

	switch n := node.(type) {
	case *pql.AggregateExpr:
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
		operation, err := NewSelectorFromMatrix(n, p.tagOpts)
		if err != nil {
			return err
		}

		p.transforms = append(p.transforms, parser.NewTransformFromOperation(operation, p.transformLen()))
		return p.addLazyOffsetTransform(n.Offset)

	case *pql.VectorSelector:
		operation, err := NewSelectorFromVector(n, p.tagOpts)
		if err != nil {
			return err
		}

		p.transforms = append(p.transforms, parser.NewTransformFromOperation(operation, p.transformLen()))
		return p.addLazyOffsetTransform(n.Offset)

	case *pql.Call:
		expressions := n.Args
		argTypes := n.Func.ArgTypes
		argValues := make([]interface{}, 0, len(expressions))
		stringValues := make([]string, 0, len(expressions))
		for i, argType := range argTypes {
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

		if n.Func.Variadic == -1 {
			l := len(argTypes)
			for _, expr := range expressions[l:] {
				if argTypes[l-1] == pql.ValueTypeString {
					stringValues = append(stringValues, expr.(*pql.StringLiteral).Val)
				}
			}
		}

		op, ok, err := NewFunctionExpr(n.Func.Name, argValues, stringValues)
		if err != nil {
			return err
		}

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
		op, err := NewScalarOperator(n)
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
