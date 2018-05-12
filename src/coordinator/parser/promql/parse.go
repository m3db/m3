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

	"github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/parser"

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

	return &promParser{expr: expr}, nil
}

func (p *promParser) DAG() (parser.Nodes, parser.Edges, error) {
	return walk(p.expr)
}

func (p *promParser) String() string {
	return p.expr.String()
}

func walk(node pql.Node) (parser.Nodes, parser.Edges, error) {
	if node == nil {
		return nil, nil, nil
	}

	switch n := node.(type) {
	case pql.Statements:
	case *pql.AlertStmt:

	case *pql.EvalStmt:

	case *pql.RecordStmt:

	case pql.Expressions:
	case *pql.AggregateExpr:
		transforms, edges, err := walk(n.Expr)
		if err != nil {
			return nil, nil, err
		}

		opTransform := parser.NewTransformFromOperation(NewOperator(n.Op), len(transforms))
		edges = append(edges, parser.Edge{
			ParentID: transforms[len(transforms)-1].ID,
			ChildID:  opTransform.ID,
		})
		transforms = append(transforms, opTransform)
		// TODO: handle labels, params
		return transforms, edges, nil
	case *pql.BinaryExpr:

	case *pql.Call:

	case *pql.ParenExpr:

	case *pql.UnaryExpr:

	case *pql.MatrixSelector:
		operation := NewSelectorFromMatrix(n)
		return []parser.Node{parser.NewTransformFromOperation(operation, 0)}, nil, nil

	case *pql.VectorSelector:
		operation := NewSelectorFromVector(n)
		return []parser.Node{parser.NewTransformFromOperation(operation, 0)}, nil, nil

	case *pql.NumberLiteral, *pql.StringLiteral:

	default:
		return nil, nil, fmt.Errorf("promql.Walk: unhandled node type %T", node)
	}

	// TODO: This should go away once all cases have been implemented
	return nil, nil, errors.ErrNotImplemented
}
