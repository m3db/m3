package promql

import (
	"fmt"

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

func (p *promParser) DAG() (parser.Transforms, parser.Edges, error) {
	return walk(p.expr)
}

func (p *promParser) String() string {
	return p.expr.String()
}

func walk(node pql.Node) (parser.Transforms, parser.Edges, error) {
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
		edges = append(edges, &parser.Edge{
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
		return []*parser.Transform{parser.NewTransformFromOperation(operation, 0)}, nil, nil

	case *pql.VectorSelector:
		operation := NewSelectorFromVector(n)
		return []*parser.Transform{parser.NewTransformFromOperation(operation, 0)}, nil, nil

	case *pql.NumberLiteral, *pql.StringLiteral:

	default:
		return nil, nil, fmt.Errorf("promql.Walk: unhandled node type %T", node)
	}

	// TODO: This should go away once all cases have been implemented
	return nil, nil, fmt.Errorf("not implemented")
}
