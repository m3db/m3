package promql

import (
	"github.com/m3db/m3coordinator/functions"
	"github.com/m3db/m3coordinator/parser"
	"github.com/m3db/m3coordinator/parser/common"

	"github.com/prometheus/prometheus/promql"
)

// NewSelectorFromVector creates a new fetchop
func NewSelectorFromVector(n *promql.VectorSelector) parser.Operation {
	// TODO: convert n.LabelMatchers to Matchers
	return functions.FetchOp{Name: n.Name, Offset: n.Offset, Matchers: nil}
}

// NewSelectorFromMatrix creates a new fetchop
func NewSelectorFromMatrix(n *promql.MatrixSelector) parser.Operation {
	// TODO: convert n.LabelMatchers to Matchers
	return functions.FetchOp{Name: n.Name, Offset: n.Offset, Matchers: nil, Range: n.Range}
}

// NewOperator creates a new operator based on the type
func NewOperator(opType promql.ItemType) parser.Operation {
	switch getOpType(opType) {
	case functions.CountType:
		return functions.CountOp{}
	}
	// TODO: handle other types
	return nil
}

func getOpType(opType promql.ItemType) string {
	switch opType {
	case promql.ItemType(itemCount):
		return functions.CountType
	default:
		return common.UnknownOpType
	}
}
