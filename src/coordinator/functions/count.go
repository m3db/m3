package functions

import (
	"fmt"

	"github.com/m3db/m3coordinator/executor/transform"
	"github.com/m3db/m3coordinator/parser"
)

// CountType counts number of elements in the vector
const CountType = "count"

// CountOp stores required properties for count
type CountOp struct {
}

// OpType for the operator
func (o CountOp) OpType() string {
	return CountType
}

// String representation
func (o CountOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

// Node creates an execution node
func (o CountOp) Node(controller *transform.Controller) parser.OpNode {
	return &CountNode{op: o, controller: controller}
}

// CountNode is an execution node
type CountNode struct {
	op         CountOp
	controller *transform.Controller
}
