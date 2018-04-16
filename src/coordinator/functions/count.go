package functions

import "fmt"

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
