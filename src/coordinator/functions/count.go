package functions

// CountType counts number of elements in the vector
const CountType = "count"

// CountOp stores required properties for count
type CountOp struct {
}

// OpType for the operator
func (o *CountOp) OpType() string {
	return CountType
}
