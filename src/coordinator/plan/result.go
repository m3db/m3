package plan

import "fmt"

// ResultType gets the results
const ResultType = "result"

// ResultOp is resonsible for delivering results to the clients
type ResultOp struct {
}

// OpType is the type of operation
func (r *ResultOp) OpType() string {
	return ResultType
}

// String representation
func (r *ResultOp) String() string {
	return fmt.Sprintf("type: %s", r.OpType())
}
