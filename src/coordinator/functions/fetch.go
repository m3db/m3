package functions

import (
	"fmt"
	"time"

	"github.com/m3db/m3coordinator/models"
)

// FetchType gets the series from storage
const FetchType = "fetch"

// FetchOp stores required properties for fetch
type FetchOp struct {
	Name     string
	Range    time.Duration
	Offset   time.Duration
	Matchers models.Matchers
}

// OpType for the operator
func (o FetchOp) OpType() string {
	return FetchType
}

// String representation
func (o FetchOp) String() string {
	return fmt.Sprintf("type: %s. name: %s, range: %v, offset: %v, matchers: %v", o.OpType(), o.Name, o.Range, o.Offset, o.Matchers)
}
