package functions

import (
	"context"
	"fmt"
	"time"

	"github.com/m3db/m3coordinator/executor/transform"
	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/parser"
	"github.com/m3db/m3coordinator/storage"
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

// FetchNode is the execution node
type FetchNode struct {
	op         FetchOp
	controller *transform.Controller
	storage    storage.Storage
}

// OpType for the operator
func (o FetchOp) OpType() string {
	return FetchType
}

// String representation
func (o FetchOp) String() string {
	return fmt.Sprintf("type: %s. name: %s, range: %v, offset: %v, matchers: %v", o.OpType(), o.Name, o.Range, o.Offset, o.Matchers)
}

// Node creates an execution node
func (o FetchOp) Node(controller *transform.Controller, storage storage.Storage) parser.Source {
	return &FetchNode{op: o, controller: controller, storage: storage}
}

// Execute runs the fetch node operation
func (n *FetchNode) Execute(ctx context.Context) error {
	return fmt.Errorf("not implemented")
}
