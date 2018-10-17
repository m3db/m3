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

package linear

import (
	"fmt"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/parser"
)

const (
	// NB: Because Prometheus's sort and sort_desc only look at the last value,
	// these functions are essentially noops in M3 as we don't support instant queries.

	// SortType returns timeseries elements sorted by their values, in ascending order.
	SortType = "sort"

	// SortDescType is the same as sort, but sorts in descending order.
	SortDescType = "sort_desc"
)

// NewSortOp creates a new sort operation
func NewSortOp(opType string) (parser.Params, error) {
	if opType != SortType && opType != SortDescType {
		return sortOp{}, fmt.Errorf("operator not supported: %s", opType)
	}

	return newSortOp(opType), nil
}

// sortOp stores required properties for sort ops
type sortOp struct {
	opType string
}

// OpType for the operator
func (o sortOp) OpType() string {
	return o.opType
}

// String representation
func (o sortOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

func newSortOp(opType string) sortOp {
	return sortOp{
		opType: opType,
	}
}

// Node creates an execution node
func (o sortOp) Node(
	controller *transform.Controller,
	_ transform.Options,
) transform.OpNode {
	return &sortNode{
		op:         o,
		controller: controller,
	}
}

type sortNode struct {
	op         sortOp
	controller *transform.Controller
}

// Process the block
func (n *sortNode) Process(_ parser.NodeID, b block.Block) error {
	return n.controller.Process(b)
}
