// Copyright (c) 2019 Uber Technologies, Inc.
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

package aggregation

import (
	"fmt"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
)

const (
	// AbsentType returns 1 if there are no elements in this step, or if no series
	// are present in the current block.
	M3CountType = "m3_count"
)

// NewAbsentOp creates a new absent operation.
func NewM3CountOp() parser.Params {
	return newM3CountOp()
}

// m3CountOp stores required properties for absent ops.
type m3CountOp struct{}

// OpType for the operator.
func (o m3CountOp) OpType() string {
	return M3CountType
}

// String representation.
func (o m3CountOp) String() string {
	return "type: m3_count"
}

// Node creates an execution node.
func (o m3CountOp) Node(
	controller *transform.Controller,
	_ transform.Options,
) transform.OpNode {
	fmt.Printf("creating node \n")
	return &M3CountNode{
		op:         o,
		controller: controller,
	}
}

func newM3CountOp() m3CountOp {
	return m3CountOp{}
}

// absentNode is different from base node as it uses no grouping and has
// special handling for the 0-series case.
type M3CountNode struct {
	op         parser.Params
	controller *transform.Controller
}

func (n *M3CountNode) Params() parser.Params {
	return n.op
}

func (n *M3CountNode) Process(queryCtx *models.QueryContext,
	ID parser.NodeID, b block.Block) error {
	return transform.ProcessSimpleBlock(n, n.controller, queryCtx, ID, b)
}

func (n *M3CountNode) ProcessBlock(
	queryCtx *models.QueryContext,
	ID parser.NodeID,
	bl block.Block,
) (block.Block, error) {
	// TODO probably here can mangle
	return bl, nil
}
