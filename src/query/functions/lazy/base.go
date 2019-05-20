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

package lazy

import (
	"fmt"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
)

const (
	// OffsetType offsets incoming data point timestamps and metadata by the given offset.
	OffsetType = "offset"

	// UnaryType offsets incoming data point values by the given operator.
	UnaryType = "unary"
)

const (
	ItemAdd = "+"
	ItemSub = "-"
)

// NewLazyOp creates a new lazy operation
func NewLazyOp(
	opType string,
	lazyOpts block.LazyOptions,
) (parser.Params, error) {
	return baseOp{
		opType:   opType,
		lazyOpts: lazyOpts,
	}, nil
}

// baseOp stores required properties for the baseOp
type baseOp struct {
	opType   string
	lazyOpts block.LazyOptions
}

func (o baseOp) OpType() string {
	return o.opType
}

func (o baseOp) String() string {
	return fmt.Sprintf("type: %s", o.opType)
}

func (o baseOp) Node(
	controller *transform.Controller,
	_ transform.Options,
) transform.OpNode {
	return &baseNode{
		op:         o,
		controller: controller,
	}
}

type baseNode struct {
	op         baseOp
	controller *transform.Controller
}

func (n *baseNode) Params() parser.Params {
	return n.op
}

func (n *baseNode) processBlock(b block.Block) block.Block {
	return block.NewLazyBlock(b, n.op.lazyOpts)
}

func (n *baseNode) Process(
	queryCtx *models.QueryContext,
	_ parser.NodeID,
	b block.Block,
) error {
	nextBlock := n.processBlock(b)
	return n.controller.Process(queryCtx, nextBlock)
}
