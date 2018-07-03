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

package logical

import (
	"fmt"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/executor/transform"
	"github.com/m3db/m3db/src/coordinator/parser"
)

// BaseOp stores required properties for logical operations
type BaseOp struct {
	OperatorType string
	LNode        parser.NodeID
	RNode        parser.NodeID
	Matching     *VectorMatching
	ReturnBool   bool
	ProcessorFn  MakeProcessor
}

// OpType for the operator
func (o BaseOp) OpType() string {
	return o.OperatorType
}

// String representation
func (o BaseOp) String() string {
	return fmt.Sprintf("type: %s, lnode: %s, rnode: %s", o.OpType(), o.LNode, o.RNode)
}

// Node creates an execution node
func (o BaseOp) Node(controller *transform.Controller) transform.OpNode {
	return &BaseNode{
		op:        o, controller: controller,
		cache:     transform.NewBlockCache(),
		processor: o.ProcessorFn(o, controller),
	}
}

// BaseNode is an execution node
type BaseNode struct {
	op         BaseOp
	controller *transform.Controller
	cache      *transform.BlockCache
	processor  Processor
}

// Process processes a block
func (c *BaseNode) Process(ID parser.NodeID, b block.Block) error {
	var lhs block.Block
	var rhs block.Block
	if c.op.LNode == ID {
		rBlock, ok := c.cache.Get(c.op.RNode)
		if !ok {
			c.cache.Add(ID, b)
			return nil
		}

		rhs = rBlock
		lhs = b
	} else if c.op.RNode == ID {
		lBlock, ok := c.cache.Get(c.op.LNode)
		if !ok {
			c.cache.Add(ID, b)
			return nil
		}

		lhs = lBlock
		rhs = b
	}

	nextBlock, err := c.processor.Process(lhs, rhs)
	c.cache.Remove(c.op.LNode)
	c.cache.Remove(c.op.RNode)
	if err != nil {
		return err
	}

	defer nextBlock.Close()
	return c.controller.Process(nextBlock)
}
