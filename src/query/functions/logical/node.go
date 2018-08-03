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
	"sync"

	"github.com/m3db/m3db/src/query/block"
	"github.com/m3db/m3db/src/query/executor/transform"
	"github.com/m3db/m3db/src/query/parser"
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
		controller: controller,
		cache:      transform.NewBlockCache(),
		op:         o,
		processor:  o.ProcessorFn(o, controller),
	}
}

// BaseNode is an execution node
type BaseNode struct {
	op         BaseOp
	controller *transform.Controller
	cache      *transform.BlockCache
	processor  Processor
	mu         sync.Mutex
}

// Process processes a block
func (c *BaseNode) Process(ID parser.NodeID, b block.Block) error {
	lhs, rhs, err := c.computeOrCache(ID, b)
	if err != nil {
		// Clean up any blocks from cache
		c.cleanup()
		return err
	}

	// Both blocks are not ready
	if lhs == nil || rhs == nil {
		return nil
	}

	c.cleanup()
	nextBlock, err := c.processor.Process(lhs, rhs)
	if err != nil {
		return err
	}

	defer nextBlock.Close()
	return c.controller.Process(nextBlock)
}

// computeOrCache figures out if both lhs and rhs are available, if not then it caches the incoming block
func (c *BaseNode) computeOrCache(ID parser.NodeID, b block.Block) (block.Block, block.Block, error) {
	var lhs, rhs block.Block
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.op.LNode == ID {
		rBlock, ok := c.cache.Get(c.op.RNode)
		if !ok {
			return lhs, rhs, c.cache.Add(ID, b)
		}

		rhs = rBlock
		lhs = b
	} else if c.op.RNode == ID {
		lBlock, ok := c.cache.Get(c.op.LNode)
		if !ok {
			return lhs, rhs, c.cache.Add(ID, b)
		}

		lhs = lBlock
		rhs = b
	}

	return lhs, rhs, nil
}

func (c *BaseNode) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.Remove(c.op.LNode)
	c.cache.Remove(c.op.RNode)
}
