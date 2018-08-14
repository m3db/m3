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

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/parser"
)

type logicalOp struct {
	OperatorType string
	LNode        parser.NodeID
	RNode        parser.NodeID
	makeBlock    makeBlockFn
	Matching     *VectorMatching
	ReturnBool   bool
}

// OpType for the operator
func (o logicalOp) OpType() string {
	return o.OperatorType
}

// String representation
func (o logicalOp) String() string {
	return fmt.Sprintf("type: %s, lnode: %s, rnode: %s", o.OpType(), o.LNode, o.RNode)
}

// Node creates an execution node
func (o logicalOp) Node(controller *transform.Controller, _ transform.Options) transform.OpNode {
	return &logicalNode{
		controller: controller,
		cache:      transform.NewBlockCache(),
		op:         o,
	}
}

type logicalNode struct {
	op         logicalOp
	controller *transform.Controller
	cache      *transform.BlockCache
	mu         sync.Mutex
}

type makeBlockFn func(
	logicalNode *logicalNode,
	lIter, rIter block.StepIter,
) (block.Block, error)

// NewLogicalOp creates a new logical operation
func NewLogicalOp(
	opType string,
	lNode parser.NodeID,
	rNode parser.NodeID,
	matching *VectorMatching,
) (parser.Params, error) {
	var makeBlock makeBlockFn
	switch opType {
	case AndType:
		makeBlock = makeAndBlock
	case OrType:
		makeBlock = makeOrBlock
	case UnlessType:
		makeBlock = makeUnlessBlock
	default:
		return logicalOp{}, fmt.Errorf("operator not supported: %s", opType)
	}

	return logicalOp{
		OperatorType: opType,
		LNode:        lNode,
		RNode:        rNode,
		Matching:     matching,
		makeBlock:    makeBlock,
	}, nil
}

// Process processes a block
func (n *logicalNode) Process(ID parser.NodeID, b block.Block) error {
	lhs, rhs, err := n.computeOrCache(ID, b)
	if err != nil {
		// Clean up any blocks from cache
		n.cleanup()
		return err
	}

	// Both blocks are not ready
	if lhs == nil || rhs == nil {
		return nil
	}

	n.cleanup()

	nextBlock, err := n.process(lhs, rhs)
	if err != nil {
		return err
	}

	defer nextBlock.Close()
	return n.controller.Process(nextBlock)
}

// processes two logical blocks, performing a logical operation on them
func (n *logicalNode) process(lhs, rhs block.Block) (block.Block, error) {
	lIter, err := lhs.StepIter()
	if err != nil {
		return nil, err
	}

	rIter, err := rhs.StepIter()
	if err != nil {
		return nil, err
	}

	if lIter.StepCount() != rIter.StepCount() {
		return nil, errMismatchedStepCounts
	}

	return n.op.makeBlock(n, lIter, rIter)
}

// computeOrCache figures out if both lhs and rhs are available, if not then it caches the incoming block
func (n *logicalNode) computeOrCache(ID parser.NodeID, b block.Block) (block.Block, block.Block, error) {
	var lhs, rhs block.Block
	n.mu.Lock()
	defer n.mu.Unlock()
	op := n.op
	if op.LNode == ID {
		rBlock, ok := n.cache.Get(op.RNode)
		if !ok {
			return lhs, rhs, n.cache.Add(ID, b)
		}

		rhs = rBlock
		lhs = b
	} else if op.RNode == ID {
		lBlock, ok := n.cache.Get(op.LNode)
		if !ok {
			return lhs, rhs, n.cache.Add(ID, b)
		}

		lhs = lBlock
		rhs = b
	}

	return lhs, rhs, nil
}

func (n *logicalNode) cleanup() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cache.Remove(n.op.LNode)
	n.cache.Remove(n.op.RNode)
}
