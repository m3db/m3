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

package binary

import (
	"fmt"
	"sync"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
)

type baseOp struct {
	OperatorType string
	processFunc  processFunc
	params       NodeParams
}

// NodeParams describes the types of nodes used
// for binary operations
type NodeParams struct {
	LNode, RNode         parser.NodeID
	LIsScalar, RIsScalar bool
	ReturnBool           bool
	VectorMatching       *VectorMatching
}

// OpType for the operator
func (o baseOp) OpType() string {
	return o.OperatorType
}

// String representation
func (o baseOp) String() string {
	return fmt.Sprintf("type: %s, lnode: %s, rnode: %s", o.OpType(), o.params.LNode, o.params.RNode)
}

// Node creates an execution node
func (o baseOp) Node(controller *transform.Controller, _ transform.Options) transform.OpNode {
	return &baseNode{
		op:         o,
		process:    o.processFunc,
		controller: controller,
		cache:      transform.NewBlockCache(),
	}
}

// ArithmeticFunction returns the arithmetic function for this operation type.
func ArithmeticFunction(opType string, returnBool bool) (Function, error) {
	if fn, ok := arithmeticFuncs[opType]; ok {
		return fn, nil
	}

	// For comparison functions, check if returning bool or not and return the
	// appropriate one
	if returnBool {
		opType += returnBoolSuffix
	}

	if fn, ok := comparisonFuncs[opType]; ok {
		return fn, nil
	}

	return nil, errNoMatching
}

// NewOp creates a new binary operation
func NewOp(
	opType string,
	params NodeParams,
) (parser.Params, error) {
	fn, ok := buildLogicalFunction(opType, params)
	if !ok {
		fn, ok = buildArithmeticFunction(opType, params)
		if !ok {
			fn, ok = buildComparisonFunction(opType, params)
			if !ok {
				return baseOp{}, fmt.Errorf("operator not supported: %s", opType)
			}
		}
	}

	return baseOp{
		OperatorType: opType,
		processFunc:  fn,
		params:       params,
	}, nil
}

type baseNode struct {
	op         baseOp
	process    processFunc
	controller *transform.Controller
	cache      *transform.BlockCache
	mu         sync.Mutex
}

type processFunc func(*models.QueryContext, block.Block, block.Block, *transform.Controller) (block.Block, error)

// Process processes a block
func (n *baseNode) Process(queryCtx *models.QueryContext, ID parser.NodeID, b block.Block) error {
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

	nextBlock, err := n.process(queryCtx, lhs, rhs, n.controller)
	if err != nil {
		return err
	}

	defer nextBlock.Close()
	return n.controller.Process(queryCtx, nextBlock)
}

// computeOrCache figures out if both lhs and rhs are available, if not then it caches the incoming block
func (n *baseNode) computeOrCache(ID parser.NodeID, b block.Block) (block.Block, block.Block, error) {
	var lhs, rhs block.Block
	n.mu.Lock()
	defer n.mu.Unlock()
	op := n.op
	params := op.params
	if params.LNode == ID {
		rBlock, ok := n.cache.Get(params.RNode)
		if !ok {
			return lhs, rhs, n.cache.Add(ID, b)
		}

		rhs = rBlock
		lhs = b
	} else if params.RNode == ID {
		lBlock, ok := n.cache.Get(params.LNode)
		if !ok {
			return lhs, rhs, n.cache.Add(ID, b)
		}

		lhs = lBlock
		rhs = b
	}

	return lhs, rhs, nil
}

func (n *baseNode) cleanup() {
	n.mu.Lock()
	defer n.mu.Unlock()
	params := n.op.params
	n.cache.Remove(params.LNode)
	n.cache.Remove(params.RNode)
}
