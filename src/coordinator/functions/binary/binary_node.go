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
	"math"
	"sync"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/executor/transform"
	"github.com/m3db/m3db/src/coordinator/parser"
)

const (
	// PlusType adds datapoints in both series
	PlusType = "+"

	// MinusType subtracts rhs from lhs
	MinusType = "-"

	// MultiplyType multiplies datapoints by series
	MultiplyType = "*"

	// ExpType raises lhs to the power of rhs
	ExpType = "^"

	// DivType divides datapoints by series
	// Special cases are:
	// 	 X / 0 = +Inf
	// 	-X / 0 = -Inf
	// 	 0 / 0 =  NaN
	DivType = "/"

	// ModType takes the modulo of lhs by rhs
	// Special cases are:
	// 	 X % 0 = NaN
	// 	 NaN % X = NaN
	// 	 X % NaN = NaN
	ModType = "%"
)

type mathFunc func(x, y float64) float64

var (
	mathFuncs = map[string]mathFunc{
		PlusType:     func(x, y float64) float64 { return x + y },
		MinusType:    func(x, y float64) float64 { return x - y },
		MultiplyType: func(x, y float64) float64 { return x * y },
		DivType:      func(x, y float64) float64 { return x / y },
		ModType:      math.Mod,
		ExpType:      math.Pow,
	}
)

type binaryOp struct {
	OperatorType string
	LNode        parser.NodeID
	RNode        parser.NodeID
	fn           mathFunc
	anyScalars   bool
	ReturnBool   bool
}

// OpType for the operator
func (o binaryOp) OpType() string {
	return o.OperatorType
}

// String representation
func (o binaryOp) String() string {
	return fmt.Sprintf("type: %s, lnode: %s, rnode: %s", o.OpType(), o.LNode, o.RNode)
}

// Node creates an execution node
func (o binaryOp) Node(controller *transform.Controller) transform.OpNode {
	return &binaryNode{
		controller: controller,
		cache:      transform.NewBlockCache(),
		op:         o,
	}
}

type binaryNode struct {
	op         binaryOp
	controller *transform.Controller
	cache      *transform.BlockCache
	mu         sync.Mutex
}

// NewBinaryOp creates a new binary operation
func NewBinaryOp(
	opType string,
	lNode parser.NodeID,
	rNode parser.NodeID,
	anyScalars bool,
) (parser.Params, error) {
	fn, ok := mathFuncs[opType]
	if !ok {
		return binaryOp{}, fmt.Errorf("operator not supported: %s", opType)
	}

	return binaryOp{
		OperatorType: opType,
		LNode:        lNode,
		RNode:        rNode,
		fn:           fn,
		anyScalars:   anyScalars,
	}, nil
}

// Process processes a block
func (n *binaryNode) Process(ID parser.NodeID, b block.Block) error {
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
func (n *binaryNode) process(lhs, rhs block.Block) (block.Block, error) {
	lIter, err := lhs.StepIter()
	if err != nil {
		return nil, err
	}

	rIter, err := rhs.StepIter()
	if err != nil {
		return nil, err
	}

	lMeta, rSeriesMeta := lIter.Meta(), rIter.SeriesMeta()
	builder, err := n.controller.BlockBuilder(lMeta, rSeriesMeta)
	if err != nil {
		return nil, err
	}

	if err := builder.AddCols(lIter.StepCount()); err != nil {
		return nil, err
	}

	for index := 0; lIter.Next() && rIter.Next(); index++ {
		lStep, err := lIter.Current()
		if err != nil {
			return nil, err
		}

		lValues := lStep.Values()
		rStep, err := rIter.Current()
		if err != nil {
			return nil, err
		}

		rValues := rStep.Values()
		fn := n.op.fn

		for idx, value := range lValues {
			builder.AppendValue(index, fn(value, rValues[idx]))
		}
	}

	return builder.Build(), nil
}

// computeOrCache figures out if both lhs and rhs are available, if not then it caches the incoming block
func (n *binaryNode) computeOrCache(ID parser.NodeID, b block.Block) (block.Block, block.Block, error) {
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

func (n *binaryNode) cleanup() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cache.Remove(n.op.LNode)
	n.cache.Remove(n.op.RNode)
}
