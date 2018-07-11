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
	"math"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/executor/transform"
	"github.com/m3db/m3db/src/coordinator/parser"
)

const (
	// AndType uses values from left hand side for which there is a value in right hand side with exactly matching label sets.
	// Other elements are replaced by NaNs. The metric name and values are carried over from the left-hand side.
	AndType = "and"
)

// NewAndOp creates a new And operation
func NewAndOp(lNode parser.NodeID, rNode parser.NodeID, matching *VectorMatching) BaseOp {
	return BaseOp{
		OperatorType: AndType,
		LNode:        lNode,
		RNode:        rNode,
		Matching:     matching,
		ProcessorFn:  NewAndNode,
	}
}

// AndNode is a node for And operation
type AndNode struct {
	op         BaseOp
	controller *transform.Controller
}

// NewAndNode creates a new AndNode
func NewAndNode(op BaseOp, controller *transform.Controller) Processor {
	return &AndNode{
		op:         op,
		controller: controller,
	}
}

// Process processes two logical blocks, performing And operation on them
func (c *AndNode) Process(lhs, rhs block.Block) (block.Block, error) {
	intersection := c.intersect(lhs.SeriesMeta(), rhs.SeriesMeta())
	builder, err := c.controller.BlockBuilder(lhs.Meta(), lhs.SeriesMeta())
	if err != nil {
		return nil, err
	}

	lIter, err := lhs.StepIter()
	if err != nil {
		return nil, err
	}

	rIter, err := rhs.StepIter()
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

		for idx, value := range lValues {
			rIdx := intersection[idx]
			if rIdx < 0 || math.IsNaN(rValues[rIdx]) {
				builder.AppendValue(index, math.NaN())
				continue
			}

			builder.AppendValue(index, value)
		}
	}

	return builder.Build(), nil
}

// intersect returns the slice of rhs indices if there is a match with a corresponding lhs index. If no match is found, it returns -1
func (c *AndNode) intersect(lhs, rhs []block.SeriesMeta, ) []int {
	idFunction := hashFunc(c.op.Matching.On, c.op.Matching.MatchingLabels...)
	// The set of signatures for the right-hand side.
	rightSigs := make(map[uint64]int, len(rhs))
	for idx, meta := range rhs {
		rightSigs[idFunction(meta.Tags)] = idx
	}

	matches := make([]int, len(lhs))
	for i, ls := range lhs {
		// If there's a matching entry in the right-hand side Vector, add the sample.
		if idx, ok := rightSigs[idFunction(ls.Tags)]; ok {
			matches[i] = idx
		} else {
			matches[i] = -1
		}
	}

	return matches
}
