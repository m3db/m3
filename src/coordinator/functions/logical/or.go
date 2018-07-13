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

//TODO: generalize logical functions?
const (
	// OrType uses all values from left hand side, and appends values from the right hand side which do
	// not have corresponding tags on the right
	OrType = "or"
)

// NewOrOp creates a new Or operation
func NewOrOp(lNode parser.NodeID, rNode parser.NodeID, matching *VectorMatching) BaseOp {
	return BaseOp{
		OperatorType: OrType,
		LNode:        lNode,
		RNode:        rNode,
		Matching:     matching,
		ProcessorFn:  NewOrNode,
	}
}

// OrNode is a node for And operation
type OrNode struct {
	op         BaseOp
	controller *transform.Controller
}

// NewOrNode creates a new OrNode
func NewOrNode(op BaseOp, controller *transform.Controller) Processor {
	return &OrNode{
		op:         op,
		controller: controller,
	}
}

// Process processes two logical blocks, performing Or operation on them
func (c *OrNode) Process(lhs, rhs block.Block) (block.Block, error) {
	lIter, err := lhs.StepIter()
	if err != nil {
		return nil, err
	}

	rIter, err := rhs.StepIter()
	if err != nil {
		return nil, err
	}

	builder, err := c.controller.BlockBuilder(lIter.Meta(), lIter.SeriesMeta())
	if err != nil {
		return nil, err
	}
	missingIndices := c.missing(lIter.SeriesMeta(), rIter.SeriesMeta())
	numMissing := len(missingIndices)
	stepCount := numMissing + lIter.StepCount()

	if err := builder.AddCols(stepCount); err != nil {
		return nil, err
	}

	for index := 0; lIter.Next(); index++ {
		lStep, err := lIter.Current()
		if err != nil {
			return nil, err
		}

		lValues := lStep.Values()
		for _, value := range lValues {
			builder.AppendValue(index, value)
		}
	}

	index := 0
	for _, missingIndex := range missingIndices {
		for ; index != missingIndex; index++ {
			if !rIter.Next() {
				return nil, fmt.Errorf("missing index not found in rhs")
			}
		}

		rStep, err := rIter.Current()
		if err != nil {
			return nil, err
		}

		rValues := rStep.Values()
		for _, value := range rValues {
			builder.AppendValue(index, value)
		}
	}

	return builder.Build(), nil
}

// missing returns the slice of rhs indices for which there are no corresponding
// indices on the lhs
func (c *OrNode) missing(lhs, rhs []block.SeriesMeta) []int {
	idFunction := hashFunc(c.op.Matching.On, c.op.Matching.MatchingLabels...)
	// The set of signatures for the left-hand side.
	leftSigs := make(map[uint64]int, len(lhs))
	for idx, meta := range lhs {
		leftSigs[idFunction(meta.Tags)] = idx
	}

	matches := make([]int, 0, 10)
	for i, rs := range rhs {
		// If there's no matching entry in the left-hand side Vector, add the sample.
		if _, ok := leftSigs[idFunction(rs.Tags)]; !ok {
			matches = append(matches, i)
		}
	}

	return matches
}
