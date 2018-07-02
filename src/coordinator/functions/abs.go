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

package functions

import (
	"fmt"
	"math"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/executor/transform"
	"github.com/m3db/m3db/src/coordinator/parser"
)

// AbsType takes absolute value of each datapoint in the series
const AbsType = "abs"

// AbsOp stores required properties for abs
type AbsOp struct {
}

// OpType for the operator
func (o AbsOp) OpType() string {
	return AbsType
}

// String representation
func (o AbsOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

// Node creates an execution node
func (o AbsOp) Node(controller *transform.Controller) transform.OpNode {
	return &AbsNode{op: o, controller: controller}
}

// AbsNode is an execution node
type AbsNode struct {
	op         AbsOp
	controller *transform.Controller
}

// Process the block
func (c *AbsNode) Process(ID parser.NodeID, b block.Block) error {
	builder, err := c.controller.BlockBuilder(b.Meta(), b.SeriesMeta())
	if err != nil {
		return err
	}

	stepIter := b.StepIter()
	if err := builder.AddCols(b.StepCount()); err != nil {
		return err
	}

	for index := 0; stepIter.Next(); index++ {
		step := stepIter.Current()
		values := step.Values()
		for _, value := range values {
			builder.AppendValue(index, math.Abs(value))
		}
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()
	return c.controller.Process(nextBlock)
}
