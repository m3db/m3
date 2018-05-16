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

// CountType counts all non nan elements in a list of series
const CountType = "count"

// CountOp stores required properties for count
type CountOp struct {
}

// OpType for the operator
func (o CountOp) OpType() string {
	return CountType
}

// String representation
func (o CountOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

// Node creates an execution node
func (o CountOp) Node(controller *transform.Controller) transform.OpNode {
	return &CountNode{op: o, controller: controller}
}

// CountNode is an execution node
type CountNode struct {
	op         CountOp
	controller *transform.Controller
}

// Process the block
func (c *CountNode) Process(ID parser.NodeID, block block.Block) error {
	defer block.Close()
	builder, err := c.controller.BlockBuilder(block.Meta())
	if err != nil {
		return err
	}

	stepIter := block.StepIter()
	if err := builder.AddCols(stepIter.Steps()); err != nil {
		return err
	}

	for index := 0; stepIter.Next(); index++ {
		step := stepIter.Current()
		values := step.Values()
		count := 0.0
		for _, value := range values {
			if !math.IsNaN(value) {
				count++
			}
		}

		builder.AppendValue(index, count)
	}

	return c.controller.Process(builder.Build())
}
