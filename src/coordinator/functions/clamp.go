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

// ClampMinType ensures all values except NaNs are greater than or equal to the provided argument
const ClampMinType = "clamp_min"

// ClampMaxType ensures all values except NaNs are lesser than or equal to provided argument
const ClampMaxType = "clamp_max"

// ClampOp stores required properties for clamp
type ClampOp struct {
	optype string
	scalar float64
}

// NewClampOp creates a new clamp op based on the type and arguments
func NewClampOp(args []interface{}, optype string) (ClampOp, error) {
	if len(args) != 1 {
		return ClampOp{}, fmt.Errorf("invalid number of args for clamp: %d", len(args))
	}

	if optype != ClampMinType && optype != ClampMaxType {
		return ClampOp{}, fmt.Errorf("unknown clamp type: %s", optype)
	}

	scalar, ok := args[0].(float64)
	if !ok {
		return ClampOp{}, fmt.Errorf("unable to cast argument: %v", args[0])
	}

	return ClampOp{
		optype: optype,
		scalar: scalar,
	}, nil
}

// OpType for the operator
func (o ClampOp) OpType() string {
	return o.optype
}

// String representation
func (o ClampOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

// Node creates an execution node
func (o ClampOp) Node(controller *transform.Controller) transform.OpNode {
	return &ClampNode{op: o, controller: controller}
}

// ClampNode is an execution node
type ClampNode struct {
	op         ClampOp
	controller *transform.Controller
}

// Process the block
func (c *ClampNode) Process(ID parser.NodeID, b block.Block) error {
	builder, err := c.controller.BlockBuilder(b.Meta(), b.SeriesMeta())
	if err != nil {
		return err
	}

	stepIter := b.StepIter()
	if err := builder.AddCols(b.StepCount()); err != nil {
		return err
	}

	fn := math.Min
	if c.op.optype == ClampMinType {
		fn = math.Max
	}

	for index := 0; stepIter.Next(); index++ {
		step := stepIter.Current()
		values := step.Values()
		for _, value := range values {
			builder.AppendValue(index, fn(value, c.op.scalar))
		}
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()
	return c.controller.Process(nextBlock)
}
