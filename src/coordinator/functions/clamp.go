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

var emptyClamp = ClampOp{}

// ClampOp stores required properties for clamp
// TODO(nikunj): Make clamp a lazy function
type ClampOp struct {
	opType string
	scalar float64
}

// NewClampOp creates a new clamp op based on the type and arguments
func NewClampOp(args []interface{}, optype string) (ClampOp, error) {
	if len(args) != 1 {
		return emptyClamp, fmt.Errorf("invalid number of args for clamp: %d", len(args))
	}

	if optype != ClampMinType && optype != ClampMaxType {
		return emptyClamp, fmt.Errorf("unknown clamp type: %s", optype)
	}

	scalar, ok := args[0].(float64)
	if !ok {
		return emptyClamp, fmt.Errorf("unable to cast to scalar argument: %v", args[0])
	}

	return ClampOp{
		opType: optype,
		scalar: scalar,
	}, nil
}

// OpType for the operator
func (o ClampOp) OpType() string {
	return o.opType
}

// String representation
func (o ClampOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

// Node creates an execution node
func (o ClampOp) Node(controller *transform.Controller) transform.OpNode {
	fn := math.Min
	if o.opType == ClampMinType {
		fn = math.Max
	}

	return &ClampNode{op: o, controller: controller, clampFn: fn}
}

// ClampNode is an execution node
type ClampNode struct {
	op         ClampOp
	clampFn    func(x, y float64) float64
	controller *transform.Controller
}

// Process the block
func (c *ClampNode) Process(ID parser.NodeID, b block.Block) error {
	stepIter, err := b.StepIter()
	if err != nil {
		return err
	}

	builder, err := c.controller.BlockBuilder(stepIter.Meta(), stepIter.SeriesMeta())
	if err != nil {
		return err
	}

	if err := builder.AddCols(stepIter.StepCount()); err != nil {
		return err
	}

	scalar := c.op.scalar
	for index := 0; stepIter.Next(); index++ {
		step, err := stepIter.Current()
		if err != nil {
			return err
		}

		values := step.Values()
		for _, value := range values {
			builder.AppendValue(index, c.clampFn(value, scalar))
		}
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()
	return c.controller.Process(nextBlock)
}
