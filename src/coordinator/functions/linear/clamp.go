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

package linear

import (
	"fmt"
	"math"

	"github.com/m3db/m3db/src/coordinator/executor/transform"
)

// ClampMinType ensures all values except NaNs are greater than or equal to the provided argument
const ClampMinType = "clamp_min"

// ClampMaxType ensures all values except NaNs are lesser than or equal to provided argument
const ClampMaxType = "clamp_max"

type clampOp struct {
	opType string
	scalar float64
}

// NewClampOp creates a new clamp op based on the type and arguments
func NewClampOp(args []interface{}, optype string) (BaseOp, error) {
	if len(args) != 1 {
		return emptyOp, fmt.Errorf("invalid number of args for clamp: %d", len(args))
	}

	if optype != ClampMinType && optype != ClampMaxType {
		return emptyOp, fmt.Errorf("unknown clamp type: %s", optype)
	}

	scalar, ok := args[0].(float64)
	if !ok {
		return emptyOp, fmt.Errorf("unable to cast to scalar argument: %v", args[0])
	}

	spec := clampOp{
		opType: optype,
		scalar: scalar,
	}

	return BaseOp{
		operatorType: optype,
		processorFn:  makeClampProcessor(spec),
	}, nil
}

func makeClampProcessor(spec clampOp) MakeProcessor {
	clampOp := spec
	return func(op BaseOp, controller *transform.Controller) Processor {
		fn := math.Min
		if op.operatorType == ClampMinType {
			fn = math.Max
		}

		return &clampNode{op: clampOp, controller: controller, clampFn: fn}
	}
}

type clampNode struct {
	op         clampOp
	clampFn    func(x, y float64) float64
	controller *transform.Controller
}

func (c *clampNode) Process(values []float64) []float64 {
	scalar := c.op.scalar
	for i := range values {
		values[i] = c.clampFn(values[i], scalar)
	}

	return values
}
