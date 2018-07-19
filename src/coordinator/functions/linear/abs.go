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
	"math"

	"github.com/m3db/m3db/src/coordinator/executor/transform"
)

// AbsType takes absolute value of each datapoint in the series
const AbsType = "abs"

// NewAbsOp creates a new base linear transform with an abs node
func NewAbsOp() BaseOp {
	return BaseOp{
		operatorType: AbsType,
		processorFn:  newAbsNode,
	}

}

func newAbsNode(op BaseOp, controller *transform.Controller) Processor {
	return &absNode{
		op:         op,
		controller: controller,
	}
}

type absNode struct {
	op         BaseOp
	controller *transform.Controller
}

func (c *absNode) Process(values []float64) []float64 {
	for i := range values {
		values[i] = math.Abs(values[i])
	}

	return values
}
