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

// AbsentType returns an empty timeseries if the timeseries passed in has any elements,
// and returns a timeseries with the value 1 if the timeseries passed in has no elements
const AbsentType = "absent"

// NewAbsentOp creates a new base linear transform with an absent node
func NewAbsentOp() BaseOp {
	return BaseOp{
		operatorType: AbsentType,
		processorFn:  newAbsentNode,
	}
}

func newAbsentNode(op BaseOp, controller *transform.Controller) Processor {
	return &absentNode{
		op:         op,
		controller: controller,
	}
}

type absentNode struct {
	op         BaseOp
	controller *transform.Controller
}

func (c *absentNode) Process(values []float64) []float64 {
	num := 1.0
	if !isNull(values) {
		num = math.NaN()
	}

	for i := range values {
		values[i] = num
	}
	return values
}

func isNull(vals []float64) bool {
	for _, i := range vals {
		if !math.IsNaN(i) {
			return false
		}
	}
	return true
}
