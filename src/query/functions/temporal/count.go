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

package temporal

import (
	"math"

	"github.com/m3db/m3/src/query/executor/transform"
)

// CountTemporalType generates count of all values in the specified interval
const CountTemporalType = "count_over_time"

// NewCountOp creates a new base linear transform with a count node
func NewCountOp(args []interface{}) (transform.Params, error) {
	return newBaseOp(args, CountTemporalType, newCountNode)
}

func newCountNode(op baseOp, controller *transform.Controller) Processor {
	return &countNode{
		op:         op,
		controller: controller,
	}
}

type countNode struct {
	op         baseOp
	controller *transform.Controller
}

func (c *countNode) Process(values []float64) float64 {
	var count float64
	for _, v := range values {
		if !math.IsNaN(v) {
			count++
		}
	}

	return count
}
