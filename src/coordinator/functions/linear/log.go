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

// Special cases for all of the following include:
// ln(+Inf) = +Inf
// ln(0) = -Inf
// ln(x < 0) = NaN
// ln(NaN) = NaN

// LnType calculates the natural logarithm for all elements in v
const LnType = "ln"

// Log2Type calculates the binary logarithm for all elements in v
const Log2Type = "log2"

// Log10Type calculates the decimal logarithm for all elements in v
const Log10Type = "log10"

type logOp struct {
	opType string
}

// NewLogOp creates a new log op based on the type
func NewLogOp(optype string) (BaseOp, error) {

	if optype != LnType && optype != Log2Type && optype != Log10Type {
		return emptyOp, fmt.Errorf("unknown log type: %s", optype)
	}

	spec := logOp{
		opType: optype,
	}

	return BaseOp{
		operatorType: optype,
		processorFn:  makeLogProcessor(spec),
	}, nil
}

func makeLogProcessor(spec logOp) makeProcessor {
	logOp := spec
	var fn func(x float64) float64

	return func(op BaseOp, controller *transform.Controller) Processor {
		switch op.operatorType {
		case LnType:
			fn = math.Log
		case Log2Type:
			fn = math.Log2
		case Log10Type:
			fn = math.Log10
		}

		return &logNode{op: logOp, controller: controller, logFn: fn}
	}
}

type logNode struct {
	op         logOp
	logFn      func(x float64) float64
	controller *transform.Controller
}

func (c *logNode) Process(values []float64) []float64 {
	for i := range values {
		values[i] = c.logFn(values[i])
	}

	return values
}
