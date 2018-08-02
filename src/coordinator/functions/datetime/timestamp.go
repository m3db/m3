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

package datetime

import (
	"fmt"
	"math"
	"time"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/executor/transform"
)

// TimestampType returns the timestamp of each of the values
// as the number of seconds since January 1, 1970 UTC.
const TimestampType = "timestamp"

// NewTimestampOp creates a new date op based on the type
func NewTimestampOp(optype string) (transform.Params, error) {
	if optype != TimestampType {
		return emptyOp, fmt.Errorf("unknown timestamp type: %s", optype)
	}

	return baseOp{
		operatorType: optype,
		processorFn:  newTimestampNode,
	}, nil
}

func newTimestampNode(op baseOp, controller *transform.Controller) Processor {
	return &timestampNode{
		op:         op,
		controller: controller,
	}
}

type timestampNode struct {
	op         baseOp
	controller *transform.Controller
}

func (c *timestampNode) ProcessStep(values []float64, t time.Time) []float64 {
	for i := range values {
		if math.IsNaN(values[i]) {
			values[i] = math.NaN()
			continue
		}
		values[i] = float64(t.Unix()) / 1000
	}

	return values
}

func (c *timestampNode) ProcessSeries(values []float64, bounds block.Bounds) []float64 {
	var t time.Time
	for i := range values {
		t, _ = bounds.TimeForIndex(i)
		if math.IsNaN(values[i]) {
			values[i] = math.NaN()
			continue
		}
		values[i] = float64(t.Unix()) / 1000
	}

	return values
}
