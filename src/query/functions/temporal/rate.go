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
	"fmt"
	"math"
	"time"

	"github.com/m3db/m3/src/query/executor/transform"
)

const (
	// IRateTemporalType calculates the per-second rate of increase of the time series
	// across the specified time range. This is based on the last two data points.
	IRateTemporalType = "irate"

	// IDeltaTemporalType calculates the difference between the last two values in the time series.
	// IDeltaTemporalType should only be used with gauges.
	IDeltaTemporalType = "idelta"
)

// NewRateOp creates a new base temporal transform for rate functions
func NewRateOp(args []interface{}, optype string) (transform.Params, error) {
	if optype == IRateTemporalType || optype == IDeltaTemporalType {
		return newBaseOp(args, optype, newRateNode, nil)
	}

	return nil, fmt.Errorf("unknown rate type: %s", optype)
}

func newRateNode(op baseOp, controller *transform.Controller, opts transform.Options) Processor {
	var isRate bool
	if op.operatorType == IRateTemporalType {
		isRate = true
	}

	return &rateNode{
		op:         op,
		controller: controller,
		timeSpec:   opts.TimeSpec,
		isRate:     isRate,
	}
}

type rateNode struct {
	op         baseOp
	controller *transform.Controller
	timeSpec   transform.TimeSpec
	isRate     bool
}

func (r *rateNode) Process(values []float64) float64 {
	valuesLen := len(values)
	if valuesLen < 2 {
		return math.NaN()
	}

	nonNanIdx := valuesLen - 1
	// find idx for last non-NaN value
	indexLast := findNonNanIdx(values, nonNanIdx)
	// if indexLast is 0 then you only have one value and should return a NaN
	if indexLast < 1 {
		return math.NaN()
	}

	lastSample := values[indexLast]
	nonNanIdx = findNonNanIdx(values, indexLast-1)
	if nonNanIdx == -1 {
		return math.NaN()
	}

	previousSample := values[nonNanIdx]

	var resultValue float64
	if r.isRate && lastSample < previousSample {
		// Counter reset.
		resultValue = lastSample
	} else {
		resultValue = lastSample - previousSample
	}

	if r.isRate {
		resultValue *= float64(time.Second)
		resultValue /= float64(r.timeSpec.Step) * float64(indexLast-nonNanIdx)
	}

	return resultValue
}

// findNonNanIdx iterates over the values backwards until we find a non-NaN value,
// then returns its index
func findNonNanIdx(vals []float64, startingIdx int) int {
	for i := startingIdx; i >= 0; i-- {
		if !math.IsNaN(vals[i]) {
			return i
		}
	}

	return -1
}
