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

	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/ts"
)

const (
	// IRateType calculates the per-second rate of increase of the time series
	// across the specified time range. This is based on the last two data points.
	IRateType = "irate"

	// IDeltaType calculates the difference between the last two values in the time series.
	// IDeltaTemporalType should only be used with gauges.
	IDeltaType = "idelta"
)

// NewRateOp creates a new base temporal transform for rate functions
func NewRateOp(args []interface{}, optype string) (transform.Params, error) {
	if optype == IRateType || optype == IDeltaType {
		return newBaseOp(args, optype, newRateNode, nil)
	}

	return nil, fmt.Errorf("unknown rate type: %s", optype)
}

func newRateNode(op baseOp, controller *transform.Controller, opts transform.Options) Processor {
	isRate := op.operatorType == IRateType

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

func (r *rateNode) Process(datapoints ts.Datapoints) float64 {
	valuesLen := len(datapoints)
	if valuesLen < 2 {
		return math.NaN()
	}

	nonNanIdx := valuesLen - 1
	// find idx for last non-NaN value
	indexLast := findNonNanIdx(datapoints, nonNanIdx)
	// if indexLast is 0 then you only have one value and should return a NaN
	if indexLast < 1 {
		return math.NaN()
	}

	nonNanIdx = findNonNanIdx(datapoints, indexLast-1)
	if nonNanIdx == -1 {
		return math.NaN()
	}

	previousSample := datapoints[nonNanIdx]
	lastSample := datapoints[indexLast]

	var resultValue float64
	if r.isRate && lastSample.Value < previousSample.Value {
		// Counter reset.
		resultValue = lastSample.Value
	} else {
		resultValue = lastSample.Value - previousSample.Value
	}

	if r.isRate {
		sampledInterval := lastSample.Timestamp.Sub(previousSample.Timestamp)
		if sampledInterval == 0 {
			return math.NaN()
		}

		resultValue /= sampledInterval.Seconds()
	}

	return resultValue
}

// findNonNanIdx iterates over the values backwards until we find a non-NaN value,
// then returns its index
func findNonNanIdx(dps ts.Datapoints, startingIdx int) int {
	for i := startingIdx; i >= 0; i-- {
		if !math.IsNaN(dps[i].Value) {
			return i
		}
	}

	return -1
}
