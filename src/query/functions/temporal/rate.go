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
	// IRateTemporalType calculates the per-second instant rate of increase of the time series
	// in the range vector. This is based on the last two data points.
	IRateTemporalType = "irate"
)

// NewRateOp creates a new base temporal transform for rate functions
func NewRateOp(args []interface{}, optype string) (transform.Params, error) {
	if optype == IRateTemporalType {
		return newBaseOp(args, optype, newRateNode, nil)
	}

	return nil, fmt.Errorf("unknown aggregation type: %s", optype)
}

func newRateNode(op baseOp, controller *transform.Controller, opts transform.Options) Processor {
	return &rateNode{
		op:         op,
		controller: controller,
		timeSpec:   opts.TimeSpec,
	}
}

type rateNode struct {
	op         baseOp
	controller *transform.Controller
	timeSpec   transform.TimeSpec
}

func (r *rateNode) Process(values []float64) float64 {
	switch r.op.operatorType {
	case IRateTemporalType:
		return instantValue(values, true, r.timeSpec.Step)
	default:
		panic("unknown aggregation type")
	}
}

func instantValue(values []float64, isRate bool, stepSize time.Duration) float64 {
	fmt.Println(values)
	valuesLen := len(values)
	if valuesLen < 2 {
		return math.NaN()
	}

	// {0, 1, 2, 3, 4},
	// {5, 6, 7, 8, 9},

	lastSample := values[valuesLen-1]
	previousSample := values[valuesLen-2]

	fmt.Println(lastSample, previousSample)

	var resultValue float64
	if isRate && lastSample < previousSample {
		// Counter reset.
		resultValue = lastSample
	} else {
		resultValue = lastSample - previousSample
	}
	fmt.Println("result: ", resultValue)

	if isRate {
		// Convert to per-second.
		fmt.Println(stepSize, float64(stepSize), float64(stepSize)/1000, "sec: ", stepSize.Seconds())
		resultValue /= float64(stepSize) / math.Pow10(9)
	}

	return resultValue
}
