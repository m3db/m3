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
	"github.com/m3db/m3/src/query/ts"
)

const (
	// PredictLinearType predicts the value of time series t seconds from now, based on the input series, using simple linear regression.
	// PredictLinearType should only be used with gauges.
	PredictLinearType = "predict_linear"

	// DerivType calculates the per-second derivative of the time series, using simple linear regression.
	// DerivType should only be used with gauges.
	DerivType = "deriv"
)

type linearRegressionProcessor struct {
	duration float64
}

func (l linearRegressionProcessor) Init(op baseOp, controller *transform.Controller, opts transform.Options) Processor {
	return &linearRegressionNode{
		op:         op,
		controller: controller,
		timeSpec:   opts.TimeSpec,
		duration:   l.duration,
	}
}

// NewLinearRegressionOp creates a new base temporal transform for linear regression functions
func NewLinearRegressionOp(args []interface{}, optype string) (transform.Params, error) {
	var (
		duration float64
		ok       bool
	)

	switch optype {
	case PredictLinearType:
		if len(args) != 2 {
			return emptyOp, fmt.Errorf("invalid number of args for %s: %d", PredictLinearType, len(args))
		}

		duration, ok = args[1].(float64)
		if !ok {
			return emptyOp, fmt.Errorf("unable to cast to scalar argument: %v for %s", args[1], PredictLinearType)
		}

	case DerivType:
		if len(args) != 1 {
			return emptyOp, fmt.Errorf("invalid number of args for %s: %d", DerivType, len(args))
		}

	default:
		return nil, fmt.Errorf("unknown linear regression type: %s", optype)
	}

	l := linearRegressionProcessor{
		duration: duration,
	}

	return newBaseOp(args, optype, l)
}

type linearRegressionNode struct {
	op         baseOp
	controller *transform.Controller
	timeSpec   transform.TimeSpec
	duration   float64
}

func (l linearRegressionNode) Process(dps ts.Datapoints) float64 {
	if dps.Len() < 2 {
		return math.NaN()
	}

	var slope, intercept float64

	if l.op.operatorType == PredictLinearType {
		slope, intercept = linearRegression(dps, l.timeSpec.End)
		return slope*l.duration + intercept
	}

	slope, _ = linearRegression(dps, time.Time{})
	return slope
}

// linearRegression performs a least-square linear regression analysis on the
// provided datapoints. It returns the slope, and the intercept value at the
// provided time. The algorithm we use comes from https://en.wikipedia.org/wiki/Simple_linear_regression.
func linearRegression(dps ts.Datapoints, interceptTime time.Time) (float64, float64) {
	var (
		n            float64
		sumX, sumY   float64
		sumXY, sumX2 float64

		nonNaNCount int
	)

	for _, dp := range dps {
		if math.IsNaN(dp.Value) {
			continue
		}

		if nonNaNCount == 0 {
			if interceptTime.IsZero() {
				// set interceptTime as timestamp of first non-NaN dp
				interceptTime = dp.Timestamp
			}
		}

		nonNaNCount++

		// convert to milliseconds
		x := float64(dp.Timestamp.Sub(interceptTime).Nanoseconds()/1000000) / 1e3
		n += 1.0
		sumY += dp.Value
		sumX += x
		sumXY += x * dp.Value
		sumX2 += x * x
	}

	// need at least 2 non-NaN values to calculate slope and intercept
	if nonNaNCount == 1 {
		return math.NaN(), math.NaN()
	}

	covXY := sumXY - sumX*sumY/n
	varX := sumX2 - sumX*sumX/n

	slope := covXY / varX
	intercept := sumY/n - slope*sumX/n

	return slope, intercept
}
