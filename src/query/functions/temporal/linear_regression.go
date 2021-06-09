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
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	// PredictLinearType predicts the value of time series t seconds from now,
	// based on the input series, using simple linear regression.
	// PredictLinearType should only be used with gauges.
	PredictLinearType = "predict_linear"

	// DerivType calculates the per-second derivative of the time series,
	// using simple linear regression.
	// DerivType should only be used with gauges.
	DerivType = "deriv"
)

type linearRegressionProcessor struct {
	fn      linearRegFn
	isDeriv bool
}

func (l linearRegressionProcessor) initialize(
	_ time.Duration,
	opts transform.Options,
) processor {
	return &linearRegressionNode{
		timeSpec: opts.TimeSpec(),
		fn:       l.fn,
		isDeriv:  l.isDeriv,
	}
}

type linearRegFn func(float64, float64) float64

// NewLinearRegressionOp creates a new base temporal transform
// for linear regression functions.
func NewLinearRegressionOp(
	args []interface{},
	optype string,
) (transform.Params, error) {
	var (
		fn      linearRegFn
		isDeriv bool
	)

	switch optype {
	case PredictLinearType:
		if len(args) != 2 {
			return emptyOp, fmt.Errorf("invalid number of args for %s: %d",
				PredictLinearType, len(args))
		}

		duration, ok := args[1].(float64)
		if !ok {
			return emptyOp, fmt.Errorf("unable to cast to scalar argument: %v for %s",
				args[1], PredictLinearType)
		}

		fn = func(slope, intercept float64) float64 {
			return slope*duration + intercept
		}

	case DerivType:
		if len(args) != 1 {
			return emptyOp, fmt.Errorf("invalid number of args for %s: %d",
				DerivType, len(args))
		}

		fn = func(slope, _ float64) float64 {
			return slope
		}

		isDeriv = true

	default:
		return nil, fmt.Errorf("unknown linear regression type: %s", optype)
	}

	duration, ok := args[0].(time.Duration)
	if !ok {
		return emptyOp, fmt.Errorf("unable to cast to scalar argument: %v for %s",
			args[0], optype)
	}

	l := linearRegressionProcessor{
		fn:      fn,
		isDeriv: isDeriv,
	}

	return newBaseOp(duration, optype, l)
}

type linearRegressionNode struct {
	timeSpec transform.TimeSpec
	fn       linearRegFn
	isDeriv  bool
}

func (l linearRegressionNode) process(
	dps ts.Datapoints,
	iterBounds iterationBounds,
) float64 {
	if dps.Len() < 2 {
		return math.NaN()
	}

	evaluationTime := iterBounds.end
	slope, intercept := linearRegression(dps, evaluationTime, l.isDeriv)
	return l.fn(slope, intercept)
}

func subSeconds(from xtime.UnixNano, sub xtime.UnixNano) float64 {
	return float64(from-sub) / float64(time.Second)
}

// linearRegression performs a least-square linear regression analysis on the
// provided datapoints. It returns the slope, and the intercept value at the
// provided time.
// Uses this algorithm: https://en.wikipedia.org/wiki/Simple_linear_regression.
func linearRegression(
	dps ts.Datapoints,
	interceptTime xtime.UnixNano,
	isDeriv bool,
) (float64, float64) {
	var (
		n                                   float64
		sumTimeDiff, sumVals                float64
		sumTimeDiffVals, sumTimeDiffSquared float64
		valueCount                          int
	)

	for _, dp := range dps {
		if math.IsNaN(dp.Value) {
			continue
		}

		if valueCount == 0 && isDeriv {
			// set interceptTime as timestamp of first non-NaN dp
			interceptTime = dp.Timestamp
		}

		valueCount++
		timeDiff := subSeconds(dp.Timestamp, interceptTime)
		n += 1.0
		sumVals += dp.Value
		sumTimeDiff += timeDiff
		sumTimeDiffVals += timeDiff * dp.Value
		sumTimeDiffSquared += timeDiff * timeDiff
	}

	// need at least 2 non-NaN values to calculate slope and intercept
	if valueCount == 1 {
		return math.NaN(), math.NaN()
	}

	covXY := sumTimeDiffVals - sumTimeDiff*sumVals/n
	varX := sumTimeDiffSquared - sumTimeDiff*sumTimeDiff/n

	slope := covXY / varX
	intercept := sumVals/n - slope*sumTimeDiff/n

	return slope, intercept
}
