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
	// AvgType calculates the average of all values in the specified interval
	AvgType = "avg_over_time"

	// CountType calculates count of all values in the specified interval
	CountType = "count_over_time"

	// MinType calculates the minimum of all values in the specified interval
	MinType = "min_over_time"

	// MaxType calculates the maximum of all values in the specified interval
	MaxType = "max_over_time"

	// SumType calculates the sum of all values in the specified interval
	SumType = "sum_over_time"

	// StdDevType calculates the standard deviation of all values in the specified interval
	StdDevType = "stddev_over_time"

	// StdVarType calculates the standard variance of all values in the specified interval
	StdVarType = "stdvar_over_time"

	// HoltWintersType produces a smoothed value for time series based on the specified interval
	HoltWintersType = "holt_winters"
)

type aggFunc func([]float64) float64

var (
	aggFuncs = map[string]aggFunc{
		AvgType:    avgOverTime,
		CountType:  countOverTime,
		MinType:    minOverTime,
		MaxType:    maxOverTime,
		SumType:    sumOverTime,
		StdDevType: stddevOverTime,
		StdVarType: stdvarOverTime,
	}
)

type aggProcessor struct {
	aggFunc aggFunc
}

func (a aggProcessor) Init(op baseOp, controller *transform.Controller, opts transform.Options) Processor {
	return &aggNode{
		controller: controller,
		op:         op,
		aggFunc:    a.aggFunc,
	}
}

// NewAggOp creates a new base temporal transform with a specified node
func NewAggOp(args []interface{}, optype string) (transform.Params, error) {
	if aggregationFunc, ok := aggFuncs[optype]; ok {
		a := aggProcessor{
			aggFunc: aggregationFunc,
		}

		return newBaseOp(args, optype, a)
	}

	if optype == HoltWintersType {
		// todo(braskin): move this logic to the parser
		if len(args) != 3 {
			return emptyOp, fmt.Errorf("invalid number of args for %s: %d", optype, len(args))
		}

		sf, ok := args[1].(float64)
		if !ok {
			return emptyOp, fmt.Errorf("unable to cast to scalar argument: %v for %s", args[0], optype)
		}

		tf, ok := args[2].(float64)
		if !ok {
			return emptyOp, fmt.Errorf("unable to cast to scalar argument: %v for %s", args[0], optype)
		}

		// Sanity check the input.
		if sf <= 0 || sf >= 1 {
			return emptyOp, fmt.Errorf("invalid smoothing factor. Expected: 0 < sf < 1, got: %f", sf)
		}

		if tf <= 0 || tf >= 1 {
			return emptyOp, fmt.Errorf("invalid trend factor. Expected: 0 < tf < 1, got: %f", tf)
		}

		aggregationFunc := makeHoltWintersFn(sf, tf)
		a := aggProcessor{
			aggFunc: aggregationFunc,
		}

		return newBaseOp(args, optype, a)
	}

	return nil, fmt.Errorf("unknown aggregation type: %s", optype)
}

func makeHoltWintersFn(sf, tf float64) aggFunc {
	return func(vals []float64) float64 {
		var (
			foundFirst, foundSecond bool
			secondVal               float64
			b                       float64
			x, y                    float64
			s0, s1                  float64
		)

		for i, val := range vals {
			if math.IsNaN(val) {
				continue
			}

			if !foundFirst {
				foundFirst = true
				s1 = val
				continue
			}

			if !foundSecond {
				foundSecond = true
				secondVal = val
				b = secondVal - s1
			}

			// scale the raw value against the smoothing factor.
			x = sf * val

			// scale the last smoothed value with the trend at this point.
			b = calcTrendValue(i-1, sf, tf, s0, s1, b)
			y = (1 - sf) * (s1 + b)

			s0, s1 = s1, x+y
		}

		// need at least two values to apply a smoothing operation
		if !foundSecond {
			return math.NaN()
		}

		return s1
	}
}

// Calculate the trend value at the given index i in raw data d.
// This is somewhat analogous to the slope of the trend at the given index.
// The argument "s" is the set of computed smoothed values.
// The argument "b" is the set of computed trend factors.
// The argument "d" is the set of raw input values.
func calcTrendValue(i int, sf, tf, s0, s1, b float64) float64 {
	if i == 0 {
		return b
	}

	x := tf * (s1 - s0)
	y := (1 - tf) * b

	return x + y
}

type aggNode struct {
	op         baseOp
	controller *transform.Controller
	aggFunc    func([]float64) float64
	sf, tf     float64
}

func (a *aggNode) Process(datapoints ts.Datapoints) float64 {
	return a.aggFunc(datapoints.Values())
}

func avgOverTime(values []float64) float64 {
	sum, count := sumAndCount(values)
	return sum / count
}

func countOverTime(values []float64) float64 {
	_, count := sumAndCount(values)
	if count == 0 {
		return math.NaN()
	}

	return count
}

func minOverTime(values []float64) float64 {
	var seenNotNaN bool
	min := math.Inf(1)
	for _, v := range values {
		if !math.IsNaN(v) {
			seenNotNaN = true
			min = math.Min(min, v)
		}
	}

	if !seenNotNaN {
		return math.NaN()
	}

	return min
}

func maxOverTime(values []float64) float64 {
	var seenNotNaN bool
	max := math.Inf(-1)
	for _, v := range values {
		if !math.IsNaN(v) {
			seenNotNaN = true
			max = math.Max(max, v)
		}
	}

	if !seenNotNaN {
		return math.NaN()
	}

	return max
}

func sumOverTime(values []float64) float64 {
	sum, _ := sumAndCount(values)
	return sum
}

func stddevOverTime(values []float64) float64 {
	return math.Sqrt(stdvarOverTime(values))
}

func stdvarOverTime(values []float64) float64 {
	var aux, count, mean float64
	for _, v := range values {
		if !math.IsNaN(v) {
			count++
			delta := v - mean
			mean += delta / count
			aux += delta * (v - mean)
		}
	}

	if count == 0 {
		return math.NaN()
	}

	return aux / count
}

func sumAndCount(values []float64) (float64, float64) {
	sum := 0.0
	count := 0.0
	for _, v := range values {
		if !math.IsNaN(v) {
			sum += v
			count++
		}
	}

	if count == 0 {
		return math.NaN(), 0
	}

	return sum, count
}
