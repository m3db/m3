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
	"sort"
	"time"

	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/ts"
)

const (
	// AvgType calculates the average of all values in the specified interval.
	AvgType = "avg_over_time"

	// CountType calculates count of all values in the specified interval.
	CountType = "count_over_time"

	// MinType calculates the minimum of all values in the specified interval.
	MinType = "min_over_time"

	// MaxType calculates the maximum of all values in the specified interval.
	MaxType = "max_over_time"

	// SumType calculates the sum of all values in the specified interval.
	SumType = "sum_over_time"

	// StdDevType calculates the standard deviation of all values in the specified interval.
	StdDevType = "stddev_over_time"

	// StdVarType calculates the standard variance of all values in the specified interval.
	StdVarType = "stdvar_over_time"

	// QuantileType calculates the φ-quantile (0 ≤ φ ≤ 1) of the values in the specified interval.
	QuantileType = "quantile_over_time"
)

type aggFunc func([]float64, float64) float64

var (
	aggFuncs = map[string]aggFunc{
		AvgType:      avgOverTime,
		CountType:    countOverTime,
		MinType:      minOverTime,
		MaxType:      maxOverTime,
		SumType:      sumOverTime,
		StdDevType:   stddevOverTime,
		StdVarType:   stdvarOverTime,
		QuantileType: quantileOverTime,
	}
)

type aggProcessor struct {
	aggFunc        aggFunc
	quantileScalar float64
}

func (a aggProcessor) Init(op baseOp, controller *transform.Controller, opts transform.Options) Processor {
	return &aggNode{
		controller:     controller,
		op:             op,
		aggFunc:        a.aggFunc,
		quantileScalar: a.quantileScalar,
	}
}

// NewAggOp creates a new base temporal transform with a specified node.
func NewAggOp(args []interface{}, optype string) (transform.Params, error) {
	if aggregationFunc, ok := aggFuncs[optype]; ok {
		a := aggProcessor{
			aggFunc: aggregationFunc,
		}

		if optype == QuantileType {
			if len(args) != 2 {
				return emptyOp, fmt.Errorf("invalid number of args for %s: %d", QuantileType, len(args))
			}

			scalar, ok := args[0].(float64)
			if !ok {
				return emptyOp, fmt.Errorf("unable to cast to scalar argument: %v for %s", args[1], QuantileType)
			}

			a.quantileScalar = scalar
		}

		return newBaseOp(args, optype, a)
	}

	return nil, fmt.Errorf("unknown aggregation type: %s", optype)
}

type aggNode struct {
	op             baseOp
	controller     *transform.Controller
	aggFunc        func([]float64, float64) float64
	quantileScalar float64
}

func (a *aggNode) Process(datapoints ts.Datapoints, _ time.Time) float64 {
	return a.aggFunc(datapoints.Values(), a.quantileScalar)
}

func avgOverTime(values []float64, _ float64) float64 {
	sum, count := sumAndCount(values)
	return sum / count
}

func countOverTime(values []float64, _ float64) float64 {
	_, count := sumAndCount(values)
	if count == 0 {
		return math.NaN()
	}

	return count
}

func minOverTime(values []float64, _ float64) float64 {
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

func maxOverTime(values []float64, _ float64) float64 {
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

func sumOverTime(values []float64, _ float64) float64 {
	sum, _ := sumAndCount(values)
	return sum
}

func stddevOverTime(values []float64, _ float64) float64 {
	return math.Sqrt(stdvarOverTime(values, -1))
}

func stdvarOverTime(values []float64, _ float64) float64 {
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

func quantileOverTime(values []float64, scalar float64) float64 {
	valuesSlice := make(valsSlice, 0, len(values))
	for _, v := range values {
		valuesSlice = append(values, v)
	}

	return quantile(scalar, valuesSlice)
}

type valsSlice []float64

func (s valsSlice) Len() int {
	return len(s)
}

func (s valsSlice) Less(i, j int) bool {
	if math.IsNaN(s[i]) {
		return true
	}

	return s[i] < s[j]
}

func (s valsSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// qauntile calculates the given quantile of a slice of values.
//
// This slice will be sorted.
// If 'values' has zero elements, NaN is returned.
// If q<0, -Inf is returned.
// If q>1, +Inf is returned.
func quantile(q float64, values valsSlice) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}
	sort.Sort(values)

	n := float64(len(values))
	// When the quantile lies between two values,
	// we use a weighted average of the two values.
	rank := q * (n - 1)

	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)

	weight := rank - math.Floor(rank)
	return values[int(lowerIndex)]*(1-weight) + values[int(upperIndex)]*weight
}
