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
)

const (
	// AvgTemporalType calculates the average of all values in the specified interval
	AvgTemporalType = "avg_over_time"

	// CountTemporalType calculates count of all values in the specified interval
	CountTemporalType = "count_over_time"

	// MinTemporalType calculates the minimum of all values in the specified interval
	MinTemporalType = "min_over_time"

	// MaxTemporalType calculates the maximum of all values in the specified interval
	MaxTemporalType = "max_over_time"

	// SumTemporalType calculates the sum of all values in the specified interval
	SumTemporalType = "sum_over_time"

	// StdDevTemporalType calculates the standard deviation of all values in the specified interval
	StdDevTemporalType = "stddev_over_time"

	// StdVarTemporalType calculates the standard variance of all values in the specified interval
	StdVarTemporalType = "stdvar_over_time"
)

var (
	aggFuncs = map[string]func([]float64) float64{
		AvgTemporalType:    avgOverTime,
		CountTemporalType:  countOverTime,
		MinTemporalType:    minOverTime,
		MaxTemporalType:    maxOverTime,
		SumTemporalType:    sumOverTime,
		StdDevTemporalType: stddevOverTime,
		StdVarTemporalType: stdvarOverTime,
	}
)

// NewAggOp creates a new base temporal transform with a specified node
func NewAggOp(args []interface{}, optype string) (transform.Params, error) {
	if _, ok := aggFuncs[optype]; !ok {
		return emptyOp, fmt.Errorf("unknown aggregation type: %s", optype)
	}

	return newBaseOp(args, optype, newAggNode)
}

func newAggNode(op baseOp, controller *transform.Controller) Processor {
	return &aggNode{
		op:         op,
		controller: controller,
		aggFunc:    aggFuncs[op.operatorType],
	}
}

type aggNode struct {
	op         baseOp
	controller *transform.Controller
	aggFunc    func([]float64) float64
}

func (a *aggNode) Process(values []float64) float64 {
	return a.aggFunc(values)
}

func avgOverTime(values []float64) float64 {
	var sum, count float64
	for _, v := range values {
		if !math.IsNaN(v) {
			count++
			sum += v
		}
	}

	if count == 0 {
		return math.NaN()
	}
	return sum / count
}

func countOverTime(values []float64) float64 {
	var count float64
	for _, v := range values {
		if !math.IsNaN(v) {
			count++
		}
	}
	return count
}

func minOverTime(values []float64) float64 {
	var count float64
	min := math.Inf(1)
	for _, v := range values {
		if !math.IsNaN(v) {
			count++
			min = math.Min(min, v)
		}
	}

	if count == 0 {
		return math.NaN()
	}
	return min
}

func maxOverTime(values []float64) float64 {
	var count float64
	max := math.Inf(-1)
	for _, v := range values {
		if !math.IsNaN(v) {
			count++
			max = math.Max(max, v)
		}
	}

	if count == 0 {
		return math.NaN()
	}
	return max
}

func sumOverTime(values []float64) float64 {
	var sum, count float64
	for _, v := range values {
		if !math.IsNaN(v) {
			count++
			sum += v
		}
	}

	if count == 0 {
		return math.NaN()
	}
	return sum
}

func stddevOverTime(values []float64) float64 {
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
	return math.Sqrt(aux / count)
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
