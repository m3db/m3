// Copyright (c) 2016 Uber Technologies, Inc.
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

package aggregation

import (
	"math"
	"sync"

	"github.com/m3db/m3metrics/policy"
)

const (
	minFloat64 = -math.MaxFloat64
)

// Gauge aggregates gauge values.
type Gauge struct {
	Options

	last  float64
	sum   float64
	sumSq float64
	count int64
	max   float64
	min   float64
}

// NewGauge creates a new gauge.
func NewGauge(opts Options) Gauge {
	return Gauge{
		Options: opts,
		max:     minFloat64,
		min:     math.MaxFloat64,
	}
}

// Update updates the gauge value.
func (g *Gauge) Update(value float64) {
	g.last = value

	g.sum += value
	g.count++
	if g.max < value {
		g.max = value
	}
	if g.min > value {
		g.min = value
	}

	if g.HasExpensiveAggregations {
		g.sumSq += value * value
	}
}

// Last returns the last value received.
func (g *Gauge) Last() float64 { return g.last }

// Count returns the number of values received.
func (g *Gauge) Count() int64 { return g.count }

// Sum returns the sum of gauge values.
func (g *Gauge) Sum() float64 { return g.sum }

// SumSq returns the squared sum of gauge values.
func (g *Gauge) SumSq() float64 { return g.sumSq }

// Mean returns the mean gauge value.
func (g *Gauge) Mean() float64 {
	if g.count == 0 {
		return 0.0
	}
	return g.sum / float64(g.count)
}

// Stdev returns the standard deviation gauge value.
func (g *Gauge) Stdev() float64 {
	return stdev(g.count, g.sumSq, g.sum)
}

// Min returns the minimum gauge value.
func (g *Gauge) Min() float64 { return g.min }

// Max returns the maximum gauge value.
func (g *Gauge) Max() float64 { return g.max }

// ValueOf returns the value for the aggregation type.
func (g *Gauge) ValueOf(aggType policy.AggregationType) float64 {
	switch aggType {
	case policy.Last:
		return g.Last()
	case policy.Min:
		return g.Min()
	case policy.Max:
		return g.Max()
	case policy.Mean:
		return g.Mean()
	case policy.Count:
		return float64(g.Count())
	case policy.Sum:
		return g.Sum()
	case policy.SumSq:
		return g.SumSq()
	case policy.Stdev:
		return g.Stdev()
	default:
		return 0
	}
}

// LockedGauge is a locked gauge.
type LockedGauge struct {
	sync.Mutex
	Gauge
}

// NewLockedGauge creates a new locked gauge.
func NewLockedGauge(g Gauge) *LockedGauge { return &LockedGauge{Gauge: g} }

// Reset resets the locked gauge.
func (lg *LockedGauge) Reset(g Gauge) { lg.Gauge = g }
