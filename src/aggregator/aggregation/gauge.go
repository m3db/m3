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
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
)

// Gauge aggregates gauge values.
type Gauge struct {
	Options

	lastAt     time.Time
	annotation []byte
	sum        float64
	sumSq      float64
	count      int64
	max        float64
	min        float64
	last       float64
}

// NewGauge creates a new gauge.
func NewGauge(opts Options) Gauge {
	return Gauge{
		Options: opts,
		max:     math.NaN(),
		min:     math.NaN(),
	}
}

// Update updates the gauge value.
func (g *Gauge) Update(timestamp time.Time, value float64, annotation []byte) {
	g.annotation = MaybeReplaceAnnotation(g.annotation, annotation)
	g.updateTotals(timestamp, value)
}

// UpdatePrevious removes the prevValue from the aggregation and updates with the new value.
func (g *Gauge) UpdatePrevious(timestamp time.Time, value float64, prevValue float64) {
	// remove the prevValue from the totals.
	if !math.IsNaN(prevValue) {
		g.sum -= prevValue
		if g.HasExpensiveAggregations {
			g.sumSq -= prevValue * prevValue
		}
	}
	g.count--
	// add the new value to the totals.
	g.updateTotals(timestamp, value)
}

// update the set of aggregated values that are shared between Update and UpdatePrevious.
func (g *Gauge) updateTotals(timestamp time.Time, value float64) {
	if g.lastAt.IsZero() || timestamp.After(g.lastAt) {
		// NB(r): Only set the last value if this value arrives
		// after the wall clock timestamp of previous values, not
		// the arrival time (i.e. order received).
		g.lastAt = timestamp
		g.last = value
	} else {
		g.Options.Metrics.Gauge.IncValuesOutOfOrder()
	}

	g.count++

	if math.IsNaN(value) {
		return
	}

	g.sum += value
	if math.IsNaN(g.max) || g.max < value {
		g.max = value
	}

	if math.IsNaN(g.min) || g.min > value {
		g.min = value
	}

	if g.HasExpensiveAggregations {
		g.sumSq += value * value
	}
}

// LastAt returns the time of the last value received.
func (g *Gauge) LastAt() time.Time { return g.lastAt }

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
func (g *Gauge) Min() float64 {
	return g.min
}

// Max returns the maximum gauge value.
func (g *Gauge) Max() float64 {
	return g.max
}

// ValueOf returns the value for the aggregation type.
func (g *Gauge) ValueOf(aggType aggregation.Type) float64 {
	switch aggType {
	case aggregation.Last:
		return g.Last()
	case aggregation.Min:
		return g.Min()
	case aggregation.Max:
		return g.Max()
	case aggregation.Mean:
		return g.Mean()
	case aggregation.Count:
		return float64(g.Count())
	case aggregation.Sum:
		return g.Sum()
	case aggregation.SumSq:
		return g.SumSq()
	case aggregation.Stdev:
		return g.Stdev()
	default:
		return 0
	}
}

// Annotation returns the annotation associated with the gauge.
func (g *Gauge) Annotation() []byte {
	return g.annotation
}

// Close closes the gauge.
func (g *Gauge) Close() {}
