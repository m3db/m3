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

const (
	minFloat64 = -math.MaxFloat64
)

// Gauge aggregates gauge values.
type Gauge struct {
	Options

	last   float64
	lastAt time.Time
	sum    float64
	sumSq  float64
	count  int64
	max    float64
	min    float64
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
func (g *Gauge) Update(timestamp time.Time, value float64) {
	if g.lastAt.IsZero() || timestamp.After(g.lastAt) {
		// NB(r): Only set the last value if this value arrives
		// after the wall clock timestamp of previous values, not
		// the arrival time (i.e. order received).
		g.last = value
		g.lastAt = timestamp
	} else {
		g.Options.Metrics.Gauge.IncValuesOutOfOrder()
	}

	g.sum += value
	g.count++
	if g.max < value {
		g.max = value
	}
	if g.min > value {
		g.min = value
	}

	if g.EnableExpensiveAggregations {
		g.sumSq += value * value
	}
}

// Last returns the last value received.
func (g *Gauge) Last() float64 { return g.last }

// LastAt returns the last value received time.
func (g *Gauge) LastAt() time.Time { return g.lastAt }

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
func (g *Gauge) ValueOf(aggType aggregation.Type) Value {
	switch aggType {
	case aggregation.Last:
		if g.Options.EnableAggregationLastValueAdjustTimestamp {
			return Value{
				Value:               g.Last(),
				AdjustTimestamp:     true,
				AdjustTimestampTime: g.LastAt(),
			}
		}
		return Value{Value: g.Last()}
	case aggregation.Min:
		return Value{Value: g.Min()}
	case aggregation.Max:
		return Value{Value: g.Max()}
	case aggregation.Mean:
		return Value{Value: g.Mean()}
	case aggregation.Count:
		return Value{Value: float64(g.Count())}
	case aggregation.Sum:
		return Value{Value: g.Sum()}
	case aggregation.SumSq:
		return Value{Value: g.SumSq()}
	case aggregation.Stdev:
		return Value{Value: g.Stdev()}
	default:
		return Value{}
	}
}

// Close closes the gauge.
func (g *Gauge) Close() {}
