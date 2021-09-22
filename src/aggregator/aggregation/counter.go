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

// Counter aggregates counter values.
type Counter struct {
	Options

	lastAt     time.Time
	annotation []byte
	sum        int64
	sumSq      int64
	count      int64
	max        int64
	min        int64
}

// NewCounter creates a new counter.
func NewCounter(opts Options) Counter {
	return Counter{
		Options: opts,
		max:     math.MinInt64,
		min:     math.MaxInt64,
	}
}

// Update updates the counter value.
func (c *Counter) Update(timestamp time.Time, value int64, annotation []byte) {
	if c.lastAt.IsZero() || timestamp.After(c.lastAt) {
		// NB(r): Only set the last value if this value arrives
		// after the wall clock timestamp of previous values, not
		// the arrival time (i.e. order received).
		c.lastAt = timestamp
	} else {
		c.Options.Metrics.Counter.IncValuesOutOfOrder()
	}

	c.sum += value

	c.count++
	if c.max < value {
		c.max = value
	}
	if c.min > value {
		c.min = value
	}

	if c.HasExpensiveAggregations {
		c.sumSq += value * value
	}

	c.annotation = maybeReplaceAnnotation(c.annotation, annotation)
}

// LastAt returns the time of the last value received.
func (c *Counter) LastAt() time.Time { return c.lastAt }

// Count returns the number of values received.
func (c *Counter) Count() int64 { return c.count }

// Sum returns the sum of counter values.
func (c *Counter) Sum() int64 { return c.sum }

// SumSq returns the squared sum of counter values.
func (c *Counter) SumSq() int64 { return c.sumSq }

// Mean returns the mean counter value.
func (c *Counter) Mean() float64 {
	if c.count == 0 {
		return 0
	}
	return float64(c.sum) / float64(c.count)
}

// Stdev returns the standard deviation counter value.
func (c *Counter) Stdev() float64 {
	return stdev(c.count, float64(c.sumSq), float64(c.sum))
}

// Min returns the minimum counter value.
func (c *Counter) Min() int64 { return c.min }

// Max returns the maximum counter value.
func (c *Counter) Max() int64 { return c.max }

// ValueOf returns the value for the aggregation type.
func (c *Counter) ValueOf(aggType aggregation.Type) float64 {
	switch aggType {
	case aggregation.Min:
		return float64(c.Min())
	case aggregation.Max:
		return float64(c.Max())
	case aggregation.Mean:
		return c.Mean()
	case aggregation.Count:
		return float64(c.Count())
	case aggregation.Sum:
		return float64(c.Sum())
	case aggregation.SumSq:
		return float64(c.SumSq())
	case aggregation.Stdev:
		return c.Stdev()
	default:
		return 0
	}
}

// Annotation returns the annotation associated with the counter.
func (c *Counter) Annotation() []byte {
	return c.annotation
}

// Close closes the counter.
func (c *Counter) Close() {}
