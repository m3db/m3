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
	"time"

	"github.com/m3db/m3/src/aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3/src/metrics/aggregation"
)

// Timer aggregates timer values. Timer APIs are not thread-safe.
type Timer struct {
	lastAt                   time.Time
	count                    int64      // Number of values received.
	sum                      float64    // Sum of the values.
	sumSq                    float64    // Sum of squared values.
	stream                   *cm.Stream // Stream of values received.
	annotation               []byte
	hasExpensiveAggregations bool
}

// NewTimer creates a new timer
func NewTimer(quantiles []float64, streamOpts cm.Options, opts Options) Timer {
	stream := streamOpts.StreamPool().Get()
	stream.ResetSetData(quantiles)
	return Timer{
		hasExpensiveAggregations: opts.HasExpensiveAggregations,
		stream:                   stream,
	}
}

// Add adds a timer value.
func (t *Timer) Add(timestamp time.Time, value float64, annotation []byte) {
	t.AddBatch(timestamp, []float64{value}, annotation)
}

// AddBatch adds a batch of timer values.
func (t *Timer) AddBatch(timestamp time.Time, values []float64, annotation []byte) {
	// Record last at just once.
	t.recordLastAt(timestamp)
	t.count += int64(len(values))

	if t.hasExpensiveAggregations {
		for _, v := range values {
			t.sum += v
			t.sumSq += v * v
		}
	} else {
		for _, v := range values {
			t.sum += v
		}
	}

	t.stream.AddBatch(values)

	// Keep the last annotation which was set.
	t.annotation = maybeReplaceAnnotation(t.annotation, annotation)
}

func (t *Timer) recordLastAt(timestamp time.Time) {
	if t.lastAt.IsZero() || timestamp.After(t.lastAt) {
		// NB(r): Only set the last value if this value arrives
		// after the wall clock timestamp of previous values, not
		// the arrival time (i.e. order received).
		t.lastAt = timestamp
	}
}

// LastAt returns the time of the last value received.
func (t *Timer) LastAt() time.Time { return t.lastAt }

// Quantile returns the value at a given quantile.
func (t *Timer) Quantile(q float64) float64 {
	t.stream.Flush()
	return t.stream.Quantile(q)
}

// Count returns the number of values received.
func (t *Timer) Count() int64 { return t.count }

// Min returns the minimum timer value.
func (t *Timer) Min() float64 {
	t.stream.Flush()
	return t.stream.Min()
}

// Max returns the maximum timer value.
func (t *Timer) Max() float64 {
	t.stream.Flush()
	return t.stream.Max()
}

// Sum returns the sum of timer values.
func (t *Timer) Sum() float64 { return t.sum }

// SumSq returns the squared sum of timer values.
func (t *Timer) SumSq() float64 { return t.sumSq }

// Mean returns the mean timer value.
func (t *Timer) Mean() float64 {
	if t.count == 0 {
		return 0.0
	}
	return t.sum / float64(t.count)
}

// Stdev returns the standard deviation timer value.
func (t *Timer) Stdev() float64 {
	return stdev(t.count, t.sumSq, t.sum)
}

// ValueOf returns the value for the aggregation type.
func (t *Timer) ValueOf(aggType aggregation.Type) float64 {
	if q, ok := aggType.Quantile(); ok {
		return t.Quantile(q)
	}

	switch aggType {
	case aggregation.Min:
		return t.Min()
	case aggregation.Max:
		return t.Max()
	case aggregation.Mean:
		return t.Mean()
	case aggregation.Count:
		return float64(t.Count())
	case aggregation.Sum:
		return t.Sum()
	case aggregation.SumSq:
		return t.SumSq()
	case aggregation.Stdev:
		return t.Stdev()
	}
	return 0
}

// Annotation returns the annotation associated with the timer.
func (t *Timer) Annotation() []byte {
	return t.annotation
}

// Close closes the timer.
func (t *Timer) Close() { t.stream.Close() }
