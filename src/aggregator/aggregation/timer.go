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

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
)

// Timer aggregates timer values. Timer APIs are not thread-safe
type Timer struct {
	count  int64     // number of values received
	sum    float64   // sum of the values
	sumSq  float64   // sum of squared values
	stream cm.Stream // stream of values received
}

// NewTimer creates a new timer
func NewTimer(quantiles []float64, opts cm.Options) Timer {
	stream := opts.StreamPool().Get()
	stream.ResetSetData(quantiles)
	return Timer{
		stream: stream,
	}
}

// Add adds a timer value
func (t *Timer) Add(value float64) {
	t.count++
	t.sum += value
	t.sumSq += math.Pow(value, 2)
	t.stream.Add(value)
}

// AddBatch adds a batch of timer values
func (t *Timer) AddBatch(values []float64) {
	for _, v := range values {
		t.Add(v)
	}
}

// Quantile returns the value at a given quantile
func (t *Timer) Quantile(q float64) float64 {
	t.stream.Flush()
	return t.stream.Quantile(q)
}

// Count returns the number of values received
func (t *Timer) Count() int64 { return t.count }

// Min returns the minimum timer value
func (t *Timer) Min() float64 {
	t.stream.Flush()
	return t.stream.Min()
}

// Max returns the maximum timer value
func (t *Timer) Max() float64 {
	t.stream.Flush()
	return t.stream.Max()
}

// Sum returns the sum of timer values
func (t *Timer) Sum() float64 { return t.sum }

// SumSq returns the squared sum of timer values
func (t *Timer) SumSq() float64 { return t.sumSq }

// Mean returns the mean timer value
func (t *Timer) Mean() float64 {
	if t.count == 0 {
		return 0.0
	}
	return t.sum / float64(t.count)
}

// Stdev returns the standard deviation timer value
func (t *Timer) Stdev() float64 {
	num := float64(t.count)*t.sumSq - math.Pow(t.sum, 2)
	div := t.count * (t.count - 1)
	if div == 0 {
		return 0.0
	}
	return math.Sqrt(num / float64(div))
}

// Close closes the timer
func (t *Timer) Close() {
	t.stream.Close()
}
