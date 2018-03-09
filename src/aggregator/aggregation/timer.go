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
	"sync"

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3metrics/policy"
)

// Timer aggregates timer values. Timer APIs are not thread-safe.
type Timer struct {
	Options

	count  int64     // Number of values received.
	sum    float64   // Sum of the values.
	sumSq  float64   // Sum of squared values.
	stream cm.Stream // Stream of values received.
}

// NewTimer creates a new timer
func NewTimer(quantiles []float64, streamOpts cm.Options, opts Options) Timer {
	stream := streamOpts.StreamPool().Get()
	stream.ResetSetData(quantiles)
	return Timer{
		Options: opts,
		stream:  stream,
	}
}

// Add adds a timer value.
func (t *Timer) Add(value float64) {
	t.count++
	t.sum += value
	t.stream.Add(value)

	if t.HasExpensiveAggregations {
		t.sumSq += value * value
	}
}

// AddBatch adds a batch of timer values.
func (t *Timer) AddBatch(values []float64) {
	for _, v := range values {
		t.Add(v)
	}
}

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
func (t *Timer) ValueOf(aggType policy.AggregationType) float64 {
	if q, ok := aggType.Quantile(); ok {
		return t.Quantile(q)
	}

	switch aggType {
	case policy.Min:
		return t.Min()
	case policy.Max:
		return t.Max()
	case policy.Mean:
		return t.Mean()
	case policy.Count:
		return float64(t.Count())
	case policy.Sum:
		return t.Sum()
	case policy.SumSq:
		return t.SumSq()
	case policy.Stdev:
		return t.Stdev()
	}
	return 0
}

// Close closes the timer.
func (t *Timer) Close() {
	t.stream.Close()
}

// LockedTimer is a locked timer.
type LockedTimer struct {
	sync.Mutex
	Timer
}

// NewLockedTimer creates a new locked timer.
func NewLockedTimer(timer Timer) *LockedTimer { return &LockedTimer{Timer: timer} }

// Reset resets the locked timer.
func (lt *LockedTimer) Reset(timer Timer) { lt.Timer = timer }
