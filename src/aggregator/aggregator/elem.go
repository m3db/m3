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

package aggregator

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3aggregator/aggregation"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
)

var (
	errCounterElemClosed = errors.New("counter element is closed")
	errTimerElemClosed   = errors.New("timer element is closed")
	errGaugeElemClosed   = errors.New("gauge element is closed")
)

type aggMetricFn func(
	idPrefix []byte,
	id metric.ID,
	idSuffix []byte,
	timestamp time.Time,
	value float64,
	policy policy.Policy,
)

type newMetricElemFn func() metricElem

// metricElem is the common interface for metric elements
type metricElem interface {
	// ResetSetData resets the counter and sets data
	ResetSetData(id metric.ID, policy policy.Policy)

	// AddMetric adds a new metric value
	// TODO(xichen): a value union would suffice here
	AddMetric(timestamp time.Time, mu unaggregated.MetricUnion) error

	// ReadAndDiscard processes values before a given time and discards
	// them afterwards, returning whether the element can be collected
	// after discarding the values
	ReadAndDiscard(earlierThan time.Time, fn aggMetricFn) bool

	// MarkAsTombstoned marks an element as tombstoned, which means this element
	// will be deleted once its aggregated values have been flushed
	MarkAsTombstoned()

	// Close closes the element
	Close()
}

type elemBase struct {
	sync.Mutex

	opts       Options
	id         metric.ID
	policy     policy.Policy
	tombstoned bool
	closed     bool
}

// CounterElem is the counter element
type CounterElem struct {
	elemBase

	values map[time.Time]*aggregation.Counter
}

// TimerElem is the timer element
type TimerElem struct {
	elemBase

	values map[time.Time]*aggregation.Timer
}

// GaugeElem is the gauge element
type GaugeElem struct {
	elemBase

	values map[time.Time]*aggregation.Gauge
}

// ResetSetData resets the counter and sets data
func (e *elemBase) ResetSetData(id metric.ID, policy policy.Policy) {
	e.id = id
	e.policy = policy
	e.tombstoned = false
	e.closed = false
}

// MarkAsTombstoned marks an element as tombstoned, which means this element
// will be deleted once its aggregated values have been flushed
func (e *elemBase) MarkAsTombstoned() {
	e.Lock()
	if e.closed {
		e.Unlock()
		return
	}
	e.tombstoned = true
	e.Unlock()
}

// NewCounterElem creates a new counter element
func NewCounterElem(id metric.ID, policy policy.Policy, opts Options) *CounterElem {
	return &CounterElem{
		elemBase: elemBase{opts: opts, id: id, policy: policy},
		values:   make(map[time.Time]*aggregation.Counter),
	}
}

// AddMetric adds a new counter value
func (e *CounterElem) AddMetric(timestamp time.Time, mu unaggregated.MetricUnion) error {
	alignedStart := timestamp.Truncate(e.policy.Resolution.Window)
	e.Lock()
	if e.closed {
		e.Unlock()
		return errCounterElemClosed
	}
	counter, exists := e.values[alignedStart]
	if !exists {
		counter = e.opts.CounterPool().Get()
		counter.Reset()
		e.values[alignedStart] = counter
	}
	counter.Add(mu.CounterVal)
	e.Unlock()
	return nil
}

// ReadAndDiscard processes values before a given time and discards
// them afterwards, returning whether the element can be collected
// after discarding the values
func (e *CounterElem) ReadAndDiscard(earlierThan time.Time, fn aggMetricFn) bool {
	e.Lock()
	if e.closed {
		e.Unlock()
		return false
	}
	for t, agg := range e.values {
		if t.Before(earlierThan) {
			endAt := t.Add(e.policy.Resolution.Window)
			e.processValue(endAt, agg, fn)
			e.opts.CounterPool().Put(agg)
			delete(e.values, t)
		}
	}
	canCollect := len(e.values) == 0 && e.tombstoned
	e.Unlock()
	return canCollect
}

// Close closes the counter element
func (e *CounterElem) Close() {
	e.Lock()
	if e.closed {
		e.Unlock()
		return
	}
	e.closed = true
	e.id = nil
	for t, agg := range e.values {
		e.opts.CounterPool().Put(agg)
		delete(e.values, t)
	}
	pool := e.opts.CounterElemPool()
	e.Unlock()

	pool.Put(e)
}

func (e *CounterElem) processValue(timestamp time.Time, agg *aggregation.Counter, fn aggMetricFn) {
	fn(e.opts.FullCounterPrefix(), e.id, nil, timestamp, float64(agg.Sum()), e.policy)
}

// NewTimerElem creates a new timer element
func NewTimerElem(id metric.ID, policy policy.Policy, opts Options) *TimerElem {
	return &TimerElem{
		elemBase: elemBase{opts: opts, id: id, policy: policy},
		values:   make(map[time.Time]*aggregation.Timer),
	}
}

// AddMetric adds a new batch of timer values
func (e *TimerElem) AddMetric(timestamp time.Time, mu unaggregated.MetricUnion) error {
	alignedStart := timestamp.Truncate(e.policy.Resolution.Window)
	e.Lock()
	if e.closed {
		e.Unlock()
		return errTimerElemClosed
	}
	timer, exists := e.values[alignedStart]
	if !exists {
		timer = e.opts.TimerPool().Get()
		timer.Reset()
		e.values[alignedStart] = timer
	}
	timer.AddBatch(mu.BatchTimerVal)
	e.Unlock()
	return nil
}

// ReadAndDiscard processes values before a given time and discards
// them afterwards, returning whether the element can be collected
// after discarding the values
func (e *TimerElem) ReadAndDiscard(earlierThan time.Time, fn aggMetricFn) bool {
	e.Lock()
	if e.closed {
		e.Unlock()
		return false
	}
	for t, agg := range e.values {
		if t.Before(earlierThan) {
			endAt := t.Add(e.policy.Resolution.Window)
			e.processValue(endAt, agg, fn)
			e.opts.TimerPool().Put(agg)
			delete(e.values, t)
		}
	}
	canCollect := len(e.values) == 0 && e.tombstoned
	e.Unlock()
	return canCollect
}

func (e *TimerElem) processValue(timestamp time.Time, agg *aggregation.Timer, fn aggMetricFn) {
	var (
		fullTimerPrefix   = e.opts.FullTimerPrefix()
		timerSumSuffix    = e.opts.TimerSumSuffix()
		timerSumSqSuffix  = e.opts.TimerSumSqSuffix()
		timerMeanSuffix   = e.opts.TimerMeanSuffix()
		timerLowerSuffix  = e.opts.TimerLowerSuffix()
		timerUpperSuffix  = e.opts.TimerUpperSuffix()
		timerCountSuffix  = e.opts.TimerCountSuffix()
		timerStdevSuffix  = e.opts.TimerStdevSuffix()
		timerMedianSuffix = e.opts.TimerMedianSuffix()
		quantiles         = e.opts.TimerQuantiles()
		quantileSuffixes  = e.opts.TimerQuantileSuffixes()
	)
	fn(fullTimerPrefix, e.id, timerSumSuffix, timestamp, agg.Sum(), e.policy)
	fn(fullTimerPrefix, e.id, timerSumSqSuffix, timestamp, agg.SumSq(), e.policy)
	fn(fullTimerPrefix, e.id, timerMeanSuffix, timestamp, agg.Mean(), e.policy)
	fn(fullTimerPrefix, e.id, timerLowerSuffix, timestamp, agg.Min(), e.policy)
	fn(fullTimerPrefix, e.id, timerUpperSuffix, timestamp, agg.Max(), e.policy)
	fn(fullTimerPrefix, e.id, timerCountSuffix, timestamp, float64(agg.Count()), e.policy)
	fn(fullTimerPrefix, e.id, timerStdevSuffix, timestamp, agg.Stddev(), e.policy)
	for idx, q := range quantiles {
		v := agg.Quantile(q)
		if q == 0.5 {
			fn(fullTimerPrefix, e.id, timerMedianSuffix, timestamp, v, e.policy)
		}
		fn(fullTimerPrefix, e.id, quantileSuffixes[idx], timestamp, v, e.policy)
	}
}

// Close closes the timer element
func (e *TimerElem) Close() {
	e.Lock()
	if e.closed {
		e.Unlock()
		return
	}
	e.closed = true
	e.id = nil
	for t, agg := range e.values {
		e.opts.TimerPool().Put(agg)
		delete(e.values, t)
	}
	pool := e.opts.TimerElemPool()
	e.Unlock()

	pool.Put(e)
}

// NewGaugeElem creates a new gauge element
func NewGaugeElem(id metric.ID, policy policy.Policy, opts Options) *GaugeElem {
	return &GaugeElem{
		elemBase: elemBase{opts: opts, id: id, policy: policy},
		values:   make(map[time.Time]*aggregation.Gauge),
	}
}

// AddMetric adds a new gauge value
func (e *GaugeElem) AddMetric(timestamp time.Time, mu unaggregated.MetricUnion) error {
	alignedStart := timestamp.Truncate(e.policy.Resolution.Window)
	e.Lock()
	if e.closed {
		e.Unlock()
		return errGaugeElemClosed
	}
	gauge, exists := e.values[alignedStart]
	if !exists {
		gauge = e.opts.GaugePool().Get()
		gauge.Reset()
		e.values[alignedStart] = gauge
	}
	gauge.Add(mu.GaugeVal)
	e.Unlock()
	return nil
}

// ReadAndDiscard processes values before a given time and discards
// them afterwards, returning whether the element can be collected
// after discarding the values
func (e *GaugeElem) ReadAndDiscard(earlierThan time.Time, fn aggMetricFn) bool {
	e.Lock()
	if e.closed {
		e.Unlock()
		return false
	}
	for t, agg := range e.values {
		if t.Before(earlierThan) {
			endAt := t.Add(e.policy.Resolution.Window)
			e.processValue(endAt, agg, fn)
			e.opts.GaugePool().Put(agg)
			delete(e.values, t)
		}
	}
	canCollect := len(e.values) == 0 && e.tombstoned
	e.Unlock()
	return canCollect
}

func (e *GaugeElem) processValue(timestamp time.Time, agg *aggregation.Gauge, fn aggMetricFn) {
	fn(e.opts.FullGaugePrefix(), e.id, nil, timestamp, agg.Value(), e.policy)
}

// Close closes the gauge element
func (e *GaugeElem) Close() {
	e.Lock()
	if e.closed {
		e.Unlock()
		return
	}
	e.closed = true
	e.id = nil
	for t, agg := range e.values {
		e.opts.GaugePool().Put(agg)
		delete(e.values, t)
	}
	pool := e.opts.GaugeElemPool()
	e.Unlock()

	pool.Put(e)
}
