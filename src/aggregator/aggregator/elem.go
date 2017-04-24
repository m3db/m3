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

const (
	defaultNumValues = 2
)

var (
	emptyTimedCounter timedCounter
	emptyTimedTimer   timedTimer
	emptyTimedGauge   timedGauge

	errInvalidMetricType = errors.New("invalid metric type")
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
	// ID returns the metric id.
	ID() metric.ID

	// ResetSetData resets the counter and sets data
	ResetSetData(id metric.ID, policy policy.Policy)

	// AddMetric adds a new metric value
	// TODO(xichen): a value union would suffice here
	AddMetric(timestamp time.Time, mu unaggregated.MetricUnion) error

	// Consume processes values before a given time and discards
	// them afterwards, returning whether the element can be collected
	// after discarding the values
	Consume(earlierThan time.Time, fn aggMetricFn) bool

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

type timedCounter struct {
	timeNs  int64
	counter aggregation.Counter
}

type timedTimer struct {
	timeNs int64
	timer  aggregation.Timer
}

type timedGauge struct {
	timeNs int64
	gauge  aggregation.Gauge
}

// CounterElem is the counter element
type CounterElem struct {
	elemBase

	values []timedCounter // aggregated counters sorted by time in ascending order
}

// TimerElem is the timer element
type TimerElem struct {
	elemBase

	values []timedTimer // aggregated timers sorted by time in ascending order
}

// GaugeElem is the gauge element
type GaugeElem struct {
	elemBase

	values []timedGauge // aggregated gauges sorted by time in ascending order
}

// ResetSetData resets the counter and sets data.
func (e *elemBase) ResetSetData(id metric.ID, policy policy.Policy) {
	e.id = id
	e.policy = policy
	e.tombstoned = false
	e.closed = false
}

// ID returns the metric id.
func (e *elemBase) ID() metric.ID {
	e.Lock()
	id := e.id
	e.Unlock()
	return id
}

// MarkAsTombstoned marks an element as tombstoned, which means this element
// will be deleted once its aggregated values have been flushed.
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
		values:   make([]timedCounter, 0, defaultNumValues), // in most cases values will have two entries
	}
}

// AddMetric adds a new counter value
func (e *CounterElem) AddMetric(timestamp time.Time, mu unaggregated.MetricUnion) error {
	if mu.Type != unaggregated.CounterType {
		return errInvalidMetricType
	}
	alignedStart := timestamp.Truncate(e.policy.Resolution().Window).UnixNano()
	e.Lock()
	if e.closed {
		e.Unlock()
		return errCounterElemClosed
	}
	idx := e.findOrInsertWithLock(alignedStart)
	e.values[idx].counter.Add(mu.CounterVal)
	e.Unlock()
	return nil
}

// Consume processes values before a given time and discards
// them afterwards, returning whether the element can be collected
// after discarding the values
func (e *CounterElem) Consume(earlierThan time.Time, fn aggMetricFn) bool {
	earlierThanNs := earlierThan.UnixNano()
	e.Lock()
	if e.closed {
		e.Unlock()
		return false
	}
	idx := 0
	for range e.values {
		// Bail as soon as the timestamp is no later than the target time
		if e.values[idx].timeNs >= earlierThanNs {
			break
		}
		endAt := time.Unix(0, e.values[idx].timeNs).Add(e.policy.Resolution().Window)
		e.processValue(endAt, e.values[idx].counter, fn)
		idx++
	}
	if idx > 0 {
		// Shift remaining values to the left and shrink the values slice
		n := copy(e.values[0:], e.values[idx:])
		e.values = e.values[:n]
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
	e.values = e.values[:0]
	pool := e.opts.CounterElemPool()
	e.Unlock()

	pool.Put(e)
}

// findOrInsertWithLock finds the element index whose timestamp matches
// the start time passed in, or inserts one if it doesn't exist while
// maintaining the time ascending order of the values. The returned index
// is always valid.
func (e *CounterElem) findOrInsertWithLock(alignedStart int64) int {
	numValues := len(e.values)
	// Optimize for the common case
	if numValues > 0 && e.values[numValues-1].timeNs == alignedStart {
		return numValues - 1
	}
	// Binary search for the unusual case. We intentionally do not
	// use the sort.Search() function because it requires passing
	// in a closure
	left, right := 0, numValues
	for left < right {
		mid := left + (right-left)/2 // avoid overflow
		if e.values[mid].timeNs < alignedStart {
			left = mid + 1
		} else {
			right = mid
		}
	}
	// If the current timestamp is equal to or larger than the target time,
	// return the index as is
	if left < numValues && e.values[left].timeNs == alignedStart {
		return left
	}
	// Otherwise we need to insert a new item
	e.values = append(e.values, emptyTimedCounter)
	// NB(xichen): it is ok if the source and the destination overlap
	copy(e.values[left+1:numValues+1], e.values[left:numValues])
	e.values[left] = timedCounter{
		timeNs:  alignedStart,
		counter: aggregation.NewCounter(),
	}
	return left
}

func (e *CounterElem) processValue(timestamp time.Time, agg aggregation.Counter, fn aggMetricFn) {
	fn(e.opts.FullCounterPrefix(), e.id, nil, timestamp, float64(agg.Sum()), e.policy)
}

// NewTimerElem creates a new timer element
func NewTimerElem(id metric.ID, policy policy.Policy, opts Options) *TimerElem {
	return &TimerElem{
		elemBase: elemBase{opts: opts, id: id, policy: policy},
		values:   make([]timedTimer, 0, defaultNumValues), // in most cases values will have two entries
	}
}

// AddMetric adds a new batch of timer values
func (e *TimerElem) AddMetric(timestamp time.Time, mu unaggregated.MetricUnion) error {
	if mu.Type != unaggregated.BatchTimerType {
		return errInvalidMetricType
	}
	alignedStart := timestamp.Truncate(e.policy.Resolution().Window).UnixNano()
	e.Lock()
	if e.closed {
		e.Unlock()
		return errTimerElemClosed
	}
	idx := e.findOrInsertWithLock(alignedStart)
	e.values[idx].timer.AddBatch(mu.BatchTimerVal)
	e.Unlock()
	return nil
}

// Consume processes values before a given time and discards
// them afterwards, returning whether the element can be collected
// after discarding the values
func (e *TimerElem) Consume(earlierThan time.Time, fn aggMetricFn) bool {
	earlierThanNs := earlierThan.UnixNano()
	e.Lock()
	if e.closed {
		e.Unlock()
		return false
	}
	idx := 0
	for range e.values {
		// Bail as soon as the timestamp is no later than the target time
		if e.values[idx].timeNs >= earlierThanNs {
			break
		}
		endAt := time.Unix(0, e.values[idx].timeNs).Add(e.policy.Resolution().Window)
		e.processValue(endAt, e.values[idx].timer, fn)
		// Close the timer after it's processed
		e.values[idx].timer.Close()
		idx++
	}
	if idx > 0 {
		// Shift remaining values to the left and shrink the values slice
		n := copy(e.values[0:], e.values[idx:])
		// Clear out the invalid timer items to avoid holding onto the underlying streams
		for i := n; i < len(e.values); i++ {
			e.values[i] = emptyTimedTimer
		}
		e.values = e.values[:n]
	}
	canCollect := len(e.values) == 0 && e.tombstoned
	e.Unlock()
	return canCollect
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
	for idx := range e.values {
		// Returning the underlying stream to pool
		e.values[idx].timer.Close()
		e.values[idx] = emptyTimedTimer
	}
	e.values = e.values[:0]
	pool := e.opts.TimerElemPool()
	e.Unlock()

	pool.Put(e)
}

// findOrInsertWithLock finds the element index whose timestamp matches
// the start time passed in, or inserts one if it doesn't exist while
// maintaining the time ascending order of the values. The returned index
// is always valid.
func (e *TimerElem) findOrInsertWithLock(alignedStart int64) int {
	numValues := len(e.values)
	// Optimize for the common case
	if numValues > 0 && e.values[numValues-1].timeNs == alignedStart {
		return numValues - 1
	}
	// Binary search for the unusual case. We intentionally do not
	// use the sort.Search() function because it requires passing
	// in a closure
	left, right := 0, numValues
	for left < right {
		mid := left + (right-left)/2 // avoid overflow
		if e.values[mid].timeNs < alignedStart {
			left = mid + 1
		} else {
			right = mid
		}
	}
	// If the current timestamp is equal to or larger than the target time,
	// return the index as is
	if left < numValues && e.values[left].timeNs == alignedStart {
		return left
	}
	// Otherwise we need to insert a new item
	e.values = append(e.values, emptyTimedTimer)
	// NB(xichen): it is ok if the source and the destination overlap
	copy(e.values[left+1:numValues+1], e.values[left:numValues])
	e.values[left] = timedTimer{
		timeNs: alignedStart,
		timer:  aggregation.NewTimer(e.opts.StreamOptions()),
	}
	return left
}

func (e *TimerElem) processValue(timestamp time.Time, agg aggregation.Timer, fn aggMetricFn) {
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
	fn(fullTimerPrefix, e.id, timerStdevSuffix, timestamp, agg.Stdev(), e.policy)
	for idx, q := range quantiles {
		v := agg.Quantile(q)
		if q == 0.5 {
			fn(fullTimerPrefix, e.id, timerMedianSuffix, timestamp, v, e.policy)
		}
		fn(fullTimerPrefix, e.id, quantileSuffixes[idx], timestamp, v, e.policy)
	}
}

// NewGaugeElem creates a new gauge element
func NewGaugeElem(id metric.ID, policy policy.Policy, opts Options) *GaugeElem {
	return &GaugeElem{
		elemBase: elemBase{opts: opts, id: id, policy: policy},
		values:   make([]timedGauge, 0, defaultNumValues), // in most cases values will have two entries
	}
}

// AddMetric adds a new gauge value
func (e *GaugeElem) AddMetric(timestamp time.Time, mu unaggregated.MetricUnion) error {
	if mu.Type != unaggregated.GaugeType {
		return errInvalidMetricType
	}
	alignedStart := timestamp.Truncate(e.policy.Resolution().Window).UnixNano()
	e.Lock()
	if e.closed {
		e.Unlock()
		return errGaugeElemClosed
	}
	idx := e.findOrInsertWithLock(alignedStart)
	e.values[idx].gauge.Set(mu.GaugeVal)
	e.Unlock()
	return nil
}

// Consume processes values before a given time and discards
// them afterwards, returning whether the element can be collected
// after discarding the values
func (e *GaugeElem) Consume(earlierThan time.Time, fn aggMetricFn) bool {
	earlierThanNs := earlierThan.UnixNano()
	e.Lock()
	if e.closed {
		e.Unlock()
		return false
	}
	idx := 0
	for range e.values {
		// Bail as soon as the timestamp is no later than the target time
		if e.values[idx].timeNs >= earlierThanNs {
			break
		}
		endAt := time.Unix(0, e.values[idx].timeNs).Add(e.policy.Resolution().Window)
		e.processValue(endAt, e.values[idx].gauge, fn)
		idx++
	}
	if idx > 0 {
		// Shift remaining values to the left and shrink the values slice
		n := copy(e.values[0:], e.values[idx:])
		e.values = e.values[:n]
	}
	canCollect := len(e.values) == 0 && e.tombstoned
	e.Unlock()
	return canCollect
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
	e.values = e.values[:0]
	pool := e.opts.GaugeElemPool()
	e.Unlock()

	pool.Put(e)
}

// findOrInsertWithLock finds the element index whose timestamp matches
// the start time passed in, or inserts one if it doesn't exist while
// maintaining the time ascending order of the values. The returned index
// is always valid.
func (e *GaugeElem) findOrInsertWithLock(alignedStart int64) int {
	numValues := len(e.values)
	// Optimize for the common case
	if numValues > 0 && e.values[numValues-1].timeNs == alignedStart {
		return numValues - 1
	}
	// Binary search for the unusual case. We intentionally do not
	// use the sort.Search() function because it requires passing
	// in a closure
	left, right := 0, numValues
	for left < right {
		mid := left + (right-left)/2 // avoid overflow
		if e.values[mid].timeNs < alignedStart {
			left = mid + 1
		} else {
			right = mid
		}
	}
	// If the current timestamp is equal to or larger than the target time,
	// return the index as is
	if left < numValues && e.values[left].timeNs == alignedStart {
		return left
	}
	// Otherwise we need to insert a new item
	e.values = append(e.values, emptyTimedGauge)
	// NB(xichen): it is ok if the source and the destination overlap
	copy(e.values[left+1:numValues+1], e.values[left:numValues])
	e.values[left] = timedGauge{
		timeNs: alignedStart,
		gauge:  aggregation.NewGauge(),
	}
	return left
}

func (e *GaugeElem) processValue(timestamp time.Time, agg aggregation.Gauge, fn aggMetricFn) {
	fn(e.opts.FullGaugePrefix(), e.id, nil, timestamp, agg.Value(), e.policy)
}
