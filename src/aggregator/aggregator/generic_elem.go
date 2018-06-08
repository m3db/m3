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

package aggregator

import (
	"fmt"
	"sync"
	"time"

	raggregation "github.com/m3db/m3aggregator/aggregation"
	maggregation "github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/op/applied"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/transformation"

	"github.com/mauricelam/genny/generic"
)

type lockedAggregation interface {
	generic.Type
	sync.Locker

	// Add adds a new metric value.
	Add(mu unaggregated.MetricUnion)

	// ValueOf returns the value for the given aggregation type.
	ValueOf(aggType maggregation.Type) float64

	// Close closes the aggregation object.
	Close()
}

type genericElemPool interface {
	generic.Type

	// Put returns an element to a pool.
	Put(value *GenericElem)
}

type typeSpecificElemBase interface {
	generic.Type

	// FullPrefix returns the full prefix for the given metric type.
	FullPrefix(opts Options) []byte

	// DefaultAggregationTypes returns the default aggregation types.
	DefaultAggregationTypes(aggTypesOpts maggregation.TypesOptions) maggregation.Types

	// TypeStringFor returns the type string for the given aggregation type.
	TypeStringFor(aggTypesOpts maggregation.TypesOptions, aggType maggregation.Type) []byte

	// ElemPool returns the pool for the given element.
	ElemPool(opts Options) genericElemPool

	// NewLockedAggregation creates a new locked aggregation.
	NewLockedAggregation(opts Options, aggOpts raggregation.Options) lockedAggregation

	// ResetSetData resets and sets data.
	ResetSetData(
		aggTypesOpts maggregation.TypesOptions,
		aggTypes maggregation.Types,
		useDefaultAggregation bool,
	) error

	// Close closes the element.
	Close()
}

type timedAggregation struct {
	timeNanos   int64
	aggregation lockedAggregation
}

func (ta *timedAggregation) Reset() {
	ta.timeNanos = 0
	ta.aggregation = nil
}

// GenericElem is an element storing time-bucketed aggregations.
type GenericElem struct {
	elemBase
	typeSpecificElemBase

	values              []timedAggregation // metric aggregations sorted by time in ascending order
	toConsume           []timedAggregation // small buffer to avoid memory allocations during consumption
	lastConsumedAtNanos int64              // last consumed at in Unix nanoseconds
	lastConsumedValues  []float64          // last consumed values
}

// NewGenericElem creates a new element for the given metric type.
func NewGenericElem(
	id id.RawID,
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
	opts Options,
) (*GenericElem, error) {
	e := &GenericElem{
		elemBase: newElemBase(opts),
		values:   make([]timedAggregation, 0, defaultNumValues), // in most cases values will have two entries
	}
	if err := e.ResetSetData(id, sp, aggTypes, pipeline); err != nil {
		return nil, err
	}
	return e, nil
}

// MustNewGenericElem creates a new element, or panics if the input is invalid.
func MustNewGenericElem(
	id id.RawID,
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
	opts Options,
) *GenericElem {
	elem, err := NewGenericElem(id, sp, aggTypes, pipeline, opts)
	if err != nil {
		panic(fmt.Errorf("unable to create element: %v", err))
	}
	return elem
}

// ResetSetData resets the element and sets data.
func (e *GenericElem) ResetSetData(
	id id.RawID,
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
) error {
	useDefaultAggregation := aggTypes.IsDefault()
	if useDefaultAggregation {
		aggTypes = e.DefaultAggregationTypes(e.aggTypesOpts)
	}
	if err := e.elemBase.resetSetData(id, sp, aggTypes, useDefaultAggregation, pipeline); err != nil {
		return err
	}
	if err := e.typeSpecificElemBase.ResetSetData(e.aggTypesOpts, aggTypes, useDefaultAggregation); err != nil {
		return err
	}
	// If the pipeline contains derivative transformations, we need to store past
	// values in order to compute the derivatives.
	if !e.parsedPipeline.HasDerivativeTransform {
		return nil
	}
	numAggTypes := len(e.aggTypes)
	if cap(e.lastConsumedValues) < numAggTypes {
		e.lastConsumedValues = make([]float64, numAggTypes)
	}
	e.lastConsumedValues = e.lastConsumedValues[:numAggTypes]
	for i := 0; i < len(e.lastConsumedValues); i++ {
		e.lastConsumedValues[i] = nan
	}
	return nil
}

// AddMetric adds a new metric value.
func (e *GenericElem) AddMetric(timestamp time.Time, mu unaggregated.MetricUnion) error {
	alignedStart := timestamp.Truncate(e.sp.Resolution().Window).UnixNano()
	agg, err := e.findOrCreate(alignedStart)
	if err != nil {
		return err
	}
	agg.Lock()
	agg.Add(mu)
	agg.Unlock()
	return nil
}

// Consume consumes values before a given time and removes them from the element
// after they are consumed, returning whether the element can be collected after
// the consumption is completed.
// NB: Consume is not thread-safe and must be called within a single goroutine
// to avoid race conditions.
func (e *GenericElem) Consume(
	earlierThanNanos int64,
	flushLocalFn flushLocalMetricFn,
	flushForwardedFn flushForwardedMetricFn,
) bool {
	e.Lock()
	if e.closed {
		e.Unlock()
		return false
	}
	idx := 0
	for range e.values {
		// Bail as soon as the timestamp is no later than the target time.
		if e.values[idx].timeNanos >= earlierThanNanos {
			break
		}
		idx++
	}
	e.toConsume = e.toConsume[:0]
	if idx > 0 {
		// Shift remaining values to the left and shrink the values slice.
		e.toConsume = append(e.toConsume, e.values[:idx]...)
		n := copy(e.values[0:], e.values[idx:])
		// Clear out the invalid items to avoid holding references to objects
		// for reduced GC overhead..
		for i := n; i < len(e.values); i++ {
			e.values[i].Reset()
		}
		e.values = e.values[:n]
	}
	canCollect := len(e.values) == 0 && e.tombstoned
	e.Unlock()

	// Process the aggregations that are ready for consumption.
	for i := range e.toConsume {
		endAtNanos := e.toConsume[i].timeNanos + int64(e.sp.Resolution().Window)
		e.processValue(endAtNanos, e.toConsume[i].aggregation, flushLocalFn, flushForwardedFn)
		// Closes the aggregation object after it's processed.
		e.toConsume[i].aggregation.Close()
		e.toConsume[i].Reset()
	}

	return canCollect
}

// Close closes the element.
func (e *GenericElem) Close() {
	e.Lock()
	if e.closed {
		e.Unlock()
		return
	}
	e.closed = true
	e.id = nil
	e.parsedPipeline = parsedPipeline{}
	for idx := range e.values {
		// Close the underlying aggregation objects.
		e.values[idx].aggregation.Close()
		e.values[idx].Reset()
	}
	e.values = e.values[:0]
	e.toConsume = e.toConsume[:0]
	e.lastConsumedValues = e.lastConsumedValues[:0]
	e.typeSpecificElemBase.Close()
	aggTypesPool := e.aggTypesOpts.TypesPool()
	pool := e.ElemPool(e.opts)
	e.Unlock()

	if !e.useDefaultAggregation {
		aggTypesPool.Put(e.aggTypes)
	}
	pool.Put(e)
}

// findOrCreate finds the aggregation for a given time, or creates one
// if it doesn't exist.
func (e *GenericElem) findOrCreate(alignedStart int64) (lockedAggregation, error) {
	e.RLock()
	if e.closed {
		e.RUnlock()
		return nil, errElemClosed
	}
	idx, found := e.indexOfWithLock(alignedStart)
	if found {
		agg := e.values[idx].aggregation
		e.RUnlock()
		return agg, nil
	}
	e.RUnlock()

	e.Lock()
	if e.closed {
		e.Unlock()
		return nil, errElemClosed
	}
	idx, found = e.indexOfWithLock(alignedStart)
	if found {
		agg := e.values[idx].aggregation
		e.Unlock()
		return agg, nil
	}

	// If not found, create a new aggregation.
	numValues := len(e.values)
	e.values = append(e.values, timedAggregation{})
	copy(e.values[idx+1:numValues+1], e.values[idx:numValues])
	e.values[idx] = timedAggregation{
		timeNanos:   alignedStart,
		aggregation: e.NewLockedAggregation(e.opts, e.aggOpts),
	}
	agg := e.values[idx].aggregation
	e.Unlock()
	return agg, nil
}

// indexOfWithLock finds the smallest element index whose timestamp
// is no smaller than the start time passed in, and true if it's an
// exact match, false otherwise.
func (e *GenericElem) indexOfWithLock(alignedStart int64) (int, bool) {
	numValues := len(e.values)
	// Optimize for the common case.
	if numValues > 0 && e.values[numValues-1].timeNanos == alignedStart {
		return numValues - 1, true
	}
	// Binary search for the unusual case. We intentionally do not
	// use the sort.Search() function because it requires passing
	// in a closure.
	left, right := 0, numValues
	for left < right {
		mid := left + (right-left)/2 // avoid overflow
		if e.values[mid].timeNanos < alignedStart {
			left = mid + 1
		} else {
			right = mid
		}
	}
	// If the current timestamp is equal to or larger than the target time,
	// return the index as is.
	if left < numValues && e.values[left].timeNanos == alignedStart {
		return left, true
	}
	return left, false
}

func (e *GenericElem) processValue(
	timeNanos int64,
	agg lockedAggregation,
	flushLocalFn flushLocalMetricFn,
	flushForwardedFn flushForwardedMetricFn,
) {
	var (
		fullPrefix      = e.FullPrefix(e.opts)
		transformations = e.parsedPipeline.Transformations
	)
	agg.Lock()
	for aggTypeIdx, aggType := range e.aggTypes {
		value := agg.ValueOf(aggType)
		for i := 0; i < transformations.NumSteps(); i++ {
			transformType := transformations.At(i).Transformation.Type
			if transformType.IsUnaryTransform() {
				fn := transformType.MustUnaryTransform()
				res := fn(transformation.Datapoint{TimeNanos: timeNanos, Value: value})
				value = res.Value
			} else {
				fn := transformType.MustBinaryTransform()
				prev := transformation.Datapoint{TimeNanos: e.lastConsumedAtNanos, Value: e.lastConsumedValues[aggTypeIdx]}
				curr := transformation.Datapoint{TimeNanos: timeNanos, Value: value}
				res := fn(prev, curr)
				// NB: we only need to record the value needed for derivative transformations.
				// We currently only support first-order derivative transformations so we only
				// need to keep one value. In the future if we need to support higher-order
				// derivative transformations, we need to store an array of values here.
				e.lastConsumedValues[aggTypeIdx] = value
				value = res.Value
			}
		}
		// Do we need to flush or forward NaNs?
		if !e.parsedPipeline.HasRollup {
			flushLocalFn(fullPrefix, e.id, e.TypeStringFor(e.aggTypesOpts, aggType), timeNanos, value, e.sp)
		} else {
			fm := aggregated.Metric{
				ID:        e.parsedPipeline.Rollup.ID,
				TimeNanos: timeNanos,
				Value:     value,
			}
			meta := metadata.ForwardMetadata{
				AggregationID: e.parsedPipeline.Rollup.AggregationID,
				StoragePolicy: e.sp,
				Pipeline:      e.parsedPipeline.Remainder,
			}
			flushForwardedFn(fm, meta)
		}
	}
	e.lastConsumedAtNanos = timeNanos
	agg.Unlock()
}
