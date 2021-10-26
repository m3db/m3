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
	"math"
	"sync"
	"time"

	"github.com/mauricelam/genny/generic"
	"github.com/willf/bitset"
	"go.uber.org/zap"

	raggregation "github.com/m3db/m3/src/aggregator/aggregation"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"
)

type typeSpecificAggregation interface {
	generic.Type

	// Add adds a new metric value.
	Add(t time.Time, value float64, annotation []byte)

	// UpdateVal updates a previously added value.
	UpdateVal(t time.Time, value float64, prevValue float64) error

	// AddUnion adds a new metric value union.
	AddUnion(t time.Time, mu unaggregated.MetricUnion)

	// Annotation returns the last annotation of aggregated values.
	Annotation() []byte

	// ValueOf returns the value for the given aggregation type.
	ValueOf(aggType maggregation.Type) float64

	// LastAt returns the time for last received value.
	LastAt() time.Time

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

	// Type returns the elem type.
	Type() metric.Type

	// FullPrefix returns the full prefix for the given metric type.
	FullPrefix(opts Options) []byte

	// DefaultAggregationTypes returns the default aggregation types.
	DefaultAggregationTypes(aggTypesOpts maggregation.TypesOptions) maggregation.Types

	// TypeStringFor returns the type string for the given aggregation type.
	TypeStringFor(aggTypesOpts maggregation.TypesOptions, aggType maggregation.Type) []byte

	// ElemPool returns the pool for the given element.
	ElemPool(opts Options) genericElemPool

	// NewAggregation creates a new aggregation.
	NewAggregation(opts Options, aggOpts raggregation.Options) typeSpecificAggregation

	// ResetSetData resets and sets data.
	ResetSetData(
		aggTypesOpts maggregation.TypesOptions,
		aggTypes maggregation.Types,
		useDefaultAggregation bool,
	) error

	// Close closes the element.
	Close()
}

type lockedAggregation struct {
	sync.Mutex

	dirty       bool
	flushed     bool
	closed      bool
	sourcesSeen map[uint32]*bitset.BitSet
	aggregation typeSpecificAggregation
	prevValues  []float64 // the previously emitted values (one per aggregation type).
}

type timedAggregation struct {
	startAtNanos xtime.UnixNano // start time of an aggregation window
	lockedAgg    *lockedAggregation

	// this is mutable data for specifying on each Consume which previous value the
	// current agg can reference (i.e. for binary ops). it must be mutable since the
	// set of vals within the buffer past can change and so on each consume a given agg's
	// previous depends on the state of values preceding the current at that point in time.
	previousTimeNanos xtime.UnixNano
}

func (ta *timedAggregation) Release() {
	ta.lockedAgg = nil
}

// GenericElem is an element storing time-bucketed aggregations.
type GenericElem struct {
	elemBase
	typeSpecificElemBase

	// startTime -> agg (new one per every resolution)
	values map[xtime.UnixNano]timedAggregation
	// sorted start aligned times that have been written to since the last flush
	dirty []xtime.UnixNano
	// min time in the values map. allow for iterating through map.
	minStartTime xtime.UnixNano

	// internal consume state that does not need to be synchronized.
	toConsume []timedAggregation // small buffer to avoid memory allocations during consumption
	toExpire  []timedAggregation // small buffer to avoid memory allocations during consumption
	// map of the previous consumed values for each timestamp in the buffer. needed to support binary transforms that
	// need the value from the previous timestamp.
	consumedValues valuesByTime
}

// NewGenericElem returns a new GenericElem.
func NewGenericElem(data ElemData, opts ElemOptions) (*GenericElem, error) {
	e := &GenericElem{
		elemBase: newElemBase(opts),
		dirty:    make([]xtime.UnixNano, 0, defaultNumAggregations), // in most cases values will have two entries
		values:   make(map[xtime.UnixNano]timedAggregation),
	}
	if err := e.ResetSetData(data); err != nil {
		return nil, err
	}
	return e, nil
}

// MustNewGenericElem returns a new GenericElem and panics if an error occurs.
func MustNewGenericElem(data ElemData, opts ElemOptions) *GenericElem {
	elem, err := NewGenericElem(data, opts)
	if err != nil {
		panic(fmt.Errorf("unable to create element: %v", err))
	}
	return elem
}

// ResetSetData resets the element and sets data.
func (e *GenericElem) ResetSetData(data ElemData) error {
	useDefaultAggregation := data.AggTypes.IsDefault()
	if useDefaultAggregation {
		data.AggTypes = e.DefaultAggregationTypes(e.aggTypesOpts)
	}
	if err := e.elemBase.resetSetData(data, useDefaultAggregation); err != nil {
		return err
	}
	return e.typeSpecificElemBase.ResetSetData(e.aggTypesOpts, data.AggTypes, useDefaultAggregation)
}

// ResendEnabled returns true if resends are enabled for the element.
func (e *GenericElem) ResendEnabled() bool {
	return e.resendEnabled
}

// AddUnion adds a metric value union at a given timestamp.
func (e *GenericElem) AddUnion(timestamp time.Time, mu unaggregated.MetricUnion) error {
	alignedStart := timestamp.Truncate(e.sp.Resolution().Window).UnixNano()
	lockedAgg, err := e.findOrCreate(alignedStart, createAggregationOptions{})
	if err != nil {
		return err
	}
	lockedAgg.Lock()
	if lockedAgg.closed {
		lockedAgg.Unlock()
		return errAggregationClosed
	}
	lockedAgg.aggregation.AddUnion(timestamp, mu)
	lockedAgg.Unlock()
	return nil
}

// AddValue adds a metric value at a given timestamp.
func (e *GenericElem) AddValue(timestamp time.Time, value float64, annotation []byte) error {
	alignedStart := timestamp.Truncate(e.sp.Resolution().Window).UnixNano()
	lockedAgg, err := e.findOrCreate(alignedStart, createAggregationOptions{})
	if err != nil {
		return err
	}
	lockedAgg.Lock()
	if lockedAgg.closed {
		lockedAgg.Unlock()
		return errAggregationClosed
	}
	lockedAgg.aggregation.Add(timestamp, value, annotation)
	lockedAgg.Unlock()
	return nil
}

// AddUnique adds a metric value from a given source at a given timestamp.
// If previous values from the same source have already been added to the
// same aggregation, the incoming value is discarded.
//nolint: dupl
func (e *GenericElem) AddUnique(
	timestamp time.Time,
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	alignedStart := timestamp.Truncate(e.sp.Resolution().Window).UnixNano()
	lockedAgg, err := e.findOrCreate(alignedStart, createAggregationOptions{initSourceSet: true})
	if err != nil {
		return err
	}
	lockedAgg.Lock()
	if lockedAgg.closed {
		lockedAgg.Unlock()
		return errAggregationClosed
	}
	versionsSeen := lockedAgg.sourcesSeen[metadata.SourceID]
	if versionsSeen == nil {
		// N.B - these bitsets will be transitively cached through the cached sources seen.
		versionsSeen = bitset.New(defaultNumVersions)
		lockedAgg.sourcesSeen[metadata.SourceID] = versionsSeen
	}
	version := uint(metric.Version)
	if versionsSeen.Test(version) {
		lockedAgg.Unlock()
		return errDuplicateForwardingSource
	}
	versionsSeen.Set(version)

	if metric.Version > 0 {
		e.metrics.updatedValues.Inc(1)
		for i := range metric.Values {
			if err := lockedAgg.aggregation.UpdateVal(timestamp, metric.Values[i], metric.PrevValues[i]); err != nil {
				return err
			}
		}
	} else {
		for _, v := range metric.Values {
			lockedAgg.aggregation.Add(timestamp, v, metric.Annotation)
		}
	}
	lockedAgg.Unlock()
	return nil
}

// remove expired aggregations from the values map.
func (e *GenericElem) expireValuesWithLock(
	expireStartTimeBefore xtime.UnixNano,
	timestampNanosFn timestampNanosFn,
	isEarlierThanFn isEarlierThanFn) {
	if len(e.values) == 0 {
		return
	}
	resolution := e.sp.Resolution().Window

	// always keep at least one value in the map for when calculating binary transforms. need to reference the previous
	// value.
	for isEarlierThanFn(int64(e.minStartTime), resolution, int64(expireStartTimeBefore)) {
		if v, ok := e.values[e.minStartTime]; ok {
			// Previous times are used to key into consumedValues, which are non-start-aligned. And so
			// we convert from startAligned here when setting previous.
			v.previousTimeNanos = xtime.UnixNano(timestampNanosFn(int64(e.minStartTime), resolution))
			e.toExpire = append(e.toExpire, v)

			delete(e.values, e.minStartTime)
		}
		e.minStartTime = e.minStartTime.Add(resolution)
	}
}

// return the timestamp in the values map that is before the provided time. returns false if the provided time is the
// smallest time or the map is empty.
func (e *GenericElem) previousStartAlignedWithLock(timestamp xtime.UnixNano) (xtime.UnixNano, bool) {
	if len(e.values) == 0 {
		return 0, false
	}
	resolution := e.sp.Resolution().Window
	startAligned := timestamp.Truncate(resolution).Add(-resolution)
	for !startAligned.Before(e.minStartTime) {
		_, ok := e.values[startAligned]
		if ok {
			return startAligned, true
		}
		startAligned = startAligned.Add(-resolution)
	}
	return 0, false
}

// return the next aggregation after the provided time. returns false if the provided time is the
// largest time or the map is empty.
func (e *GenericElem) nextAggWithLock(startAligned xtime.UnixNano, targetNanos int64,
	isEarlierThanFn isEarlierThanFn) (timedAggregation, bool) {
	if len(e.values) == 0 {
		return timedAggregation{}, false
	}
	resolution := e.sp.Resolution().Window
	ts := startAligned.Add(resolution)
	for isEarlierThanFn(int64(ts), resolution, targetNanos) {
		agg, ok := e.values[ts]
		if ok {
			return agg, true
		}
		ts = ts.Add(resolution)
	}
	return timedAggregation{}, false
}

// Consume consumes values before a given time and removes them from the element
// after they are consumed, returning whether the element can be collected after
// the consumption is completed.
// NB: Consume is not thread-safe and must be called within a single goroutine
// to avoid race conditions.
func (e *GenericElem) Consume(
	targetNanos int64,
	isEarlierThanFn isEarlierThanFn,
	timestampNanosFn timestampNanosFn,
	targetNanosFn targetNanosFn,
	flushLocalFn flushLocalMetricFn,
	flushForwardedFn flushForwardedMetricFn,
	onForwardedFlushedFn onForwardingElemFlushedFn,
) bool {
	resolution := e.sp.Resolution().Window
	// reverse engineer the allowed lateness.
	latenessAllowed := time.Duration(targetNanos - targetNanosFn(targetNanos))
	e.Lock()
	if e.closed {
		e.Unlock()
		return false
	}
	e.toConsume = e.toConsume[:0]

	// Evaluate and GC expired items.
	dirtyTimes := e.dirty
	e.dirty = e.dirty[:0]
	for _, dirtyTime := range dirtyTimes {
		agg, ok := e.values[dirtyTime]
		if !ok {
			dirtyTime := dirtyTime
			instrument.EmitAndLogInvariantViolation(e.opts.InstrumentOptions(), func(l *zap.Logger) {
				l.Error("dirty timestamp not in map", zap.Time("ts", dirtyTime.ToTime()))
			})
		}

		if !isEarlierThanFn(int64(dirtyTime), resolution, targetNanos) {
			e.dirty = append(e.dirty, dirtyTime)
			continue
		}

		agg.lockedAgg.dirty = false
		previousStartAligned, ok := e.previousStartAlignedWithLock(dirtyTime)
		if ok {
			agg.previousTimeNanos = xtime.UnixNano(timestampNanosFn(int64(previousStartAligned), resolution))
		}
		e.toConsume = append(e.toConsume, agg)

		// add the nextAgg to the dirty set as well in case we need to cascade the value.
		nextAgg, ok := e.nextAggWithLock(dirtyTime, targetNanos, isEarlierThanFn)
		// only add nextAgg if not already in the dirty set
		if ok && !nextAgg.lockedAgg.dirty {
			nextAgg.previousTimeNanos = xtime.UnixNano(timestampNanosFn(int64(dirtyTime), resolution))
			e.toConsume = append(e.toConsume, nextAgg)
		}
	}

	// To expire values, we want to expire anything earlier than a time
	// after we would not accept new writes
	minUpdateableTimeNanos := xtime.UnixNano(targetNanos)
	if e.resendEnabled {
		// If resend is enabled we are NOT waiting until the buffer expires to flush,
		// and therefore we need to expire only up to (consumeTime - bufferPast).
		// If resend is disabled, we can just expire up to the consumeTime
		// because the flushing already accounts for the entire buffer past range.
		minUpdateableTimeNanos = minUpdateableTimeNanos.Add(-e.bufferForPastTimedMetricFn(resolution))
	}

	// When expiring, we search for the previous existing value first, and then expire anything preceding that.
	// This ensures that we never entirely clear all values since we need to ensure one latest value at any given
	// point in time so that any future value will still have a previous reference.
	e.toExpire = e.toExpire[:0]
	expiredNanos, ok := e.previousStartAlignedWithLock(minUpdateableTimeNanos)
	if ok {
		e.expireValuesWithLock(expiredNanos, timestampNanosFn, isEarlierThanFn)
	}

	canCollect := len(e.dirty) == 0 && e.tombstoned
	e.Unlock()

	// Process the aggregations that are ready for consumption.
	for i := range e.toConsume {
		timeNanos := xtime.UnixNano(timestampNanosFn(int64(e.toConsume[i].startAtNanos), resolution))
		e.toConsume[i].lockedAgg.Lock()
		_ = e.processValueWithAggregationLock(
			timeNanos,
			e.toConsume[i].previousTimeNanos,
			e.toConsume[i].lockedAgg,
			flushLocalFn,
			flushForwardedFn,
			resolution,
			latenessAllowed,
		)
		e.toConsume[i].lockedAgg.flushed = true
		e.toConsume[i].lockedAgg.Unlock()
	}

	// Cleanup expired item after consuming since consuming still has a ref to the locked aggregation.
	for i := range e.toExpire {
		e.toExpire[i].lockedAgg.closed = true
		e.toExpire[i].lockedAgg.aggregation.Close()
		if e.toExpire[i].lockedAgg.sourcesSeen != nil {
			e.cachedSourceSetsLock.Lock()
			// This is to make sure there aren't too many cached source sets taking up
			// too much space.
			if len(e.cachedSourceSets) < e.opts.MaxNumCachedSourceSets() {
				e.cachedSourceSets = append(e.cachedSourceSets, e.toExpire[i].lockedAgg.sourcesSeen)
			}
			e.cachedSourceSetsLock.Unlock()
			e.toExpire[i].lockedAgg.sourcesSeen = nil
		}
		e.toExpire[i].Release()

		delete(e.consumedValues, e.toExpire[i].previousTimeNanos)
	}

	if e.parsedPipeline.HasRollup {
		forwardedAggregationKey, _ := e.ForwardedAggregationKey()
		onForwardedFlushedFn(e.onForwardedAggregationWrittenFn, forwardedAggregationKey)
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
	e.writeForwardedMetricFn = nil
	e.onForwardedAggregationWrittenFn = nil
	for idx := range e.cachedSourceSets {
		e.cachedSourceSets[idx] = nil
	}
	e.cachedSourceSets = nil

	resolution := e.sp.Resolution().Window
	// note: this is not in the hot path so it's ok to iterate over the map.
	// this allows to catch any bugs with unexpected entries still in the map.
	for k := range e.values {
		if k < e.minStartTime {
			k := k
			instrument.EmitAndLogInvariantViolation(e.opts.InstrumentOptions(), func(l *zap.Logger) {
				l.Error("aggregate timestamp is less than min",
					zap.Time("ts", k.ToTime()),
					zap.Time("min", e.minStartTime.ToTime()))
			})
		}
		delete(e.values, k)
		// Close the underlying aggregation objects.
		if v, ok := e.values[e.minStartTime]; ok {
			v.lockedAgg.sourcesSeen = nil
			v.lockedAgg.aggregation.Close()
			v.Release()
			delete(e.values, e.minStartTime)
		}
		e.minStartTime = e.minStartTime.Add(resolution)
	}
	e.typeSpecificElemBase.Close()
	aggTypesPool := e.aggTypesOpts.TypesPool()
	pool := e.ElemPool(e.opts)
	e.Unlock()

	// internal consumption state that doesn't need to be synchronized.
	e.toConsume = e.toConsume[:0]
	e.dirty = e.dirty[:0]
	e.toExpire = e.toExpire[:0]
	e.consumedValues = nil
	e.minStartTime = 0

	if !e.useDefaultAggregation {
		aggTypesPool.Put(e.aggTypes)
	}
	pool.Put(e)
}

func (e *GenericElem) insertDirty(alignedStart xtime.UnixNano) {
	numValues := len(e.dirty)

	// Optimize for the common case.
	if numValues > 0 && e.dirty[numValues-1] == alignedStart {
		return
	}
	// Binary search for the unusual case. We intentionally do not
	// use the sort.Search() function because it requires passing
	// in a closure.
	left, right := 0, numValues
	for left < right {
		mid := left + (right-left)/2 // avoid overflow
		if e.dirty[mid] < alignedStart {
			left = mid + 1
		} else {
			right = mid
		}
	}
	// If the current timestamp is equal to or larger than the target time,
	// return the index as is.
	if left < numValues && e.dirty[left] == alignedStart {
		return
	}

	e.dirty = append(e.dirty, 0)
	copy(e.dirty[left+1:numValues+1], e.dirty[left:numValues])
	e.dirty[left] = alignedStart
}

// find finds the aggregation for a given time, or returns nil.
//nolint: dupl
func (e *GenericElem) find(alignedStartNanos xtime.UnixNano) (*lockedAggregation, bool, error) {
	e.RLock()
	if e.closed {
		e.RUnlock()
		return nil, false, errElemClosed
	}
	timedAgg, ok := e.values[alignedStartNanos]
	if ok {
		dirty := timedAgg.lockedAgg.dirty
		e.RUnlock()
		return timedAgg.lockedAgg, dirty, nil
	}
	e.RUnlock()
	return nil, false, nil
}

// findOrCreate finds the aggregation for a given time, or creates one
// if it doesn't exist.
//nolint: dupl
func (e *GenericElem) findOrCreate(
	alignedStartNanos int64,
	createOpts createAggregationOptions,
) (*lockedAggregation, error) {
	alignedStart := xtime.UnixNano(alignedStartNanos)
	found, isDirty, err := e.find(alignedStart)
	if err != nil {
		return nil, err
	}
	if found != nil && isDirty {
		return found, err
	}

	e.Lock()
	if e.closed {
		e.Unlock()
		return nil, errElemClosed
	}

	timedAgg, ok := e.values[alignedStart]
	if ok {
		if !timedAgg.lockedAgg.dirty {
			timedAgg.lockedAgg.dirty = true
			e.insertDirty(alignedStart)
		}
		e.Unlock()
		return timedAgg.lockedAgg, nil
	}

	var sourcesSeen map[uint32]*bitset.BitSet
	if createOpts.initSourceSet {
		e.cachedSourceSetsLock.Lock()
		if numCachedSourceSets := len(e.cachedSourceSets); numCachedSourceSets > 0 {
			sourcesSeen = e.cachedSourceSets[numCachedSourceSets-1]
			e.cachedSourceSets[numCachedSourceSets-1] = nil
			e.cachedSourceSets = e.cachedSourceSets[:numCachedSourceSets-1]
			for _, bs := range sourcesSeen {
				bs.ClearAll()
			}
		} else {
			sourcesSeen = make(map[uint32]*bitset.BitSet)
		}
		e.cachedSourceSetsLock.Unlock()
	}
	timedAgg = timedAggregation{
		startAtNanos: alignedStart,
		lockedAgg: &lockedAggregation{
			sourcesSeen: sourcesSeen,
			aggregation: e.NewAggregation(e.opts, e.aggOpts),
			prevValues:  make([]float64, len(e.aggTypes)),
			dirty:       true,
		},
	}
	e.values[alignedStart] = timedAgg
	e.insertDirty(alignedStart)
	if len(e.values) == 1 || e.minStartTime > alignedStart {
		e.minStartTime = alignedStart
	}
	e.Unlock()
	return timedAgg.lockedAgg, nil
}

// returns true if a datapoint is emitted.
func (e *GenericElem) processValueWithAggregationLock(
	timeNanos xtime.UnixNano,
	prevTimeNanos xtime.UnixNano,
	lockedAgg *lockedAggregation,
	flushLocalFn flushLocalMetricFn,
	flushForwardedFn flushForwardedMetricFn,
	resolution time.Duration,
	latenessAllowed time.Duration) bool {
	var (
		transformations  = e.parsedPipeline.Transformations
		discardNaNValues = e.opts.DiscardNaNAggregatedValues()
		emitted          bool
	)
	for aggTypeIdx, aggType := range e.aggTypes {
		var extraDp transformation.Datapoint
		value := lockedAgg.aggregation.ValueOf(aggType)
		for _, transformOp := range transformations {
			unaryOp, isUnaryOp := transformOp.UnaryTransform()
			binaryOp, isBinaryOp := transformOp.BinaryTransform()
			unaryMultiOp, isUnaryMultiOp := transformOp.UnaryMultiOutputTransform()
			switch {
			case isUnaryOp:
				curr := transformation.Datapoint{
					TimeNanos: int64(timeNanos),
					Value:     value,
				}

				res := unaryOp.Evaluate(curr)

				value = res.Value

			case isBinaryOp:
				prev := transformation.Datapoint{
					Value: nan,
				}
				// lazily construct consumedValues since they are only needed by binary transforms.
				if e.consumedValues == nil {
					e.consumedValues = make(valuesByTime)
				}
				if _, ok := e.consumedValues[prevTimeNanos]; ok {
					prev = e.consumedValues[prevTimeNanos][aggTypeIdx]
				}
				curr := transformation.Datapoint{
					TimeNanos: int64(timeNanos),
					Value:     value,
				}
				res := binaryOp.Evaluate(prev, curr, transformation.FeatureFlags{})

				// NB: we only need to record the value needed for derivative transformations.
				// We currently only support first-order derivative transformations so we only
				// need to keep one value. In the future if we need to support higher-order
				// derivative transformations, we need to store an array of values here.
				if !math.IsNaN(curr.Value) {
					if e.consumedValues[timeNanos] == nil {
						e.consumedValues[timeNanos] = make([]transformation.Datapoint, len(e.aggTypes))
					}
					e.consumedValues[timeNanos][aggTypeIdx] = curr
				}

				value = res.Value
			case isUnaryMultiOp:
				curr := transformation.Datapoint{
					TimeNanos: int64(timeNanos),
					Value:     value,
				}

				var res transformation.Datapoint
				res, extraDp = unaryMultiOp.Evaluate(curr, resolution)
				value = res.Value
			}
		}

		if discardNaNValues && math.IsNaN(value) {
			continue
		}

		// It's ok to send a 0 prevValue on the first forward because it's not used in AddUnique unless it's a
		// resend (version > 0)
		prevValue := lockedAgg.prevValues[aggTypeIdx]
		lockedAgg.prevValues[aggTypeIdx] = value
		if lockedAgg.flushed {
			// no need to resend a value that hasn't changed.
			if (math.IsNaN(prevValue) && math.IsNaN(value)) || (prevValue == value) {
				continue
			}
		}
		emitted = true

		if !e.parsedPipeline.HasRollup {
			toFlush := make([]transformation.Datapoint, 0, 2)
			toFlush = append(toFlush, transformation.Datapoint{
				TimeNanos: int64(timeNanos),
				Value:     value,
			})
			if extraDp.TimeNanos != 0 {
				toFlush = append(toFlush, extraDp)
			}
			for _, point := range toFlush {
				switch e.idPrefixSuffixType {
				case NoPrefixNoSuffix:
					flushLocalFn(nil, e.id, nil, point.TimeNanos, point.Value, lockedAgg.aggregation.Annotation(), e.sp)
				case WithPrefixWithSuffix:
					flushLocalFn(e.FullPrefix(e.opts), e.id, e.TypeStringFor(e.aggTypesOpts, aggType),
						point.TimeNanos, point.Value, lockedAgg.aggregation.Annotation(), e.sp)
				}
			}
		} else {
			forwardedAggregationKey, _ := e.ForwardedAggregationKey()
			// only record lag for the initial flush (not resends)
			if !lockedAgg.flushed {
				// latenessAllowed is not due to processing delay, so it remove it from lag calc.
				e.forwardLagMetric(resolution).RecordDuration(time.Since(timeNanos.ToTime().Add(-latenessAllowed)))
			}
			flushForwardedFn(e.writeForwardedMetricFn, forwardedAggregationKey,
				int64(timeNanos), value, prevValue, lockedAgg.aggregation.Annotation())
		}
	}
	return emitted
}
