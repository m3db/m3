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
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/uber-go/tally"
	"github.com/willf/bitset"
	"go.uber.org/zap"

	raggregation "github.com/m3db/m3/src/aggregator/aggregation"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	mpipeline "github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	// Default number of aggregation buckets allocated initially.
	defaultNumAggregations = 2

	// Default initial number of sources.
	defaultNumSources = 1024

	// Default number of versions.
	defaultNumVersions = 4

	// Maximum transformation derivative order that is supported.
	// A default value of 1 means we currently only support transformations that
	// compute first-order derivatives. This applies to the most common usecases
	// without imposing significant bookkeeping overhead.
	maxSupportedTransformationDerivativeOrder = 1
)

var (
	nan                          = math.NaN()
	errElemClosed                = errors.New("element is closed")
	errAggregationClosed         = errors.New("aggregation is closed")
	errDuplicateForwardingSource = errors.New("duplicate forwarding source")
)

// isEarlierThanFn determines whether the timestamps of the metrics in a given
// aggregation window are earlier than the given target time.
type isEarlierThanFn func(windowStartNanos int64, resolution time.Duration, targetNanos int64) bool

// timestampNanosFn determines the final timestamps of metrics in a given aggregation
// window with a given resolution.
type timestampNanosFn func(windowStartNanos int64, resolution time.Duration) int64

type createAggregationOptions struct {
	// initSourceSet determines whether to initialize the source set.
	initSourceSet bool
	resendEnabled bool
}

// IDPrefixSuffixType configs if the id should be added with prefix or suffix
// after aggregation.
type IDPrefixSuffixType int

const (
	// WithPrefixWithSuffix adds both prefix and suffix to the id after aggregation.
	WithPrefixWithSuffix IDPrefixSuffixType = iota
	// NoPrefixNoSuffix adds neither prefix nor suffix to the id after aggregation.
	NoPrefixNoSuffix
)

// metricElem is the common interface for metric elements.
type metricElem interface {
	Registerable
	// ID returns the metric id.
	ID() id.RawID

	// ResetSetData resets the element and sets data.
	ResetSetData(data ElemData) error

	// SetForwardedCallbacks sets the callback functions to write forwarded
	// metrics for elements producing such forwarded metrics.
	SetForwardedCallbacks(
		writeFn writeForwardedMetricFn,
		onDoneFn onForwardedAggregationDoneFn,
	)

	// AddUnion adds a metric value union at a given timestamp.
	AddUnion(timestamp time.Time, mu unaggregated.MetricUnion, resendEnabled bool) error

	// AddMetric adds a metric value at a given timestamp.
	AddValue(timestamp time.Time, value float64, annotation []byte) error

	// AddUnique adds a metric value from a given source at a given timestamp.
	// If previous values from the same source/version have already been added to the
	// same aggregation, the incoming value is discarded.
	AddUnique(timestamp time.Time, metric aggregated.ForwardedMetric, metadata metadata.ForwardMetadata) error

	// Consume consumes values before a given time and removes
	// them from the element after they are consumed, returning whether
	// the element can be collected after the consumption is completed.
	Consume(
		targetNanos int64,
		isEarlierThanFn isEarlierThanFn,
		timestampNanosFn timestampNanosFn,
		targetNanosFn targetNanosFn,
		flushLocalFn flushLocalMetricFn,
		flushForwardedFn flushForwardedMetricFn,
		onForwardedFlushedFn onForwardingElemFlushedFn,
		jitter time.Duration,
		flushType flushType,
	) bool

	// MarkAsTombstoned marks an element as tombstoned, which means this element
	// will be deleted once its aggregated values have been flushed.
	MarkAsTombstoned()

	// Close closes the element.
	Close()
}

// ElemData are initialization parameters for an element.
type ElemData struct {
	ID                 id.RawID
	StoragePolicy      policy.StoragePolicy
	AggTypes           maggregation.Types
	Pipeline           applied.Pipeline
	NumForwardedTimes  int
	IDPrefixSuffixType IDPrefixSuffixType
	ListType           metricListType
}

// nolint: maligned
type elemBase struct {
	sync.RWMutex

	// Immutable states.
	opts                            Options
	aggTypesOpts                    maggregation.TypesOptions
	id                              id.RawID
	sp                              policy.StoragePolicy
	useDefaultAggregation           bool
	aggTypes                        maggregation.Types
	aggOpts                         raggregation.Options
	parsedPipeline                  parsedPipeline
	numForwardedTimes               int
	idPrefixSuffixType              IDPrefixSuffixType
	writeForwardedMetricFn          writeForwardedMetricFn
	onForwardedAggregationWrittenFn onForwardedAggregationDoneFn
	metrics                         *elemMetrics
	bufferForPastTimedMetricFn      BufferForPastTimedMetricFn
	listType                        metricListType

	// Mutable states.
	tombstoned       bool
	closed           bool
	cachedSourceSets []map[uint32]*bitset.BitSet // nolint: structcheck
	// a cache of the lag metrics that don't require grabbing a lock to access.
	forwardLagMetrics map[forwardLagKey]tally.Histogram
}

// consumeState is transient state for a timedAggregation that can change every flush round.
// this state is thrown away after the timedAggregation is processed in a flush round.
type consumeState struct {
	// the annotation copied from the lockedAgg.
	annotation []byte
	// the values copied from the lockedAgg.
	values []float64
	// the start time of the aggregation.
	startAt xtime.UnixNano
	// the start aligned timestamp of the previous aggregation. used to lookup the consumedValues of the previous
	// aggregation for binary transformations.
	prevStartTime xtime.UnixNano
	// the dirty bit copied from the lockedAgg.
	dirty bool
	// copied from the timedAggregation
	resendEnabled bool
}

// mutable state for a timedAggregation that is local to the flusher. does not need to be synchronized.
// this state is kept around for the lifetime of the timedAggregation.
type flushState struct {
	// the consumed values from the previous flush. used for binary transformations. note these are the values before
	// transformation. emittedValues are after transformation.
	consumedValues []float64
	// the emitted values from the previous flush. used to determine if the emitted values have not changed and
	// can be skipped.
	emittedValues []float64
	// true if this aggregation has ever been flushed.
	flushed bool
}

var isDirty = func(state consumeState) bool {
	return state.dirty
}

// close is called when the aggregation has expired and is no longer needed.
func (f *flushState) close() {
	f.consumedValues = f.consumedValues[:0]
	f.emittedValues = f.emittedValues[:0]
}

type elemMetrics struct {
	sync.RWMutex
	scope         tally.Scope
	updatedValues tally.Counter
	retriedValues tally.Counter
	forwardLags   map[forwardLagKey]tally.Histogram
}

type forwardLagKey struct {
	resolution    time.Duration
	listType      metricListType
	flushType     flushType
	fwdType       string
	jitterApplied bool
}

func (e *elemMetrics) forwardLagMetric(key forwardLagKey) tally.Histogram {
	e.RLock()
	m, ok := e.forwardLags[key]
	if ok {
		e.RUnlock()
		return m
	}
	e.RUnlock()
	e.Lock()
	m, ok = e.forwardLags[key]
	if ok {
		e.Unlock()
		return m
	}
	jitterApplied := "false"
	if key.jitterApplied {
		jitterApplied = "true"
	}
	m = e.scope.
		Tagged(map[string]string{
			"resolution": key.resolution.String(),
			"list-type":  key.listType.String(),
			"flush-type": key.flushType.String(),
			"type":       key.fwdType,
			"jitter":     jitterApplied,
		}).
		Histogram("forward-lag", tally.DurationBuckets{
			10 * time.Millisecond,
			500 * time.Millisecond,
			time.Second,
			2 * time.Second,
			5 * time.Second,
			10 * time.Second,
			15 * time.Second,
			20 * time.Second,
			25 * time.Second,
			30 * time.Second,
			35 * time.Second,
			40 * time.Second,
			45 * time.Second,
			60 * time.Second,
			90 * time.Second,
			120 * time.Second,
		})
	e.forwardLags[key] = m
	e.Unlock()
	return m
}

// ElemOptions are the parameters for constructing a new elemBase.
type ElemOptions struct {
	aggregatorOpts  Options
	elemMetrics     *elemMetrics
	aggregationOpts raggregation.Options
}

// NewElemOptions constructs a new ElemOptions
func NewElemOptions(aggregatorOpts Options) ElemOptions {
	scope := aggregatorOpts.InstrumentOptions().MetricsScope()
	return ElemOptions{
		aggregatorOpts:  aggregatorOpts,
		aggregationOpts: raggregation.NewOptions(aggregatorOpts.InstrumentOptions()),
		elemMetrics: &elemMetrics{
			updatedValues: scope.Counter("updated-values"),
			retriedValues: scope.Counter("retried-values"),
			scope:         scope,
			forwardLags:   make(map[forwardLagKey]tally.Histogram),
		},
	}
}

func newElemBase(opts ElemOptions) elemBase {
	return elemBase{
		opts:                       opts.aggregatorOpts,
		aggTypesOpts:               opts.aggregatorOpts.AggregationTypesOptions(),
		aggOpts:                    opts.aggregationOpts,
		metrics:                    opts.elemMetrics,
		bufferForPastTimedMetricFn: opts.aggregatorOpts.BufferForPastTimedMetricFn(),
		forwardLagMetrics:          make(map[forwardLagKey]tally.Histogram),
	}
}

func (e *elemBase) forwardLagMetric(
	resolution time.Duration, fwdType string, jitterApplied bool, flushType flushType,
) tally.Histogram {
	key := forwardLagKey{
		resolution:    resolution,
		listType:      e.listType,
		fwdType:       fwdType,
		flushType:     flushType,
		jitterApplied: jitterApplied,
	}
	m, ok := e.forwardLagMetrics[key]
	if !ok {
		// if not cached locally, get from the singleton map that requires locking.
		m = e.metrics.forwardLagMetric(key)
		e.forwardLagMetrics[key] = m
	}
	return m
}

// resetSetData resets the element base and sets data.
func (e *elemBase) resetSetData(data ElemData, useDefaultAggregation bool) error {
	parsed, err := newParsedPipeline(data.Pipeline)
	if err != nil {
		l := e.opts.InstrumentOptions().Logger()
		l.Error("error parsing pipeline", zap.Error(err))
		return err
	}
	e.id = data.ID
	e.sp = data.StoragePolicy
	e.aggTypes = data.AggTypes
	e.useDefaultAggregation = useDefaultAggregation
	e.aggOpts.ResetSetData(data.AggTypes)
	e.parsedPipeline = parsed
	e.numForwardedTimes = data.NumForwardedTimes
	e.tombstoned = false
	e.closed = false
	e.idPrefixSuffixType = data.IDPrefixSuffixType
	e.listType = data.ListType
	return nil
}

func (e *elemBase) SetForwardedCallbacks(
	writeFn writeForwardedMetricFn,
	onDoneFn onForwardedAggregationDoneFn,
) {
	e.writeForwardedMetricFn = writeFn
	e.onForwardedAggregationWrittenFn = onDoneFn
}

func (e *elemBase) ID() id.RawID { return e.id }

func (e *elemBase) ForwardedID() (id.RawID, bool) {
	if !e.parsedPipeline.HasRollup {
		return nil, false
	}
	return e.parsedPipeline.Rollup.ID, true
}

func (e *elemBase) ForwardedAggregationKey() (aggregationKey, bool) {
	if !e.parsedPipeline.HasRollup {
		return aggregationKey{}, false
	}
	return aggregationKey{
		aggregationID:     e.parsedPipeline.Rollup.AggregationID,
		storagePolicy:     e.sp,
		pipeline:          e.parsedPipeline.Remainder,
		numForwardedTimes: e.numForwardedTimes + 1,
	}, true
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

type counterElemBase struct{}

func (e counterElemBase) Type() metric.Type { return metric.CounterType }

func (e counterElemBase) FullPrefix(opts Options) []byte { return opts.FullCounterPrefix() }

func (e counterElemBase) DefaultAggregationTypes(aggTypesOpts maggregation.TypesOptions) maggregation.Types {
	return aggTypesOpts.DefaultCounterAggregationTypes()
}

func (e counterElemBase) TypeStringFor(aggTypesOpts maggregation.TypesOptions, aggType maggregation.Type) []byte {
	return aggTypesOpts.TypeStringForCounter(aggType)
}

func (e counterElemBase) ElemPool(opts Options) CounterElemPool { return opts.CounterElemPool() }

func (e counterElemBase) NewAggregation(_ Options, aggOpts raggregation.Options) counterAggregation {
	return newCounterAggregation(raggregation.NewCounter(aggOpts))
}

func (e *counterElemBase) ResetSetData(
	_ maggregation.TypesOptions,
	aggTypes maggregation.Types,
	_ bool,
) error {
	if !aggTypes.IsValidForCounter() {
		return fmt.Errorf("invalid aggregation types %s for counter", aggTypes.String())
	}
	return nil
}

func (e *counterElemBase) Close() {}

type timerElemBase struct {
	quantiles     []float64
	quantilesPool pool.FloatsPool
}

func (e timerElemBase) Type() metric.Type { return metric.TimerType }

func (e timerElemBase) FullPrefix(opts Options) []byte { return opts.FullTimerPrefix() }

func (e timerElemBase) DefaultAggregationTypes(aggTypesOpts maggregation.TypesOptions) maggregation.Types {
	return aggTypesOpts.DefaultTimerAggregationTypes()
}

func (e timerElemBase) TypeStringFor(aggTypesOpts maggregation.TypesOptions, aggType maggregation.Type) []byte {
	return aggTypesOpts.TypeStringForTimer(aggType)
}

func (e timerElemBase) ElemPool(opts Options) TimerElemPool { return opts.TimerElemPool() }

func (e timerElemBase) NewAggregation(opts Options, aggOpts raggregation.Options) timerAggregation {
	newTimer := raggregation.NewTimer(e.quantiles, opts.StreamOptions(), aggOpts)
	return newTimerAggregation(newTimer)
}

func (e *timerElemBase) ResetSetData(
	aggTypesOpts maggregation.TypesOptions,
	aggTypes maggregation.Types,
	useDefaultAggregation bool,
) error {
	if !aggTypes.IsValidForTimer() {
		return fmt.Errorf("invalid aggregation types %s for timer", aggTypes.String())
	}
	if useDefaultAggregation {
		e.quantiles = aggTypesOpts.Quantiles()
		e.quantilesPool = nil
		return nil
	}

	var (
		quantilesPool     = aggTypesOpts.QuantilesPool()
		isQuantilesPooled bool
	)
	e.quantiles, isQuantilesPooled = aggTypes.PooledQuantiles(quantilesPool)
	if isQuantilesPooled {
		e.quantilesPool = quantilesPool
	} else {
		e.quantilesPool = nil
	}
	return nil
}

func (e *timerElemBase) Close() {
	if e.quantilesPool != nil {
		e.quantilesPool.Put(e.quantiles)
	}
	e.quantiles = nil
	e.quantilesPool = nil
}

type gaugeElemBase struct{}

func (e gaugeElemBase) Type() metric.Type { return metric.GaugeType }

func (e gaugeElemBase) FullPrefix(opts Options) []byte { return opts.FullGaugePrefix() }

func (e gaugeElemBase) DefaultAggregationTypes(aggTypesOpts maggregation.TypesOptions) maggregation.Types {
	return aggTypesOpts.DefaultGaugeAggregationTypes()
}

func (e gaugeElemBase) TypeStringFor(aggTypesOpts maggregation.TypesOptions, aggType maggregation.Type) []byte {
	return aggTypesOpts.TypeStringForGauge(aggType)
}

func (e gaugeElemBase) ElemPool(opts Options) GaugeElemPool { return opts.GaugeElemPool() }

func (e gaugeElemBase) NewAggregation(_ Options, aggOpts raggregation.Options) gaugeAggregation {
	return newGaugeAggregation(raggregation.NewGauge(aggOpts))
}

func (e *gaugeElemBase) ResetSetData(
	_ maggregation.TypesOptions,
	aggTypes maggregation.Types,
	_ bool,
) error {
	if !aggTypes.IsValidForGauge() {
		return fmt.Errorf("invalid aggregation types %s for gauge", aggTypes.String())
	}
	return nil
}

func (e *gaugeElemBase) Close() {}

// nolint: maligned
type parsedPipeline struct {
	// Whether the source pipeline contains derivative transformations at its head.
	HasDerivativeTransform bool

	// Transformation operations from the head of the source pipeline this
	// parsed pipeline was derived from.
	Transformations []transformation.Op

	// Whether the source pipeline contains a rollup operation that is either at the
	// head of the source pipeline or immediately following the transformation operations
	// at the head of the source pipeline if any.
	HasRollup bool

	// Rollup operation that is either at the head of the source pipeline or
	// immediately following the transformation operations at the head of the
	// source pipeline if applicable.
	Rollup applied.RollupOp

	// The remainder of the source pipeline after stripping the transformation
	// and rollup operations from the head of the source pipeline.
	Remainder applied.Pipeline
}

// parsePipeline parses the given pipeline and returns an error if the pipeline is invalid.
// A valid pipeline should take the form of one of the following:
// * Empty pipeline with no operations.
// * Pipeline that starts with a rollup operation.
// * Pipeline that starts with a transformation operation and contains at least one
//   rollup operation. Additionally, the transformation derivative order computed from
//   the list of transformations must be no more than the maximum transformation derivative
//   order that is supported.
func newParsedPipeline(pipeline applied.Pipeline) (parsedPipeline, error) {
	if pipeline.IsEmpty() {
		return parsedPipeline{}, nil
	}
	var (
		firstRollupOpIdx              = -1
		transformationDerivativeOrder int
		numSteps                      = pipeline.Len()
	)
	for i := 0; i < numSteps; i++ {
		pipelineOp := pipeline.At(i)
		if pipelineOp.Type != mpipeline.TransformationOpType && pipelineOp.Type != mpipeline.RollupOpType {
			err := fmt.Errorf("pipeline %v step %d has invalid operation type %v", pipeline, i, pipelineOp.Type)
			return parsedPipeline{}, err
		}
		if pipelineOp.Type == mpipeline.RollupOpType {
			if firstRollupOpIdx == -1 {
				firstRollupOpIdx = i
			}
		} else if firstRollupOpIdx == -1 {
			// We only care about the transformation operations at the head of the pipeline
			// before the first rollup operation since those are going to be processed locally.
			transformOp := pipelineOp.Transformation
			// A binary transformation is a transformation that computes first-order derivatives.
			if transformOp.Type.IsBinaryTransform() {
				transformationDerivativeOrder++
			}
		}
	}

	// Pipelines that compute higher order derivatives require keeping more states including
	// the raw values and lower order derivatives. For example, a pipline such as `aggregate Last |
	// perSecond | perSecond` requires storing both the raw value and the first-order derivatives.
	// The maximum supported transformation derivative order determines the maximum number of
	// states we keep track of per value, which as a result limits the highest order of derivatives
	// we can compute from transformations.
	if transformationDerivativeOrder > maxSupportedTransformationDerivativeOrder {
		return parsedPipeline{}, fmt.Errorf("pipeline %v transformation derivative order is %d higher than supported %d", pipeline, transformationDerivativeOrder, maxSupportedTransformationDerivativeOrder)
	}

	var (
		hasRollup              = firstRollupOpIdx != -1
		hasDerivativeTransform = transformationDerivativeOrder > 0
		transformPipeline      applied.Pipeline
		remainder              applied.Pipeline
		rollup                 applied.RollupOp
	)
	if hasRollup {
		transformPipeline = pipeline.SubPipeline(0, firstRollupOpIdx)
		remainder = pipeline.SubPipeline(firstRollupOpIdx+1, numSteps)
		rollup = pipeline.At(firstRollupOpIdx).Rollup
	} else {
		transformPipeline = pipeline
	}

	transformations := make([]transformation.Op, 0, transformPipeline.Len())
	for i := 0; i < transformPipeline.Len(); i++ {
		op, err := transformPipeline.At(i).Transformation.Type.NewOp()
		if err != nil {
			err := fmt.Errorf("transform could not construct op: %v", err)
			return parsedPipeline{}, err
		}
		transformations = append(transformations, op)
	}

	return parsedPipeline{
		HasDerivativeTransform: hasDerivativeTransform,
		HasRollup:              hasRollup,
		Transformations:        transformations,
		Remainder:              remainder,
		Rollup:                 rollup,
	}, nil
}
