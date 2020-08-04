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

	raggregation "github.com/m3db/m3/src/aggregator/aggregation"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	mpipeline "github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/x/pool"
	"go.uber.org/zap"

	"github.com/willf/bitset"
)

const (
	// Default number of aggregation buckets allocated initially.
	defaultNumAggregations = 2

	// Default initial number of sources.
	defaultNumSources = 1024

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
	// Type returns the metric type.
	Type() metric.Type

	// ID returns the metric id.
	ID() id.RawID

	// ForwardedID returns the id of the forwarded metric if applicable.
	ForwardedID() (id.RawID, bool)

	// ForwardedAggregationKey returns the forwarded aggregation key if applicable.
	ForwardedAggregationKey() (aggregationKey, bool)

	// ResetSetData resets the element and sets data.
	ResetSetData(
		id id.RawID,
		sp policy.StoragePolicy,
		aggTypes maggregation.Types,
		pipeline applied.Pipeline,
		numForwardedTimes int,
		idPrefixSuffixType IDPrefixSuffixType,
	) error

	// SetForwardedCallbacks sets the callback functions to write forwarded
	// metrics for elements producing such forwarded metrics.
	SetForwardedCallbacks(
		writeFn writeForwardedMetricFn,
		onDoneFn onForwardedAggregationDoneFn,
	)

	// AddUnion adds a metric value union at a given timestamp.
	AddUnion(timestamp time.Time, mu unaggregated.MetricUnion) error

	// AddMetric adds a metric value at a given timestamp.
	AddValue(timestamp time.Time, value float64) error

	// AddUnique adds a metric value from a given source at a given timestamp.
	// If previous values from the same source have already been added to the
	// same aggregation, the incoming value is discarded.
	AddUnique(timestamp time.Time, values []float64, sourceID uint32) error

	// Consume consumes values before a given time and removes
	// them from the element after they are consumed, returning whether
	// the element can be collected after the consumption is completed.
	Consume(
		targetNanos int64,
		isEarlierThanFn isEarlierThanFn,
		timestampNanosFn timestampNanosFn,
		flushLocalFn flushLocalMetricFn,
		flushForwardedFn flushForwardedMetricFn,
		onForwardedFlushedFn onForwardingElemFlushedFn,
	) bool

	// MarkAsTombstoned marks an element as tombstoned, which means this element
	// will be deleted once its aggregated values have been flushed.
	MarkAsTombstoned()

	// Close closes the element.
	Close()
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

	// Mutable states.
	tombstoned           bool
	closed               bool
	cachedSourceSetsLock sync.Mutex       // nolint: structcheck
	cachedSourceSets     []*bitset.BitSet // nolint: structcheck
}

func newElemBase(opts Options) elemBase {
	return elemBase{
		opts:         opts,
		aggTypesOpts: opts.AggregationTypesOptions(),
		aggOpts:      raggregation.NewOptions(opts.InstrumentOptions()),
	}
}

// resetSetData resets the element base and sets data.
func (e *elemBase) resetSetData(
	id id.RawID,
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
	useDefaultAggregation bool,
	pipeline applied.Pipeline,
	numForwardedTimes int,
	idPrefixSuffixType IDPrefixSuffixType,
) error {
	parsed, err := newParsedPipeline(pipeline)
	if err != nil {
		l := e.opts.InstrumentOptions().Logger()
		l.Error("error parsing pipeline", zap.Error(err))
		return err
	}
	e.id = id
	e.sp = sp
	e.aggTypes = aggTypes
	e.useDefaultAggregation = useDefaultAggregation
	e.aggOpts.ResetSetData(aggTypes)
	e.parsedPipeline = parsed
	e.numForwardedTimes = numForwardedTimes
	e.tombstoned = false
	e.closed = false
	e.idPrefixSuffixType = idPrefixSuffixType
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
