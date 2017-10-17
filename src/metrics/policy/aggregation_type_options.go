// Copyright (c) 2017 Uber Technologies, Inc.
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

package policy

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3x/pool"
)

// QuantileTypeStringFn returns the byte-slice type string for a quantile value.
type QuantileTypeStringFn func(quantile float64) []byte

// TypeStringTransformFn transforms the type string.
type TypeStringTransformFn func(typeString []byte) []byte

// AggregationTypesOptions provides a set of options for aggregation types.
type AggregationTypesOptions interface {
	// Validate checks if the options are valid.
	Validate() error

	// Read-Write methods.

	// SetDefaultCounterAggregationTypes sets the default aggregation types for counters.
	SetDefaultCounterAggregationTypes(value AggregationTypes) AggregationTypesOptions

	// DefaultCounterAggregationTypes returns the default aggregation types for counters.
	DefaultCounterAggregationTypes() AggregationTypes

	// SetDefaultTimerAggregationTypes sets the default aggregation types for timers.
	SetDefaultTimerAggregationTypes(value AggregationTypes) AggregationTypesOptions

	// DefaultTimerAggregationTypes returns the default aggregation types for timers.
	DefaultTimerAggregationTypes() AggregationTypes

	// SetDefaultGaugeAggregationTypes sets the default aggregation types for gauges.
	SetDefaultGaugeAggregationTypes(value AggregationTypes) AggregationTypesOptions

	// DefaultGaugeAggregationTypes returns the default aggregation types for gauges.
	DefaultGaugeAggregationTypes() AggregationTypes

	// SetTimerQuantileTypeStringFn sets the quantile type string function for timers.
	SetTimerQuantileTypeStringFn(value QuantileTypeStringFn) AggregationTypesOptions

	// TimerQuantileTypeStringFn returns the quantile type string function for timers.
	TimerQuantileTypeStringFn() QuantileTypeStringFn

	// SetGlobalTypeStringTransformFn sets the type string transform functions.
	// The GlobalTypeStringTransformFn will only be applied to the global type strings, it
	// will NOT be applied to the metric type specific overrides.
	SetGlobalTypeStringTransformFn(value TypeStringTransformFn) AggregationTypesOptions

	// GlobalTypeStringTransformFn returns the global type string transform functions.
	GlobalTypeStringTransformFn() TypeStringTransformFn

	// SetAggregationTypesPool sets the aggregation types pool.
	SetAggregationTypesPool(pool AggregationTypesPool) AggregationTypesOptions

	// AggregationTypesPool returns the aggregation types pool.
	AggregationTypesPool() AggregationTypesPool

	// SetQuantilesPool sets the timer quantiles pool.
	SetQuantilesPool(pool pool.FloatsPool) AggregationTypesOptions

	// QuantilesPool returns the timer quantiles pool.
	QuantilesPool() pool.FloatsPool

	/// Write-only options.

	// SetGlobalTypeStringOverrides sets the global type strings.
	// The GlobalTypeStringTransformFn will be applied to these type strings.
	SetGlobalTypeStringOverrides(m map[AggregationType][]byte) AggregationTypesOptions

	// SetCounterTypeStringOverrides sets the overrides for counter type strings.
	SetCounterTypeStringOverrides(m map[AggregationType][]byte) AggregationTypesOptions

	// SetTimerTypeStringOverrides sets the overrides for timer type strings.
	SetTimerTypeStringOverrides(m map[AggregationType][]byte) AggregationTypesOptions

	// SetGaugeTypeStringOverrides sets the overrides for gauge type strings.
	SetGaugeTypeStringOverrides(m map[AggregationType][]byte) AggregationTypesOptions

	// Read only methods.

	// DefaultCounterAggregationTypeStrings returns the type string for
	// default counter aggregation types.
	DefaultCounterAggregationTypeStrings() [][]byte

	// DefaultTimerAggregationTypeStrings returns the type string for
	// default timer aggregation types.
	DefaultTimerAggregationTypeStrings() [][]byte

	// DefaultGaugeAggregationTypeStrings returns the type string for
	// default gauge aggregation types.
	DefaultGaugeAggregationTypeStrings() [][]byte

	// TypeString returns the type string for the aggregation type for counters.
	TypeStringForCounter(value AggregationType) []byte

	// TypeString returns the type string for the aggregation type for timers.
	TypeStringForTimer(value AggregationType) []byte

	// TypeString returns the type string for the aggregation type for gauges.
	TypeStringForGauge(value AggregationType) []byte

	// AggregationTypeForCounter returns the aggregation type with the given type string for counters.
	AggregationTypeForCounter(value []byte) AggregationType

	// AggregationTypeForTimer returns the aggregation type with the given type string for timers.
	AggregationTypeForTimer(value []byte) AggregationType

	// AggregationTypeForGauge returns the aggregation type with the given type string for gauges.
	AggregationTypeForGauge(value []byte) AggregationType

	// TimerQuantiles returns the quantiles for timers.
	TimerQuantiles() []float64

	// IsContainedInDefaultAggregationTypes checks if the given aggregation type is
	// contained in the default aggregation types for the metric type.
	IsContainedInDefaultAggregationTypes(at AggregationType, mt metric.Type) bool
}

var (
	defaultDefaultCounterAggregationTypes = AggregationTypes{
		Sum,
	}
	defaultDefaultTimerAggregationTypes = AggregationTypes{
		Sum,
		SumSq,
		Mean,
		Min,
		Max,
		Count,
		Stdev,
		Median,
		P50,
		P95,
		P99,
	}
	defaultDefaultGaugeAggregationTypes = AggregationTypes{
		Last,
	}

	defaultUnknownTypeString = []byte("unknown")
	defaultLastTypeString    = []byte("last")
	defaultSumTypeString     = []byte("sum")
	defaultSumSqTypeString   = []byte("sum_sq")
	defaultMeanTypeString    = []byte("mean")
	defaultMinTypeString     = []byte("lower")
	defaultMaxTypeString     = []byte("upper")
	defaultCountTypeString   = []byte("count")
	defaultStdevTypeString   = []byte("stdev")
	defaultMedianTypeString  = []byte("median")

	defaultCounterTypeStringOverride = map[AggregationType][]byte{
		Sum: nil,
	}
	defaultTimerTypeStringOverride = map[AggregationType][]byte{}
	defaultGaugeTypeStringOverride = map[AggregationType][]byte{
		Last: nil,
	}
)

type options struct {
	defaultCounterAggregationTypes AggregationTypes
	defaultTimerAggregationTypes   AggregationTypes
	defaultGaugeAggregationTypes   AggregationTypes
	timerQuantileTypeStringFn      QuantileTypeStringFn
	globalTypeStringTransformFn    TypeStringTransformFn
	aggTypesPool                   AggregationTypesPool
	quantilesPool                  pool.FloatsPool

	defaultTypeStrings [][]byte
	counterTypeStrings [][]byte
	timerTypeStrings   [][]byte
	gaugeTypeStrings   [][]byte

	globalTypeStringOverrides map[AggregationType][]byte
	counterTypeStringOverride map[AggregationType][]byte
	timerTypeStringOverride   map[AggregationType][]byte
	gaugeTypeStringOverride   map[AggregationType][]byte

	defaultCounterAggregationTypeStrings [][]byte
	defaultTimerAggregationTypeStrings   [][]byte
	defaultGaugeAggregationTypeStrings   [][]byte
	timerQuantiles                       []float64
}

// NewAggregationTypesOptions returns a default AggregationTypesOptions.
func NewAggregationTypesOptions() AggregationTypesOptions {
	o := &options{
		defaultCounterAggregationTypes: defaultDefaultCounterAggregationTypes,
		defaultGaugeAggregationTypes:   defaultDefaultGaugeAggregationTypes,
		defaultTimerAggregationTypes:   defaultDefaultTimerAggregationTypes,
		timerQuantileTypeStringFn:      defaultTimerQuantileTypeStringFn,
		globalTypeStringTransformFn:    noopTransformFn,
		counterTypeStringOverride:      defaultCounterTypeStringOverride,
		timerTypeStringOverride:        defaultTimerTypeStringOverride,
		gaugeTypeStringOverride:        defaultGaugeTypeStringOverride,
	}
	o.initPools()
	o.computeAllDerived()
	return o
}

func (o *options) initPools() {
	o.aggTypesPool = NewAggregationTypesPool(nil)
	o.aggTypesPool.Init(func() AggregationTypes {
		return make(AggregationTypes, 0, len(ValidAggregationTypes))
	})

	o.quantilesPool = pool.NewFloatsPool(nil, nil)
	o.quantilesPool.Init()
}

func (o *options) Validate() error {
	if err := o.ensureUniqueTypeString(o.counterTypeStrings, metric.CounterType); err != nil {
		return err
	}
	if err := o.ensureUniqueTypeString(o.timerTypeStrings, metric.TimerType); err != nil {
		return err
	}
	return o.ensureUniqueTypeString(o.gaugeTypeStrings, metric.GaugeType)
}

func (o *options) ensureUniqueTypeString(typeStrings [][]byte, t metric.Type) error {
	m := make(map[string]int, len(typeStrings))
	for aggType, typeString := range typeStrings {
		s := string(typeString)
		if existAggType, ok := m[s]; ok {
			return fmt.Errorf("invalid options, found duplicated type string: '%s' for aggregation type %v and %v for metric type: %s",
				s, AggregationType(aggType), AggregationType(existAggType), t.String())
		}
		m[s] = aggType
	}
	return nil
}

func (o *options) SetDefaultCounterAggregationTypes(aggTypes AggregationTypes) AggregationTypesOptions {
	opts := *o
	opts.defaultCounterAggregationTypes = aggTypes
	opts.computeTypeStrings()
	return &opts
}

func (o *options) DefaultCounterAggregationTypes() AggregationTypes {
	return o.defaultCounterAggregationTypes
}

func (o *options) SetDefaultTimerAggregationTypes(aggTypes AggregationTypes) AggregationTypesOptions {
	opts := *o
	opts.defaultTimerAggregationTypes = aggTypes
	opts.computeQuantiles()
	opts.computeTypeStrings()
	return &opts
}

func (o *options) DefaultTimerAggregationTypes() AggregationTypes {
	return o.defaultTimerAggregationTypes
}

func (o *options) SetDefaultGaugeAggregationTypes(aggTypes AggregationTypes) AggregationTypesOptions {
	opts := *o
	opts.defaultGaugeAggregationTypes = aggTypes
	opts.computeTypeStrings()
	return &opts
}

func (o *options) DefaultGaugeAggregationTypes() AggregationTypes {
	return o.defaultGaugeAggregationTypes
}

func (o *options) SetTimerQuantileTypeStringFn(value QuantileTypeStringFn) AggregationTypesOptions {
	opts := *o
	opts.timerQuantileTypeStringFn = value
	opts.computeTypeStrings()
	return &opts
}

func (o *options) TimerQuantileTypeStringFn() QuantileTypeStringFn {
	return o.timerQuantileTypeStringFn
}

func (o *options) SetGlobalTypeStringTransformFn(value TypeStringTransformFn) AggregationTypesOptions {
	opts := *o
	opts.globalTypeStringTransformFn = value
	opts.computeTypeStrings()
	return &opts
}

func (o *options) GlobalTypeStringTransformFn() TypeStringTransformFn {
	return o.globalTypeStringTransformFn
}

func (o *options) SetAggregationTypesPool(pool AggregationTypesPool) AggregationTypesOptions {
	opts := *o
	opts.aggTypesPool = pool
	return &opts
}

func (o *options) AggregationTypesPool() AggregationTypesPool {
	return o.aggTypesPool
}

func (o *options) SetQuantilesPool(pool pool.FloatsPool) AggregationTypesOptions {
	opts := *o
	opts.quantilesPool = pool
	return &opts
}

func (o *options) QuantilesPool() pool.FloatsPool {
	return o.quantilesPool
}

func (o *options) SetGlobalTypeStringOverrides(m map[AggregationType][]byte) AggregationTypesOptions {
	opts := *o
	opts.globalTypeStringOverrides = m
	opts.computeTypeStrings()
	return &opts
}

func (o *options) SetCounterTypeStringOverrides(m map[AggregationType][]byte) AggregationTypesOptions {
	opts := *o
	opts.counterTypeStringOverride = m
	opts.computeTypeStrings()
	return &opts
}

func (o *options) SetTimerTypeStringOverrides(m map[AggregationType][]byte) AggregationTypesOptions {
	opts := *o
	opts.timerTypeStringOverride = m
	opts.computeTypeStrings()
	return &opts
}

func (o *options) SetGaugeTypeStringOverrides(m map[AggregationType][]byte) AggregationTypesOptions {
	opts := *o
	opts.gaugeTypeStringOverride = m
	opts.computeTypeStrings()
	return &opts
}

func (o *options) DefaultCounterAggregationTypeStrings() [][]byte {
	return o.defaultCounterAggregationTypeStrings
}

func (o *options) DefaultTimerAggregationTypeStrings() [][]byte {
	return o.defaultTimerAggregationTypeStrings
}

func (o *options) DefaultGaugeAggregationTypeStrings() [][]byte {
	return o.defaultGaugeAggregationTypeStrings
}

func (o *options) TypeStringForCounter(aggType AggregationType) []byte {
	return o.counterTypeStrings[aggType.ID()]
}

func (o *options) TypeStringForTimer(aggType AggregationType) []byte {
	return o.timerTypeStrings[aggType.ID()]
}

func (o *options) TypeStringForGauge(aggType AggregationType) []byte {
	return o.gaugeTypeStrings[aggType.ID()]
}

func (o *options) AggregationTypeForCounter(value []byte) AggregationType {
	return aggregationTypeWithTypeString(value, o.counterTypeStrings)
}

func (o *options) AggregationTypeForTimer(value []byte) AggregationType {
	return aggregationTypeWithTypeString(value, o.timerTypeStrings)
}

func (o *options) AggregationTypeForGauge(value []byte) AggregationType {
	return aggregationTypeWithTypeString(value, o.gaugeTypeStrings)
}

func (o *options) TimerQuantiles() []float64 {
	return o.timerQuantiles
}

func (o *options) IsContainedInDefaultAggregationTypes(at AggregationType, mt metric.Type) bool {
	var aggTypes AggregationTypes
	switch mt {
	case metric.CounterType:
		aggTypes = o.DefaultCounterAggregationTypes()
	case metric.GaugeType:
		aggTypes = o.DefaultGaugeAggregationTypes()
	case metric.TimerType:
		aggTypes = o.DefaultTimerAggregationTypes()
	}

	return aggTypes.Contains(at)
}

func aggregationTypeWithTypeString(value []byte, typeStrings [][]byte) AggregationType {
	for aggType, b := range typeStrings {
		if bytes.Equal(b, value) {
			return AggregationType(aggType)
		}
	}
	return UnknownAggregationType
}

func (o *options) computeAllDerived() {
	o.computeQuantiles()
	o.computeTypeStrings()
}

func (o *options) computeQuantiles() {
	o.timerQuantiles, _ = o.DefaultTimerAggregationTypes().PooledQuantiles(o.QuantilesPool())
}

func (o *options) computeTypeStrings() {
	// NB(cw) The order matters.
	o.computeDefaultTypeStrings()
	o.computeCounterTypeStrings()
	o.computeTimerTypeStrings()
	o.computeGaugeTypeStrings()
	o.computeDefaultCounterAggregationTypeString()
	o.computeDefaultTimerAggregationTypeString()
	o.computeDefaultGaugeAggregationTypeString()
}

func (o *options) computeDefaultTypeStrings() {
	o.defaultTypeStrings = make([][]byte, MaxAggregationTypeID+1)
	o.defaultTypeStrings[UnknownAggregationType.ID()] = defaultUnknownTypeString
	transformFn := o.GlobalTypeStringTransformFn()
	for aggType := range ValidAggregationTypes {
		var typeString []byte
		switch aggType {
		case Last:
			typeString = defaultLastTypeString
		case Min:
			typeString = defaultMinTypeString
		case Max:
			typeString = defaultMaxTypeString
		case Mean:
			typeString = defaultMeanTypeString
		case Median:
			typeString = defaultMedianTypeString
		case Count:
			typeString = defaultCountTypeString
		case Sum:
			typeString = defaultSumTypeString
		case SumSq:
			typeString = defaultSumSqTypeString
		case Stdev:
			typeString = defaultStdevTypeString
		default:
			q, ok := aggType.Quantile()
			if ok {
				typeString = o.timerQuantileTypeStringFn(q)
			}
		}
		override, ok := o.globalTypeStringOverrides[aggType]
		if ok {
			typeString = override
		}
		o.defaultTypeStrings[aggType.ID()] = transformFn(typeString)
	}
}

func (o *options) computeCounterTypeStrings() {
	o.counterTypeStrings = o.computeOverrideTypeStrings(o.counterTypeStringOverride)
}

func (o *options) computeTimerTypeStrings() {
	o.timerTypeStrings = o.computeOverrideTypeStrings(o.timerTypeStringOverride)
}

func (o *options) computeGaugeTypeStrings() {
	o.gaugeTypeStrings = o.computeOverrideTypeStrings(o.gaugeTypeStringOverride)
}

func (o options) computeOverrideTypeStrings(m map[AggregationType][]byte) [][]byte {
	res := make([][]byte, len(o.defaultTypeStrings))
	for aggType, defaultTypeString := range o.defaultTypeStrings {
		if overrideTypeString, ok := m[AggregationType(aggType)]; ok {
			res[aggType] = overrideTypeString
			continue
		}
		res[aggType] = defaultTypeString
	}
	return res
}

func (o *options) computeDefaultCounterAggregationTypeString() {
	o.defaultCounterAggregationTypeStrings = make([][]byte, len(o.DefaultCounterAggregationTypes()))
	for i, aggType := range o.DefaultCounterAggregationTypes() {
		o.defaultCounterAggregationTypeStrings[i] = o.TypeStringForCounter(aggType)
	}
}

func (o *options) computeDefaultTimerAggregationTypeString() {
	o.defaultTimerAggregationTypeStrings = make([][]byte, len(o.DefaultTimerAggregationTypes()))
	for i, aggType := range o.DefaultTimerAggregationTypes() {
		o.defaultTimerAggregationTypeStrings[i] = o.TypeStringForTimer(aggType)
	}
}

func (o *options) computeDefaultGaugeAggregationTypeString() {
	o.defaultGaugeAggregationTypeStrings = make([][]byte, len(o.DefaultGaugeAggregationTypes()))
	for i, aggType := range o.DefaultGaugeAggregationTypes() {
		o.defaultGaugeAggregationTypeStrings[i] = o.TypeStringForGauge(aggType)
	}
}

// By default we use e.g. "p50", "p95", "p99" for the 50th/95th/99th percentile.
func defaultTimerQuantileTypeStringFn(quantile float64) []byte {
	str := strconv.FormatFloat(quantile*100, 'f', -1, 64)
	idx := strings.Index(str, ".")
	if idx != -1 {
		str = str[:idx] + str[idx+1:]
	}
	return []byte("p" + str)
}

func noopTransformFn(b []byte) []byte   { return b }
func suffixTransformFn(b []byte) []byte { return append([]byte("."), b...) }
