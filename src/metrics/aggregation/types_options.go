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

package aggregation

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

// TypesOptions provides a set of options for aggregation types.
type TypesOptions interface {
	// Validate checks if the options are valid.
	Validate() error

	// Read-Write methods.

	// SetDefaultCounterAggregationTypes sets the default aggregation types for counters.
	SetDefaultCounterAggregationTypes(value Types) TypesOptions

	// DefaultCounterAggregationTypes returns the default aggregation types for counters.
	DefaultCounterAggregationTypes() Types

	// SetDefaultTimerAggregationTypes sets the default aggregation types for timers.
	SetDefaultTimerAggregationTypes(value Types) TypesOptions

	// DefaultTimerAggregationTypes returns the default aggregation types for timers.
	DefaultTimerAggregationTypes() Types

	// SetDefaultGaugeAggregationTypes sets the default aggregation types for gauges.
	SetDefaultGaugeAggregationTypes(value Types) TypesOptions

	// DefaultGaugeAggregationTypes returns the default aggregation types for gauges.
	DefaultGaugeAggregationTypes() Types

	// SetTimerQuantileTypeStringFn sets the quantile type string function for timers.
	SetTimerQuantileTypeStringFn(value QuantileTypeStringFn) TypesOptions

	// TimerQuantileTypeStringFn returns the quantile type string function for timers.
	TimerQuantileTypeStringFn() QuantileTypeStringFn

	// SetGlobalTypeStringTransformFn sets the type string transform functions.
	// The GlobalTypeStringTransformFn will only be applied to the global type strings, it
	// will NOT be applied to the metric type specific overrides.
	SetGlobalTypeStringTransformFn(value TypeStringTransformFn) TypesOptions

	// GlobalTypeStringTransformFn returns the global type string transform functions.
	GlobalTypeStringTransformFn() TypeStringTransformFn

	// SetTypesPool sets the aggregation types pool.
	SetTypesPool(pool TypesPool) TypesOptions

	// TypesPool returns the aggregation types pool.
	TypesPool() TypesPool

	// SetQuantilesPool sets the timer quantiles pool.
	SetQuantilesPool(pool pool.FloatsPool) TypesOptions

	// QuantilesPool returns the timer quantiles pool.
	QuantilesPool() pool.FloatsPool

	/// Write-only options.

	// SetGlobalTypeStringOverrides sets the global type strings.
	// The GlobalTypeStringTransformFn will be applied to these type strings.
	SetGlobalTypeStringOverrides(m map[Type][]byte) TypesOptions

	// SetCounterTypeStringOverrides sets the overrides for counter type strings.
	SetCounterTypeStringOverrides(m map[Type][]byte) TypesOptions

	// SetTimerTypeStringOverrides sets the overrides for timer type strings.
	SetTimerTypeStringOverrides(m map[Type][]byte) TypesOptions

	// SetGaugeTypeStringOverrides sets the overrides for gauge type strings.
	SetGaugeTypeStringOverrides(m map[Type][]byte) TypesOptions

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

	// TypeStringForCounter returns the type string for the aggregation type for counters.
	TypeStringForCounter(value Type) []byte

	// TypeStringForTimer returns the type string for the aggregation type for timers.
	TypeStringForTimer(value Type) []byte

	// TypeStringForGauge returns the type string for the aggregation type for gauges.
	TypeStringForGauge(value Type) []byte

	// TypeForCounter returns the aggregation type with the given type string for counters.
	TypeForCounter(value []byte) Type

	// TypeForTimer returns the aggregation type with the given type string for timers.
	TypeForTimer(value []byte) Type

	// TypeForGauge returns the aggregation type with the given type string for gauges.
	TypeForGauge(value []byte) Type

	// TimerQuantiles returns the quantiles for timers.
	TimerQuantiles() []float64

	// IsContainedInDefaultAggregationTypes checks if the given aggregation type is
	// contained in the default aggregation types for the metric type.
	IsContainedInDefaultAggregationTypes(at Type, mt metric.Type) bool
}

var (
	defaultDefaultCounterAggregationTypes = Types{
		Sum,
	}
	defaultDefaultTimerAggregationTypes = Types{
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
	defaultDefaultGaugeAggregationTypes = Types{
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

	defaultCounterAggregationTypeStringOverride = map[Type][]byte{
		Sum: nil,
	}
	defaultTimerTypeStringOveAggregationTypes = map[Type][]byte{}
	defaultGaugeAggregationTypestringOverride = map[Type][]byte{
		Last: nil,
	}
)

type options struct {
	defaultCounterAggregationTypes Types
	defaultTimerAggregationTypes   Types
	defaultGaugeAggregationTypes   Types
	timerQuantileTypeStringFn      QuantileTypeStringFn
	globalTypeStringTransformFn    TypeStringTransformFn
	aggTypesPool                   TypesPool
	quantilesPool                  pool.FloatsPool

	defaultTypeStrings [][]byte
	counterTypeStrings [][]byte
	timerTypeStrings   [][]byte
	gaugeTypeStrings   [][]byte

	globalTypeStringOverrides map[Type][]byte
	counterTypeStringOverride map[Type][]byte
	timerTypeStringOverride   map[Type][]byte
	gaugeTypeStringOverride   map[Type][]byte

	defaultCounterAggregationTypeStrings [][]byte
	defaultTimerAggregationTypeStrings   [][]byte
	defaultGaugeAggregationTypestrings   [][]byte
	timerQuantiles                       []float64
}

// NewTypesOptions returns a default TypesOptions.
func NewTypesOptions() TypesOptions {
	o := &options{
		defaultCounterAggregationTypes: defaultDefaultCounterAggregationTypes,
		defaultGaugeAggregationTypes:   defaultDefaultGaugeAggregationTypes,
		defaultTimerAggregationTypes:   defaultDefaultTimerAggregationTypes,
		timerQuantileTypeStringFn:      defaultTimerQuantileTypeStringFn,
		globalTypeStringTransformFn:    noopTransformFn,
		counterTypeStringOverride:      defaultCounterAggregationTypeStringOverride,
		timerTypeStringOverride:        defaultTimerTypeStringOveAggregationTypes,
		gaugeTypeStringOverride:        defaultGaugeAggregationTypestringOverride,
	}
	o.initPools()
	o.computeAllDerived()
	return o
}

func (o *options) initPools() {
	o.aggTypesPool = NewTypesPool(nil)
	o.aggTypesPool.Init(func() Types {
		return make(Types, 0, len(ValidTypes))
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
				s, Type(aggType), Type(existAggType), t.String())
		}
		m[s] = aggType
	}
	return nil
}

func (o *options) SetDefaultCounterAggregationTypes(aggTypes Types) TypesOptions {
	opts := *o
	opts.defaultCounterAggregationTypes = aggTypes
	opts.computeTypeStrings()
	return &opts
}

func (o *options) DefaultCounterAggregationTypes() Types {
	return o.defaultCounterAggregationTypes
}

func (o *options) SetDefaultTimerAggregationTypes(aggTypes Types) TypesOptions {
	opts := *o
	opts.defaultTimerAggregationTypes = aggTypes
	opts.computeQuantiles()
	opts.computeTypeStrings()
	return &opts
}

func (o *options) DefaultTimerAggregationTypes() Types {
	return o.defaultTimerAggregationTypes
}

func (o *options) SetDefaultGaugeAggregationTypes(aggTypes Types) TypesOptions {
	opts := *o
	opts.defaultGaugeAggregationTypes = aggTypes
	opts.computeTypeStrings()
	return &opts
}

func (o *options) DefaultGaugeAggregationTypes() Types {
	return o.defaultGaugeAggregationTypes
}

func (o *options) SetTimerQuantileTypeStringFn(value QuantileTypeStringFn) TypesOptions {
	opts := *o
	opts.timerQuantileTypeStringFn = value
	opts.computeTypeStrings()
	return &opts
}

func (o *options) TimerQuantileTypeStringFn() QuantileTypeStringFn {
	return o.timerQuantileTypeStringFn
}

func (o *options) SetGlobalTypeStringTransformFn(value TypeStringTransformFn) TypesOptions {
	opts := *o
	opts.globalTypeStringTransformFn = value
	opts.computeTypeStrings()
	return &opts
}

func (o *options) GlobalTypeStringTransformFn() TypeStringTransformFn {
	return o.globalTypeStringTransformFn
}

func (o *options) SetTypesPool(pool TypesPool) TypesOptions {
	opts := *o
	opts.aggTypesPool = pool
	return &opts
}

func (o *options) TypesPool() TypesPool {
	return o.aggTypesPool
}

func (o *options) SetQuantilesPool(pool pool.FloatsPool) TypesOptions {
	opts := *o
	opts.quantilesPool = pool
	return &opts
}

func (o *options) QuantilesPool() pool.FloatsPool {
	return o.quantilesPool
}

func (o *options) SetGlobalTypeStringOverrides(m map[Type][]byte) TypesOptions {
	opts := *o
	opts.globalTypeStringOverrides = m
	opts.computeTypeStrings()
	return &opts
}

func (o *options) SetCounterTypeStringOverrides(m map[Type][]byte) TypesOptions {
	opts := *o
	opts.counterTypeStringOverride = m
	opts.computeTypeStrings()
	return &opts
}

func (o *options) SetTimerTypeStringOverrides(m map[Type][]byte) TypesOptions {
	opts := *o
	opts.timerTypeStringOverride = m
	opts.computeTypeStrings()
	return &opts
}

func (o *options) SetGaugeTypeStringOverrides(m map[Type][]byte) TypesOptions {
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
	return o.defaultGaugeAggregationTypestrings
}

func (o *options) TypeStringForCounter(aggType Type) []byte {
	return o.counterTypeStrings[aggType.ID()]
}

func (o *options) TypeStringForTimer(aggType Type) []byte {
	return o.timerTypeStrings[aggType.ID()]
}

func (o *options) TypeStringForGauge(aggType Type) []byte {
	return o.gaugeTypeStrings[aggType.ID()]
}

func (o *options) TypeForCounter(value []byte) Type {
	return aggregationTypeWithTypeString(value, o.counterTypeStrings)
}

func (o *options) TypeForTimer(value []byte) Type {
	return aggregationTypeWithTypeString(value, o.timerTypeStrings)
}

func (o *options) TypeForGauge(value []byte) Type {
	return aggregationTypeWithTypeString(value, o.gaugeTypeStrings)
}

func (o *options) TimerQuantiles() []float64 {
	return o.timerQuantiles
}

func (o *options) IsContainedInDefaultAggregationTypes(at Type, mt metric.Type) bool {
	var aggTypes Types
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

func aggregationTypeWithTypeString(value []byte, typeStrings [][]byte) Type {
	for aggType, b := range typeStrings {
		if bytes.Equal(b, value) {
			return Type(aggType)
		}
	}
	return UnknownType
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
	o.computeDefaultTimerTypeSAggregationTypes()
	o.computeDefaultGaugeAggregationTypestring()
}

func (o *options) computeDefaultTypeStrings() {
	o.defaultTypeStrings = make([][]byte, maxTypeID+1)
	o.defaultTypeStrings[UnknownType.ID()] = defaultUnknownTypeString
	transformFn := o.GlobalTypeStringTransformFn()
	for aggType := range ValidTypes {
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

func (o options) computeOverrideTypeStrings(m map[Type][]byte) [][]byte {
	res := make([][]byte, len(o.defaultTypeStrings))
	for aggType, defaultTypeString := range o.defaultTypeStrings {
		if overrideTypeString, ok := m[Type(aggType)]; ok {
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

func (o *options) computeDefaultTimerTypeSAggregationTypes() {
	o.defaultTimerAggregationTypeStrings = make([][]byte, len(o.DefaultTimerAggregationTypes()))
	for i, aggType := range o.DefaultTimerAggregationTypes() {
		o.defaultTimerAggregationTypeStrings[i] = o.TypeStringForTimer(aggType)
	}
}

func (o *options) computeDefaultGaugeAggregationTypestring() {
	o.defaultGaugeAggregationTypestrings = make([][]byte, len(o.DefaultGaugeAggregationTypes()))
	for i, aggType := range o.DefaultGaugeAggregationTypes() {
		o.defaultGaugeAggregationTypestrings[i] = o.TypeStringForGauge(aggType)
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
