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
	"strconv"
	"strings"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3x/pool"
)

// QuantileSuffixFn returns the byte-slice suffix for a quantile value
type QuantileSuffixFn func(quantile float64) []byte

// AggregationTypesOptions provides a set of options for aggregation types.
type AggregationTypesOptions interface {
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

	// SetLastSuffix sets the suffix for aggregation type last.
	SetLastSuffix(value []byte) AggregationTypesOptions

	// LastSuffix returns the suffix for aggregation type last.
	LastSuffix() []byte

	// SetMinSuffix sets the suffix for aggregation type min.
	SetMinSuffix(value []byte) AggregationTypesOptions

	// MinSuffix returns the suffix for aggregation type min.
	MinSuffix() []byte

	// SetMaxSuffix sets the suffix for aggregation type max.
	SetMaxSuffix(value []byte) AggregationTypesOptions

	// MaxSuffix returns the suffix for aggregation type max.
	MaxSuffix() []byte

	// SetMeanSuffix sets the suffix for aggregation type mean.
	SetMeanSuffix(value []byte) AggregationTypesOptions

	// MeanSuffix returns the suffix for aggregation type mean.
	MeanSuffix() []byte

	// SetMedianSuffix sets the suffix for aggregation type median.
	SetMedianSuffix(value []byte) AggregationTypesOptions

	// MedianSuffix returns the suffix for aggregation type median.
	MedianSuffix() []byte

	// SetCountSuffix sets the suffix for aggregation type count.
	SetCountSuffix(value []byte) AggregationTypesOptions

	// CountSuffix returns the suffix for aggregation type count.
	CountSuffix() []byte

	// SetSumSuffix sets the suffix for aggregation type sum.
	SetSumSuffix(value []byte) AggregationTypesOptions

	// SumSuffix returns the suffix for aggregation type sum.
	SumSuffix() []byte

	// SetSumSqSuffix sets the suffix for aggregation type sum square.
	SetSumSqSuffix(value []byte) AggregationTypesOptions

	// SumSqSuffix returns the suffix for aggregation type sum square.
	SumSqSuffix() []byte

	// SetStdevSuffix sets the suffix for aggregation type standard deviation.
	SetStdevSuffix(value []byte) AggregationTypesOptions

	// StdevSuffix returns the suffix for aggregation type standard deviation.
	StdevSuffix() []byte

	// SetTimerQuantileSuffixFn sets the quantile suffix function for timers.
	SetTimerQuantileSuffixFn(value QuantileSuffixFn) AggregationTypesOptions

	// TimerQuantileSuffixFn returns the quantile suffix function for timers.
	TimerQuantileSuffixFn() QuantileSuffixFn

	// SetAggregationTypesPool sets the aggregation types pool.
	SetAggregationTypesPool(pool AggregationTypesPool) AggregationTypesOptions

	// AggregationTypesPool returns the aggregation types pool.
	AggregationTypesPool() AggregationTypesPool

	// SetQuantilesPool sets the timer quantiles pool.
	SetQuantilesPool(pool pool.FloatsPool) AggregationTypesOptions

	// QuantilesPool returns the timer quantiles pool.
	QuantilesPool() pool.FloatsPool

	/// Write-only options.

	// SetCounterSuffixOverrides sets the overrides for counter suffixes.
	SetCounterSuffixOverrides(m map[AggregationType][]byte) AggregationTypesOptions

	// SetTimerSuffixOverrides sets the overrides for timer suffixes.
	SetTimerSuffixOverrides(m map[AggregationType][]byte) AggregationTypesOptions

	// SetGaugeSuffixOverrides sets the overrides for gauge suffixes.
	SetGaugeSuffixOverrides(m map[AggregationType][]byte) AggregationTypesOptions

	// Read only methods.

	// DefaultCounterAggregationSuffixes returns the suffix for
	// default counter aggregation types.
	DefaultCounterAggregationSuffixes() [][]byte

	// DefaultTimerAggregationSuffixes returns the suffix for
	// default timer aggregation types.
	DefaultTimerAggregationSuffixes() [][]byte

	// DefaultGaugeAggregationSuffixes returns the suffix for
	// default gauge aggregation types.
	DefaultGaugeAggregationSuffixes() [][]byte

	// Suffix returns the suffix for the aggregation type for counters.
	SuffixForCounter(value AggregationType) []byte

	// Suffix returns the suffix for the aggregation type for timers.
	SuffixForTimer(value AggregationType) []byte

	// Suffix returns the suffix for the aggregation type for gauges.
	SuffixForGauge(value AggregationType) []byte

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

	defaultAggregationLastSuffix   = []byte(".last")
	defaultAggregationSumSuffix    = []byte(".sum")
	defaultAggregationSumSqSuffix  = []byte(".sum_sq")
	defaultAggregationMeanSuffix   = []byte(".mean")
	defaultAggregationMinSuffix    = []byte(".lower")
	defaultAggregationMaxSuffix    = []byte(".upper")
	defaultAggregationCountSuffix  = []byte(".count")
	defaultAggregationStdevSuffix  = []byte(".stdev")
	defaultAggregationMedianSuffix = []byte(".median")

	defaultCounterSuffixOverride = map[AggregationType][]byte{
		Sum: nil,
	}
	defaultTimerSuffixOverride = map[AggregationType][]byte{}
	defaultGaugeSuffixOverride = map[AggregationType][]byte{
		Last: nil,
	}
)

type options struct {
	defaultCounterAggregationTypes AggregationTypes
	defaultTimerAggregationTypes   AggregationTypes
	defaultGaugeAggregationTypes   AggregationTypes
	sumSuffix                      []byte
	sumSqSuffix                    []byte
	meanSuffix                     []byte
	lastSuffix                     []byte
	minSuffix                      []byte
	maxSuffix                      []byte
	countSuffix                    []byte
	stdevSuffix                    []byte
	medianSuffix                   []byte
	timerQuantileSuffixFn          QuantileSuffixFn
	aggTypesPool                   AggregationTypesPool
	quantilesPool                  pool.FloatsPool

	defaultSuffixes [][]byte
	counterSuffixes [][]byte
	timerSuffixes   [][]byte
	gaugeSuffixes   [][]byte

	counterSuffixOverride map[AggregationType][]byte
	timerSuffixOverride   map[AggregationType][]byte
	gaugeSuffixOverride   map[AggregationType][]byte

	defaultCounterAggregationSuffixes [][]byte
	defaultTimerAggregationSuffixes   [][]byte
	defaultGaugeAggregationSuffixes   [][]byte
	timerQuantiles                    []float64
}

// NewAggregationTypesOptions returns a default AggregationTypesOptions.
func NewAggregationTypesOptions() AggregationTypesOptions {
	o := &options{
		defaultCounterAggregationTypes: defaultDefaultCounterAggregationTypes,
		defaultGaugeAggregationTypes:   defaultDefaultGaugeAggregationTypes,
		defaultTimerAggregationTypes:   defaultDefaultTimerAggregationTypes,
		lastSuffix:                     defaultAggregationLastSuffix,
		minSuffix:                      defaultAggregationMinSuffix,
		maxSuffix:                      defaultAggregationMaxSuffix,
		meanSuffix:                     defaultAggregationMeanSuffix,
		medianSuffix:                   defaultAggregationMedianSuffix,
		countSuffix:                    defaultAggregationCountSuffix,
		sumSuffix:                      defaultAggregationSumSuffix,
		sumSqSuffix:                    defaultAggregationSumSqSuffix,
		stdevSuffix:                    defaultAggregationStdevSuffix,
		timerQuantileSuffixFn:          defaultTimerQuantileSuffixFn,
		counterSuffixOverride:          defaultCounterSuffixOverride,
		timerSuffixOverride:            defaultTimerSuffixOverride,
		gaugeSuffixOverride:            defaultGaugeSuffixOverride,
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

func (o *options) SetDefaultCounterAggregationTypes(aggTypes AggregationTypes) AggregationTypesOptions {
	opts := *o
	opts.defaultCounterAggregationTypes = aggTypes
	opts.computeSuffixes()
	return &opts
}

func (o *options) DefaultCounterAggregationTypes() AggregationTypes {
	return o.defaultCounterAggregationTypes
}

func (o *options) SetDefaultTimerAggregationTypes(aggTypes AggregationTypes) AggregationTypesOptions {
	opts := *o
	opts.defaultTimerAggregationTypes = aggTypes
	opts.computeQuantiles()
	opts.computeSuffixes()
	return &opts
}

func (o *options) DefaultTimerAggregationTypes() AggregationTypes {
	return o.defaultTimerAggregationTypes
}

func (o *options) SetDefaultGaugeAggregationTypes(aggTypes AggregationTypes) AggregationTypesOptions {
	opts := *o
	opts.defaultGaugeAggregationTypes = aggTypes
	opts.computeSuffixes()
	return &opts
}

func (o *options) DefaultGaugeAggregationTypes() AggregationTypes {
	return o.defaultGaugeAggregationTypes
}

func (o *options) SetLastSuffix(value []byte) AggregationTypesOptions {
	opts := *o
	opts.lastSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) LastSuffix() []byte {
	return o.lastSuffix
}

func (o *options) SetMinSuffix(value []byte) AggregationTypesOptions {
	opts := *o
	opts.minSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) MinSuffix() []byte {
	return o.minSuffix
}

func (o *options) SetMaxSuffix(value []byte) AggregationTypesOptions {
	opts := *o
	opts.maxSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) MaxSuffix() []byte {
	return o.maxSuffix
}

func (o *options) SetMeanSuffix(value []byte) AggregationTypesOptions {
	opts := *o
	opts.meanSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) MeanSuffix() []byte {
	return o.meanSuffix
}

func (o *options) SetMedianSuffix(value []byte) AggregationTypesOptions {
	opts := *o
	opts.medianSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) MedianSuffix() []byte {
	return o.medianSuffix
}

func (o *options) SetCountSuffix(value []byte) AggregationTypesOptions {
	opts := *o
	opts.countSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) CountSuffix() []byte {
	return o.countSuffix
}

func (o *options) SetSumSuffix(value []byte) AggregationTypesOptions {
	opts := *o
	opts.sumSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) SumSuffix() []byte {
	return o.sumSuffix
}

func (o *options) SetSumSqSuffix(value []byte) AggregationTypesOptions {
	opts := *o
	opts.sumSqSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) SumSqSuffix() []byte {
	return o.sumSqSuffix
}

func (o *options) SetStdevSuffix(value []byte) AggregationTypesOptions {
	opts := *o
	opts.stdevSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) StdevSuffix() []byte {
	return o.stdevSuffix
}

func (o *options) SetTimerQuantileSuffixFn(value QuantileSuffixFn) AggregationTypesOptions {
	opts := *o
	opts.timerQuantileSuffixFn = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) TimerQuantileSuffixFn() QuantileSuffixFn {
	return o.timerQuantileSuffixFn
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

func (o *options) SetCounterSuffixOverrides(m map[AggregationType][]byte) AggregationTypesOptions {
	opts := *o
	opts.counterSuffixOverride = m
	opts.computeSuffixes()
	return &opts
}

func (o *options) SetTimerSuffixOverrides(m map[AggregationType][]byte) AggregationTypesOptions {
	opts := *o
	opts.timerSuffixOverride = m
	opts.computeSuffixes()
	return &opts
}

func (o *options) SetGaugeSuffixOverrides(m map[AggregationType][]byte) AggregationTypesOptions {
	opts := *o
	opts.gaugeSuffixOverride = m
	opts.computeSuffixes()
	return &opts
}

func (o *options) DefaultCounterAggregationSuffixes() [][]byte {
	return o.defaultCounterAggregationSuffixes
}

func (o *options) DefaultTimerAggregationSuffixes() [][]byte {
	return o.defaultTimerAggregationSuffixes
}

func (o *options) DefaultGaugeAggregationSuffixes() [][]byte {
	return o.defaultGaugeAggregationSuffixes
}

func (o *options) SuffixForCounter(aggType AggregationType) []byte {
	return o.counterSuffixes[aggType.ID()]
}

func (o *options) SuffixForTimer(aggType AggregationType) []byte {
	return o.timerSuffixes[aggType.ID()]
}

func (o *options) SuffixForGauge(aggType AggregationType) []byte {
	return o.gaugeSuffixes[aggType.ID()]
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

func (o *options) computeAllDerived() {
	o.computeQuantiles()
	o.computeSuffixes()
}

func (o *options) computeQuantiles() {
	o.timerQuantiles, _ = o.DefaultTimerAggregationTypes().PooledQuantiles(o.QuantilesPool())
}

func (o *options) computeSuffixes() {
	// NB(cw) The order matters.
	o.computeDefaultSuffixes()
	o.computeCounterSuffixes()
	o.computeTimerSuffixes()
	o.computeGaugeSuffixes()
	o.computeDefaultCounterAggregationSuffix()
	o.computeDefaultTimerAggregationSuffix()
	o.computeDefaultGaugeAggregationSuffix()
}

func (o *options) computeDefaultSuffixes() {
	o.defaultSuffixes = make([][]byte, MaxAggregationTypeID+1)
	for aggType := range ValidAggregationTypes {
		switch aggType {
		case Last:
			o.defaultSuffixes[aggType.ID()] = o.LastSuffix()
		case Min:
			o.defaultSuffixes[aggType.ID()] = o.MinSuffix()
		case Max:
			o.defaultSuffixes[aggType.ID()] = o.MaxSuffix()
		case Mean:
			o.defaultSuffixes[aggType.ID()] = o.MeanSuffix()
		case Median:
			o.defaultSuffixes[aggType.ID()] = o.MedianSuffix()
		case Count:
			o.defaultSuffixes[aggType.ID()] = o.CountSuffix()
		case Sum:
			o.defaultSuffixes[aggType.ID()] = o.SumSuffix()
		case SumSq:
			o.defaultSuffixes[aggType.ID()] = o.SumSqSuffix()
		case Stdev:
			o.defaultSuffixes[aggType.ID()] = o.StdevSuffix()
		default:
			q, ok := aggType.Quantile()
			if ok {
				o.defaultSuffixes[aggType.ID()] = o.timerQuantileSuffixFn(q)
			}
		}
	}
}

func (o *options) computeCounterSuffixes() {
	o.counterSuffixes = o.computeOverrideSuffixes(o.counterSuffixOverride)
}

func (o *options) computeTimerSuffixes() {
	o.timerSuffixes = o.computeOverrideSuffixes(o.timerSuffixOverride)
}

func (o *options) computeGaugeSuffixes() {
	o.gaugeSuffixes = o.computeOverrideSuffixes(o.gaugeSuffixOverride)
}

func (o options) computeOverrideSuffixes(m map[AggregationType][]byte) [][]byte {
	res := make([][]byte, MaxAggregationTypeID+1)
	for aggType := range ValidAggregationTypes {
		if suffix, ok := m[aggType]; ok {
			res[aggType.ID()] = suffix
			continue
		}
		res[aggType.ID()] = o.defaultSuffixes[aggType.ID()]
	}
	return res
}

func (o *options) computeDefaultCounterAggregationSuffix() {
	o.defaultCounterAggregationSuffixes = make([][]byte, len(o.DefaultCounterAggregationTypes()))
	for i, aggType := range o.DefaultCounterAggregationTypes() {
		o.defaultCounterAggregationSuffixes[i] = o.SuffixForCounter(aggType)
	}
}

func (o *options) computeDefaultTimerAggregationSuffix() {
	o.defaultTimerAggregationSuffixes = make([][]byte, len(o.DefaultTimerAggregationTypes()))
	for i, aggType := range o.DefaultTimerAggregationTypes() {
		o.defaultTimerAggregationSuffixes[i] = o.SuffixForTimer(aggType)
	}
}

func (o *options) computeDefaultGaugeAggregationSuffix() {
	o.defaultGaugeAggregationSuffixes = make([][]byte, len(o.DefaultGaugeAggregationTypes()))
	for i, aggType := range o.DefaultGaugeAggregationTypes() {
		o.defaultGaugeAggregationSuffixes[i] = o.SuffixForGauge(aggType)
	}
}

// By default we use e.g. ".p50", ".p95", ".p99" for the 50th/95th/99th percentile.
func defaultTimerQuantileSuffixFn(quantile float64) []byte {
	str := strconv.FormatFloat(quantile*100, 'f', -1, 64)
	idx := strings.Index(str, ".")
	if idx != -1 {
		str = str[:idx] + str[idx+1:]
	}
	return []byte(".p" + str)
}
