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
	"github.com/m3db/m3metrics/metric"
)

// AggregationTypesOptions provides a set of options for aggregation types.
type AggregationTypesOptions interface {
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
)

type options struct {
	defaultCounterAggregationTypes AggregationTypes
	defaultTimerAggregationTypes   AggregationTypes
	defaultGaugeAggregationTypes   AggregationTypes
}

// NewAggregationTypesOptions returns a default Options.
func NewAggregationTypesOptions() AggregationTypesOptions {
	return &options{
		defaultCounterAggregationTypes: defaultDefaultCounterAggregationTypes,
		defaultGaugeAggregationTypes:   defaultDefaultGaugeAggregationTypes,
		defaultTimerAggregationTypes:   defaultDefaultTimerAggregationTypes,
	}
}

func (o *options) SetDefaultCounterAggregationTypes(aggTypes AggregationTypes) AggregationTypesOptions {
	opts := *o
	opts.defaultCounterAggregationTypes = aggTypes
	return &opts
}

func (o *options) DefaultCounterAggregationTypes() AggregationTypes {
	return o.defaultCounterAggregationTypes
}

func (o *options) SetDefaultTimerAggregationTypes(aggTypes AggregationTypes) AggregationTypesOptions {
	opts := *o
	opts.defaultTimerAggregationTypes = aggTypes
	return &opts
}

func (o *options) DefaultTimerAggregationTypes() AggregationTypes {
	return o.defaultTimerAggregationTypes
}

func (o *options) SetDefaultGaugeAggregationTypes(aggTypes AggregationTypes) AggregationTypesOptions {
	opts := *o
	opts.defaultGaugeAggregationTypes = aggTypes
	return &opts
}

func (o *options) DefaultGaugeAggregationTypes() AggregationTypes {
	return o.defaultGaugeAggregationTypes
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
