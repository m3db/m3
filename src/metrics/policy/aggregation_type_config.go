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
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
)

// AggregationTypesConfiguration contains configuration for aggregation types.
type AggregationTypesConfiguration struct {
	// Default aggregation types for counter metrics.
	DefaultCounterAggregationTypes *AggregationTypes `yaml:"defaultCounterAggregationTypes"`

	// Default aggregation types for timer metrics.
	DefaultTimerAggregationTypes *AggregationTypes `yaml:"defaultTimerAggregationTypes"`

	// Default aggregation types for gauge metrics.
	DefaultGaugeAggregationTypes *AggregationTypes `yaml:"defaultGaugeAggregationTypes"`

	// Metric suffix for aggregation type last.
	LastSuffix *string `yaml:"lastSuffix"`

	// Metric suffix for aggregation type sum.
	SumSuffix *string `yaml:"sumSuffix"`

	// Metric suffix for aggregation type sum square.
	SumSqSuffix *string `yaml:"sumSqSuffix"`

	// Metric suffix for aggregation type mean.
	MeanSuffix *string `yaml:"meanSuffix"`

	// Metric suffix for aggregation type min.
	MinSuffix *string `yaml:"minSuffix"`

	// Metric suffix for aggregation type max.
	MaxSuffix *string `yaml:"maxSuffix"`

	// Metric suffix for aggregation type count.
	CountSuffix *string `yaml:"countSuffix"`

	// Metric suffix for aggregation type standard deviation.
	StdevSuffix *string `yaml:"stdevSuffix"`

	// Metric suffix for aggregation type median.
	MedianSuffix *string `yaml:"medianSuffix"`

	// Counter suffix overrides.
	CounterSuffixOverrides map[AggregationType]string `yaml:"counterSuffixOverrides"`

	// Timer suffix overrides.
	TimerSuffixOverrides map[AggregationType]string `yaml:"timerSuffixOverrides"`

	// Gauge suffix overrides.
	GaugeSuffixOverrides map[AggregationType]string `yaml:"gaugeSuffixOverrides"`

	// Pool of aggregation types.
	AggregationTypesPool pool.ObjectPoolConfiguration `yaml:"aggregationTypesPool"`

	// Pool of quantile slices.
	QuantilesPool pool.BucketizedPoolConfiguration `yaml:"quantilesPool"`
}

// NewOptions creates a new Option.
func (c AggregationTypesConfiguration) NewOptions(instrumentOpts instrument.Options) AggregationTypesOptions {
	opts := NewAggregationTypesOptions()
	if c.DefaultCounterAggregationTypes != nil {
		opts = opts.SetDefaultCounterAggregationTypes(*c.DefaultCounterAggregationTypes)
	}
	if c.DefaultGaugeAggregationTypes != nil {
		opts = opts.SetDefaultGaugeAggregationTypes(*c.DefaultGaugeAggregationTypes)
	}
	if c.DefaultTimerAggregationTypes != nil {
		opts = opts.SetDefaultTimerAggregationTypes(*c.DefaultTimerAggregationTypes)
	}

	opts = setSuffix(opts, c.LastSuffix, opts.SetLastSuffix)
	opts = setSuffix(opts, c.SumSuffix, opts.SetSumSuffix)
	opts = setSuffix(opts, c.SumSqSuffix, opts.SetSumSqSuffix)
	opts = setSuffix(opts, c.MeanSuffix, opts.SetMeanSuffix)
	opts = setSuffix(opts, c.MinSuffix, opts.SetMinSuffix)
	opts = setSuffix(opts, c.MaxSuffix, opts.SetMaxSuffix)
	opts = setSuffix(opts, c.CountSuffix, opts.SetCountSuffix)
	opts = setSuffix(opts, c.StdevSuffix, opts.SetStdevSuffix)
	opts = setSuffix(opts, c.MedianSuffix, opts.SetMedianSuffix)

	opts = opts.SetCounterSuffixOverrides(parseSuffixOverride(c.CounterSuffixOverrides))
	opts = opts.SetGaugeSuffixOverrides(parseSuffixOverride(c.GaugeSuffixOverrides))
	opts = opts.SetTimerSuffixOverrides(parseSuffixOverride(c.TimerSuffixOverrides))

	scope := instrumentOpts.MetricsScope()

	// Set aggregation types pool.
	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("aggregation-types-pool"))
	aggTypesPoolOpts := c.AggregationTypesPool.NewObjectPoolOptions(iOpts)
	aggTypesPool := NewAggregationTypesPool(aggTypesPoolOpts)
	opts = opts.SetAggregationTypesPool(aggTypesPool)
	aggTypesPool.Init(func() AggregationTypes {
		return make(AggregationTypes, 0, len(ValidAggregationTypes))
	})

	// Set quantiles pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("quantile-pool"))
	quantilesPool := pool.NewFloatsPool(
		c.QuantilesPool.NewBuckets(),
		c.QuantilesPool.NewObjectPoolOptions(iOpts),
	)
	opts = opts.SetQuantilesPool(quantilesPool)
	quantilesPool.Init()
	return opts
}

func setSuffix(
	opts AggregationTypesOptions,
	strP *string,
	fn func(value []byte) AggregationTypesOptions,
) AggregationTypesOptions {
	if strP == nil {
		return opts
	}
	str := *strP
	if str == "" {
		return fn(nil)
	}
	return fn([]byte(str))
}

func parseSuffixOverride(m map[AggregationType]string) map[AggregationType][]byte {
	res := make(map[AggregationType][]byte, len(m))
	for aggType, s := range m {
		var bytes []byte
		if s != "" {
			// NB(cw) []byte("") is empty with a cap of 8.
			bytes = []byte(s)
		}
		res[aggType] = bytes
	}
	return res
}
