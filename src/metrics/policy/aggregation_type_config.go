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
	"fmt"

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

	// Global type string overrides.
	GlobalOverrides map[AggregationType]string `yaml:"globalOverrides"`

	// Type string overrides for Counter.
	CounterOverrides map[AggregationType]string `yaml:"counterOverrides"`

	// Type string overrides for Timer.
	TimerOverrides map[AggregationType]string `yaml:"timerOverrides"`

	// Type string overrides for Gauge.
	GaugeOverrides map[AggregationType]string `yaml:"gaugeOverrides"`

	// TransformFnType configs the global type string transform function type.
	TransformFnType *transformFnType `yaml:"transformFnType"`

	// Pool of aggregation types.
	AggregationTypesPool pool.ObjectPoolConfiguration `yaml:"aggregationTypesPool"`

	// Pool of quantile slices.
	QuantilesPool pool.BucketizedPoolConfiguration `yaml:"quantilesPool"`
}

// NewOptions creates a new Option.
func (c AggregationTypesConfiguration) NewOptions(instrumentOpts instrument.Options) (AggregationTypesOptions, error) {
	opts := NewAggregationTypesOptions()
	if c.TransformFnType != nil {
		fn, err := c.TransformFnType.TransformFn()
		if err != nil {
			return nil, err
		}
		opts = opts.SetGlobalTypeStringTransformFn(fn)
	}

	if c.DefaultCounterAggregationTypes != nil {
		opts = opts.SetDefaultCounterAggregationTypes(*c.DefaultCounterAggregationTypes)
	}
	if c.DefaultGaugeAggregationTypes != nil {
		opts = opts.SetDefaultGaugeAggregationTypes(*c.DefaultGaugeAggregationTypes)
	}
	if c.DefaultTimerAggregationTypes != nil {
		opts = opts.SetDefaultTimerAggregationTypes(*c.DefaultTimerAggregationTypes)
	}

	opts = opts.SetGlobalTypeStringOverrides(parseTypeStringOverride(c.GlobalOverrides))
	opts = opts.SetCounterTypeStringOverrides(parseTypeStringOverride(c.CounterOverrides))
	opts = opts.SetGaugeTypeStringOverrides(parseTypeStringOverride(c.GaugeOverrides))
	opts = opts.SetTimerTypeStringOverrides(parseTypeStringOverride(c.TimerOverrides))

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

	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return opts, nil
}

func parseTypeStringOverride(m map[AggregationType]string) map[AggregationType][]byte {
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

type transformFnType string

var (
	noopTransformType   transformFnType = "noop"
	suffixTransformType transformFnType = "suffix"

	validTypes = []transformFnType{
		noopTransformType,
		suffixTransformType,
	}
)

func (t *transformFnType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	var validStrings []string
	for _, validType := range validTypes {
		validString := string(validType)
		if validString == str {
			*t = validType
			return nil
		}
		validStrings = append(validStrings, validString)
	}

	return fmt.Errorf("invalid transform type %s, valid types are: %v", str, validStrings)
}

func (t transformFnType) TransformFn() (TypeStringTransformFn, error) {
	switch t {
	case noopTransformType:
		return noopTransformFn, nil
	case suffixTransformType:
		return suffixTransformFn, nil
	default:
		return nil, fmt.Errorf("invalid type string transform function type: %s", string(t))
	}
}
