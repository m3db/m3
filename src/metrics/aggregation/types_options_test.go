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
	"fmt"
	"testing"

	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func TestTypesOptionsValidateDefault(t *testing.T) {
	o := NewTypesOptions()

	// Validate base options
	require.Equal(t, defaultDefaultCounterAggregationTypes, o.DefaultCounterAggregationTypes())
	require.Equal(t, defaultDefaultTimerAggregationTypes, o.DefaultTimerAggregationTypes())
	require.Equal(t, defaultDefaultGaugeAggregationTypes, o.DefaultGaugeAggregationTypes())
	require.NotNil(t, o.QuantileTypeStringFn())
	require.NotNil(t, o.CounterTypeStringTransformFn())
	require.NotNil(t, o.TimerTypeStringTransformFn())
	require.NotNil(t, o.GaugeTypeStringTransformFn())

	// Validate derived options
	opts := o.(*options)
	validateQuantiles(t, o)
	require.Equal(t, typeStrings(nil), opts.counterTypeStrings)
	require.Equal(t, typeStrings(nil), opts.timerTypeStrings)
	require.Equal(t, typeStrings(nil), opts.gaugeTypeStrings)
}

func TestOptionsSetDefaultCounterAggregationTypes(t *testing.T) {
	aggTypes := Types{Mean, SumSq}
	o := NewTypesOptions().SetDefaultCounterAggregationTypes(aggTypes)
	require.Equal(t, aggTypes, o.DefaultCounterAggregationTypes())
	require.Equal(t, typeStrings(nil), o.(*options).counterTypeStrings)
}

func TestOptionsSetDefaultTimerAggregationTypes(t *testing.T) {
	aggTypes := Types{Mean, SumSq, P99, P9999}
	o := NewTypesOptions().SetDefaultTimerAggregationTypes(aggTypes)
	require.Equal(t, aggTypes, o.DefaultTimerAggregationTypes())
	require.Equal(t, []float64{0.99, 0.9999}, o.Quantiles())
	require.Equal(t, typeStrings(nil), o.(*options).counterTypeStrings)
}

func TestOptionsSetDefaultGaugeAggregationTypes(t *testing.T) {
	aggTypes := Types{Mean, SumSq}
	o := NewTypesOptions().SetDefaultGaugeAggregationTypes(aggTypes)
	require.Equal(t, aggTypes, o.DefaultGaugeAggregationTypes())
	require.Equal(t, typeStrings(nil), o.(*options).gaugeTypeStrings)
}

func TestOptionsSetTimerQuantileTypeStringFn(t *testing.T) {
	fn := func(q float64) []byte { return []byte(fmt.Sprintf("%1.2f", q)) }
	o := NewTypesOptions().SetQuantileTypeStringFn(fn)
	require.Equal(t, []byte("0.96"), o.QuantileTypeStringFn()(0.9582))
	validateQuantiles(t, o)
}

func TestOptionSetCounterTypeStringTranformFn(t *testing.T) {
	o := NewTypesOptions().SetCounterTypeStringTransformFn(EmptyTransform)
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Last))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Min))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Max))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Mean))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Median))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Count))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Sum))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(SumSq))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Stdev))
}

func TestOptionSetTimerTypeStringTranformFn(t *testing.T) {
	o := NewTypesOptions().SetTimerTypeStringTransformFn(SuffixTransform)
	require.Equal(t, []byte(".last"), o.TypeStringForTimer(Last))
	require.Equal(t, []byte(".lower"), o.TypeStringForTimer(Min))
	require.Equal(t, []byte(".upper"), o.TypeStringForTimer(Max))
	require.Equal(t, []byte(".mean"), o.TypeStringForTimer(Mean))
	require.Equal(t, []byte(".median"), o.TypeStringForTimer(Median))
	require.Equal(t, []byte(".count"), o.TypeStringForTimer(Count))
	require.Equal(t, []byte(".sum"), o.TypeStringForTimer(Sum))
	require.Equal(t, []byte(".sum_sq"), o.TypeStringForTimer(SumSq))
	require.Equal(t, []byte(".stdev"), o.TypeStringForTimer(Stdev))
	require.Equal(t, []byte(".p50"), o.TypeStringForTimer(P50))
	require.Equal(t, []byte(".p95"), o.TypeStringForTimer(P95))
	require.Equal(t, []byte(".p99"), o.TypeStringForTimer(P99))
}

func TestOptionSetGaugeTypeStringTranformFn(t *testing.T) {
	o := NewTypesOptions().SetGaugeTypeStringTransformFn(EmptyTransform)
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Last))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Min))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Max))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Mean))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Median))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Count))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Sum))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(SumSq))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Stdev))
}

func TestOptionSetAllTypeStringTranformFns(t *testing.T) {
	o := NewTypesOptions().
		SetCounterTypeStringTransformFn(EmptyTransform).
		SetTimerTypeStringTransformFn(SuffixTransform).
		SetGaugeTypeStringTransformFn(EmptyTransform)

	require.Equal(t, []byte(nil), o.TypeStringForCounter(Last))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Min))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Max))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Mean))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Median))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Count))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Sum))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(SumSq))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Stdev))

	require.Equal(t, []byte(".last"), o.TypeStringForTimer(Last))
	require.Equal(t, []byte(".lower"), o.TypeStringForTimer(Min))
	require.Equal(t, []byte(".upper"), o.TypeStringForTimer(Max))
	require.Equal(t, []byte(".mean"), o.TypeStringForTimer(Mean))
	require.Equal(t, []byte(".median"), o.TypeStringForTimer(Median))
	require.Equal(t, []byte(".count"), o.TypeStringForTimer(Count))
	require.Equal(t, []byte(".sum"), o.TypeStringForTimer(Sum))
	require.Equal(t, []byte(".sum_sq"), o.TypeStringForTimer(SumSq))
	require.Equal(t, []byte(".stdev"), o.TypeStringForTimer(Stdev))
	require.Equal(t, []byte(".p50"), o.TypeStringForTimer(P50))
	require.Equal(t, []byte(".p95"), o.TypeStringForTimer(P95))
	require.Equal(t, []byte(".p99"), o.TypeStringForTimer(P99))

	require.Equal(t, []byte(nil), o.TypeStringForGauge(Last))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Min))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Max))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Mean))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Median))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Count))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Sum))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(SumSq))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Stdev))
}

func TestOptionsCounterTypeString(t *testing.T) {
	o := NewTypesOptions()
	require.Equal(t, []byte("last"), o.TypeStringForCounter(Last))
	require.Equal(t, []byte("lower"), o.TypeStringForCounter(Min))
	require.Equal(t, []byte("upper"), o.TypeStringForCounter(Max))
	require.Equal(t, []byte("mean"), o.TypeStringForCounter(Mean))
	require.Equal(t, []byte("median"), o.TypeStringForCounter(Median))
	require.Equal(t, []byte("count"), o.TypeStringForCounter(Count))
	require.Equal(t, []byte("sum"), o.TypeStringForCounter(Sum))
	require.Equal(t, []byte("sum_sq"), o.TypeStringForCounter(SumSq))
	require.Equal(t, []byte("stdev"), o.TypeStringForCounter(Stdev))
}

func TestOptionsTimerTypeString(t *testing.T) {
	o := NewTypesOptions()
	require.Equal(t, []byte("last"), o.TypeStringForTimer(Last))
	require.Equal(t, []byte("lower"), o.TypeStringForTimer(Min))
	require.Equal(t, []byte("upper"), o.TypeStringForTimer(Max))
	require.Equal(t, []byte("mean"), o.TypeStringForTimer(Mean))
	require.Equal(t, []byte("median"), o.TypeStringForTimer(Median))
	require.Equal(t, []byte("count"), o.TypeStringForTimer(Count))
	require.Equal(t, []byte("sum"), o.TypeStringForTimer(Sum))
	require.Equal(t, []byte("sum_sq"), o.TypeStringForTimer(SumSq))
	require.Equal(t, []byte("stdev"), o.TypeStringForTimer(Stdev))
}

func TestOptionsGaugeTypeString(t *testing.T) {
	o := NewTypesOptions()
	require.Equal(t, []byte("last"), o.TypeStringForGauge(Last))
	require.Equal(t, []byte("lower"), o.TypeStringForGauge(Min))
	require.Equal(t, []byte("upper"), o.TypeStringForGauge(Max))
	require.Equal(t, []byte("mean"), o.TypeStringForGauge(Mean))
	require.Equal(t, []byte("median"), o.TypeStringForGauge(Median))
	require.Equal(t, []byte("count"), o.TypeStringForGauge(Count))
	require.Equal(t, []byte("sum"), o.TypeStringForGauge(Sum))
	require.Equal(t, []byte("sum_sq"), o.TypeStringForGauge(SumSq))
	require.Equal(t, []byte("stdev"), o.TypeStringForGauge(Stdev))
}

func TestOptionTimerQuantileTypeString(t *testing.T) {
	o := NewTypesOptions()
	cases := []struct {
		quantile float64
		b        []byte
	}{
		{
			quantile: 0.01,
			b:        []byte("p1"),
		},
		{
			quantile: 0.1,
			b:        []byte("p10"),
		},
		{
			quantile: 0.5,
			b:        []byte("p50"),
		},
		{
			quantile: 0.9,
			b:        []byte("p90"),
		},
		{
			quantile: 0.90,
			b:        []byte("p90"),
		},
		{
			quantile: 0.90,
			b:        []byte("p90"),
		},
		{
			quantile: 0.909,
			b:        []byte("p909"),
		},
		{
			quantile: 0.999,
			b:        []byte("p999"),
		},
		{
			quantile: 0.9990,
			b:        []byte("p999"),
		},
		{
			quantile: 0.9999,
			b:        []byte("p9999"),
		},
		{
			quantile: 0.99995,
			b:        []byte("p99995"),
		},
		{
			quantile: 0.123,
			b:        []byte("p123"),
		},
	}

	for _, c := range cases {
		require.Equal(t, c.b, o.QuantileTypeStringFn()(c.quantile))
	}
}

func TestSetQuantilesPool(t *testing.T) {
	p := pool.NewFloatsPool(nil, nil)
	o := NewTypesOptions().SetQuantilesPool(p)
	require.Equal(t, p, o.QuantilesPool())
}

func validateQuantiles(t *testing.T, o TypesOptions) {
	typeStringFn := o.QuantileTypeStringFn()
	quantiles, _ := o.DefaultTimerAggregationTypes().PooledQuantiles(nil)
	require.Equal(t, o.Quantiles(), quantiles)

	for _, aggType := range o.DefaultTimerAggregationTypes() {
		q, ok := aggType.Quantile()
		if !ok || aggType == Median {
			continue
		}
		require.Equal(t, typeStringFn(q), o.TypeStringForTimer(aggType))
	}
}

func typeStrings(overrides map[Type][]byte) [][]byte {
	defaultTypeStrings := map[Type][]byte{
		Last:   []byte("last"),
		Min:    []byte("lower"),
		Max:    []byte("upper"),
		Mean:   []byte("mean"),
		Median: []byte("median"),
		Count:  []byte("count"),
		Sum:    []byte("sum"),
		SumSq:  []byte("sum_sq"),
		Stdev:  []byte("stdev"),
		P10:    []byte("p10"),
		P20:    []byte("p20"),
		P30:    []byte("p30"),
		P40:    []byte("p40"),
		P50:    []byte("p50"),
		P60:    []byte("p60"),
		P70:    []byte("p70"),
		P80:    []byte("p80"),
		P90:    []byte("p90"),
		P95:    []byte("p95"),
		P99:    []byte("p99"),
		P999:   []byte("p999"),
		P9999:  []byte("p9999"),
	}
	res := make([][]byte, maxTypeID+1)
	for t, bstr := range defaultTypeStrings {
		if override, exist := overrides[t]; exist {
			res[t.ID()] = override
			continue
		}
		res[t.ID()] = bstr
	}
	return res
}
