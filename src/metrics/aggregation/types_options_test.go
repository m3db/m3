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

	"github.com/m3db/m3/src/x/pool"

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

func TestOptionTypeStringTranformFn(t *testing.T) {
	inputs := []struct {
		aggType  Type
		expected []byte
	}{
		{aggType: Last, expected: []byte(nil)},
		{aggType: Min, expected: []byte(nil)},
		{aggType: Max, expected: []byte(nil)},
		{aggType: Mean, expected: []byte(nil)},
		{aggType: Median, expected: []byte(nil)},
		{aggType: Count, expected: []byte(nil)},
		{aggType: Sum, expected: []byte(nil)},
		{aggType: SumSq, expected: []byte(nil)},
		{aggType: Stdev, expected: []byte(nil)},
	}

	o := NewTypesOptions().SetCounterTypeStringTransformFn(EmptyTransform)
	for _, input := range inputs {
		require.Equal(t, input.expected, o.TypeStringForCounter(input.aggType))
	}
}

func TestOptionSetTimerTypeStringTranformFn(t *testing.T) {
	inputs := []struct {
		aggType  Type
		expected []byte
	}{
		{aggType: Last, expected: []byte(".last")},
		{aggType: Min, expected: []byte(".lower")},
		{aggType: Max, expected: []byte(".upper")},
		{aggType: Mean, expected: []byte(".mean")},
		{aggType: Median, expected: []byte(".median")},
		{aggType: Count, expected: []byte(".count")},
		{aggType: Sum, expected: []byte(".sum")},
		{aggType: SumSq, expected: []byte(".sum_sq")},
		{aggType: Stdev, expected: []byte(".stdev")},
		{aggType: P50, expected: []byte(".p50")},
		{aggType: P95, expected: []byte(".p95")},
		{aggType: P99, expected: []byte(".p99")},
	}

	o := NewTypesOptions().SetTimerTypeStringTransformFn(SuffixTransform)
	for _, input := range inputs {
		require.Equal(t, input.expected, o.TypeStringForTimer(input.aggType))
	}
}

func TestOptionSetGaugeTypeStringTranformFn(t *testing.T) {
	inputs := []struct {
		aggType  Type
		expected []byte
	}{
		{aggType: Last, expected: []byte(nil)},
		{aggType: Min, expected: []byte(nil)},
		{aggType: Max, expected: []byte(nil)},
		{aggType: Mean, expected: []byte(nil)},
		{aggType: Median, expected: []byte(nil)},
		{aggType: Count, expected: []byte(nil)},
		{aggType: Sum, expected: []byte(nil)},
		{aggType: SumSq, expected: []byte(nil)},
		{aggType: Stdev, expected: []byte(nil)},
	}

	o := NewTypesOptions().SetGaugeTypeStringTransformFn(EmptyTransform)
	for _, input := range inputs {
		require.Equal(t, input.expected, o.TypeStringForGauge(input.aggType))
	}
}

func TestOptionSetAllTypeStringTranformFns(t *testing.T) {
	o := NewTypesOptions().
		SetCounterTypeStringTransformFn(EmptyTransform).
		SetTimerTypeStringTransformFn(SuffixTransform).
		SetGaugeTypeStringTransformFn(EmptyTransform)

	inputs := []struct {
		aggType  Type
		expected []byte
	}{
		{aggType: Last, expected: []byte(nil)},
		{aggType: Min, expected: []byte(nil)},
		{aggType: Max, expected: []byte(nil)},
		{aggType: Mean, expected: []byte(nil)},
		{aggType: Median, expected: []byte(nil)},
		{aggType: Count, expected: []byte(nil)},
		{aggType: Sum, expected: []byte(nil)},
		{aggType: SumSq, expected: []byte(nil)},
		{aggType: Stdev, expected: []byte(nil)},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, o.TypeStringForCounter(input.aggType))
	}

	inputs = []struct {
		aggType  Type
		expected []byte
	}{
		{aggType: Last, expected: []byte(".last")},
		{aggType: Min, expected: []byte(".lower")},
		{aggType: Max, expected: []byte(".upper")},
		{aggType: Mean, expected: []byte(".mean")},
		{aggType: Median, expected: []byte(".median")},
		{aggType: Count, expected: []byte(".count")},
		{aggType: Sum, expected: []byte(".sum")},
		{aggType: SumSq, expected: []byte(".sum_sq")},
		{aggType: Stdev, expected: []byte(".stdev")},
		{aggType: P50, expected: []byte(".p50")},
		{aggType: P95, expected: []byte(".p95")},
		{aggType: P99, expected: []byte(".p99")},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, o.TypeStringForTimer(input.aggType))
	}

	inputs = []struct {
		aggType  Type
		expected []byte
	}{
		{aggType: Last, expected: []byte(nil)},
		{aggType: Min, expected: []byte(nil)},
		{aggType: Max, expected: []byte(nil)},
		{aggType: Mean, expected: []byte(nil)},
		{aggType: Median, expected: []byte(nil)},
		{aggType: Count, expected: []byte(nil)},
		{aggType: Sum, expected: []byte(nil)},
		{aggType: SumSq, expected: []byte(nil)},
		{aggType: Stdev, expected: []byte(nil)},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, o.TypeStringForGauge(input.aggType))
	}
}

func TestOptionsTypeStringForCounter(t *testing.T) {
	inputs := []struct {
		aggType  Type
		expected []byte
	}{
		{aggType: Last, expected: []byte("last")},
		{aggType: Min, expected: []byte("lower")},
		{aggType: Max, expected: []byte("upper")},
		{aggType: Mean, expected: []byte("mean")},
		{aggType: Median, expected: []byte("median")},
		{aggType: Count, expected: []byte("count")},
		{aggType: Sum, expected: []byte("sum")},
		{aggType: SumSq, expected: []byte("sum_sq")},
		{aggType: Stdev, expected: []byte("stdev")},
		{aggType: P50, expected: []byte("p50")},
		{aggType: P95, expected: []byte("p95")},
		{aggType: P99, expected: []byte("p99")},
	}

	o := NewTypesOptions()
	for _, input := range inputs {
		require.Equal(t, input.expected, o.TypeStringForCounter(input.aggType))
	}
}

func TestOptionsTypeStringForTimer(t *testing.T) {
	inputs := []struct {
		aggType  Type
		expected []byte
	}{
		{aggType: Last, expected: []byte("last")},
		{aggType: Min, expected: []byte("lower")},
		{aggType: Max, expected: []byte("upper")},
		{aggType: Mean, expected: []byte("mean")},
		{aggType: Median, expected: []byte("median")},
		{aggType: Count, expected: []byte("count")},
		{aggType: Sum, expected: []byte("sum")},
		{aggType: SumSq, expected: []byte("sum_sq")},
		{aggType: Stdev, expected: []byte("stdev")},
		{aggType: P50, expected: []byte("p50")},
		{aggType: P95, expected: []byte("p95")},
		{aggType: P99, expected: []byte("p99")},
	}

	o := NewTypesOptions()
	for _, input := range inputs {
		require.Equal(t, input.expected, o.TypeStringForTimer(input.aggType))
	}
}

func TestOptionsTypeStringForGauge(t *testing.T) {
	inputs := []struct {
		aggType  Type
		expected []byte
	}{
		{aggType: Last, expected: []byte("last")},
		{aggType: Min, expected: []byte("lower")},
		{aggType: Max, expected: []byte("upper")},
		{aggType: Mean, expected: []byte("mean")},
		{aggType: Median, expected: []byte("median")},
		{aggType: Count, expected: []byte("count")},
		{aggType: Sum, expected: []byte("sum")},
		{aggType: SumSq, expected: []byte("sum_sq")},
		{aggType: Stdev, expected: []byte("stdev")},
		{aggType: P50, expected: []byte("p50")},
		{aggType: P95, expected: []byte("p95")},
		{aggType: P99, expected: []byte("p99")},
	}

	o := NewTypesOptions()
	for _, input := range inputs {
		require.Equal(t, input.expected, o.TypeStringForGauge(input.aggType))
	}
}

func TestOptionsTypeForCounter(t *testing.T) {
	inputs := []struct {
		typeStr  []byte
		expected Type
	}{
		{typeStr: []byte("last"), expected: Last},
		{typeStr: []byte("lower"), expected: Min},
		{typeStr: []byte("upper"), expected: Max},
		{typeStr: []byte("mean"), expected: Mean},
		{typeStr: []byte("median"), expected: Median},
		{typeStr: []byte("count"), expected: Count},
		{typeStr: []byte("sum"), expected: Sum},
		{typeStr: []byte("sum_sq"), expected: SumSq},
		{typeStr: []byte("stdev"), expected: Stdev},
		{typeStr: []byte("p50"), expected: P50},
		{typeStr: []byte("p95"), expected: P95},
		{typeStr: []byte("p99"), expected: P99},
		{typeStr: []byte(nil), expected: UnknownType},
		{typeStr: []byte("abcd"), expected: UnknownType},
	}

	o := NewTypesOptions()
	for _, input := range inputs {
		require.Equal(t, input.expected, o.TypeForCounter(input.typeStr))
	}
}

func TestOptionsTypeForTimer(t *testing.T) {
	inputs := []struct {
		typeStr  []byte
		expected Type
	}{
		{typeStr: []byte(".last"), expected: Last},
		{typeStr: []byte(".lower"), expected: Min},
		{typeStr: []byte(".upper"), expected: Max},
		{typeStr: []byte(".mean"), expected: Mean},
		{typeStr: []byte(".median"), expected: Median},
		{typeStr: []byte(".count"), expected: Count},
		{typeStr: []byte(".sum"), expected: Sum},
		{typeStr: []byte(".sum_sq"), expected: SumSq},
		{typeStr: []byte(".stdev"), expected: Stdev},
		{typeStr: []byte(".p50"), expected: P50},
		{typeStr: []byte(".p95"), expected: P95},
		{typeStr: []byte(".p99"), expected: P99},
		{typeStr: []byte(nil), expected: UnknownType},
		{typeStr: []byte("abcd"), expected: UnknownType},
	}

	o := NewTypesOptions().SetTimerTypeStringTransformFn(SuffixTransform)
	for _, input := range inputs {
		require.Equal(t, input.expected, o.TypeForTimer(input.typeStr))
	}
}

func TestOptionsTypeForGauge(t *testing.T) {
	inputs := []struct {
		typeStr  []byte
		expected Type
	}{
		{typeStr: []byte("last"), expected: Last},
		{typeStr: []byte("lower"), expected: Min},
		{typeStr: []byte("upper"), expected: Max},
		{typeStr: []byte("mean"), expected: Mean},
		{typeStr: []byte("median"), expected: Median},
		{typeStr: []byte("count"), expected: Count},
		{typeStr: []byte("sum"), expected: Sum},
		{typeStr: []byte("sum_sq"), expected: SumSq},
		{typeStr: []byte("stdev"), expected: Stdev},
		{typeStr: []byte("p50"), expected: P50},
		{typeStr: []byte("p95"), expected: P95},
		{typeStr: []byte("p99"), expected: P99},
		{typeStr: []byte(nil), expected: UnknownType},
		{typeStr: []byte("abcd"), expected: UnknownType},
	}

	o := NewTypesOptions()
	for _, input := range inputs {
		require.Equal(t, input.expected, o.TypeForGauge(input.typeStr))
	}
}

func TestOptionQuantileTypeString(t *testing.T) {
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
	res := make([][]byte, maxTypeID+1)
	for t, bstr := range typeStringNames {
		if override, exist := overrides[t]; exist {
			res[t.ID()] = override
			continue
		}
		res[t.ID()] = bstr
	}
	return res
}
