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
	"strings"
	"testing"

	"github.com/m3db/m3x/pool"
	"github.com/stretchr/testify/require"
)

func TestAggregationTypesOptionsValidateDefault(t *testing.T) {
	o := NewAggregationTypesOptions()

	// Validate base options
	require.Equal(t, defaultDefaultCounterAggregationTypes, o.DefaultCounterAggregationTypes())
	require.Equal(t, defaultDefaultTimerAggregationTypes, o.DefaultTimerAggregationTypes())
	require.Equal(t, defaultDefaultGaugeAggregationTypes, o.DefaultGaugeAggregationTypes())
	require.NotNil(t, o.TimerQuantileTypeStringFn())
	require.NotNil(t, o.GlobalTypeStringTransformFn())

	// Validate derived options
	validateQuantiles(t, o)
	require.Equal(t, [][]byte{nil}, o.DefaultCounterAggregationTypeStrings())
	require.Equal(t, [][]byte{
		[]byte(defaultSumTypeString),
		[]byte(defaultSumSqTypeString),
		[]byte(defaultMeanTypeString),
		[]byte(defaultMinTypeString),
		[]byte(defaultMaxTypeString),
		[]byte(defaultCountTypeString),
		[]byte(defaultStdevTypeString),
		[]byte(defaultMedianTypeString),
		[]byte("p50"),
		[]byte("p95"),
		[]byte("p99"),
	}, o.DefaultTimerAggregationTypeStrings())
	require.Equal(t, [][]byte{nil}, o.DefaultGaugeAggregationTypeStrings())
}

func validateQuantiles(t *testing.T, o AggregationTypesOptions) {
	typeStringFn := o.TimerQuantileTypeStringFn()
	quantiles, _ := o.DefaultTimerAggregationTypes().PooledQuantiles(nil)
	require.Equal(t, o.TimerQuantiles(), quantiles)

	for _, aggType := range o.DefaultTimerAggregationTypes() {
		q, ok := aggType.Quantile()
		if !ok || aggType == Median {
			continue
		}
		require.Equal(t, typeStringFn(q), o.TypeStringForTimer(aggType))
	}
}

func TestOptionsSetDefaultCounterAggregationTypes(t *testing.T) {
	aggTypes := AggregationTypes{Mean, SumSq}
	o := NewAggregationTypesOptions().SetDefaultCounterAggregationTypes(aggTypes)
	require.Equal(t, aggTypes, o.DefaultCounterAggregationTypes())
	require.Equal(t, [][]byte{[]byte(defaultMeanTypeString), []byte(defaultSumSqTypeString)}, o.DefaultCounterAggregationTypeStrings())
}

func TestOptionsSetDefaultTimerAggregationTypes(t *testing.T) {
	aggTypes := AggregationTypes{Mean, SumSq, P99, P9999}
	o := NewAggregationTypesOptions().SetDefaultTimerAggregationTypes(aggTypes)
	require.Equal(t, aggTypes, o.DefaultTimerAggregationTypes())
	require.Equal(t, []float64{0.99, 0.9999}, o.TimerQuantiles())
	require.Equal(t, [][]byte{[]byte(defaultMeanTypeString), []byte(defaultSumSqTypeString), []byte("p99"), []byte("p9999")}, o.DefaultTimerAggregationTypeStrings())
}

func TestOptionsSetDefaultGaugeAggregationTypes(t *testing.T) {
	aggTypes := AggregationTypes{Mean, SumSq}
	o := NewAggregationTypesOptions().SetDefaultGaugeAggregationTypes(aggTypes)
	require.Equal(t, aggTypes, o.DefaultGaugeAggregationTypes())
	require.Equal(t, [][]byte{[]byte(defaultMeanTypeString), []byte(defaultSumSqTypeString)}, o.DefaultGaugeAggregationTypeStrings())
}

func TestOptionsSetTimerSumSqTypeString(t *testing.T) {
	newSumSqTypeString := []byte("testTimerSumSqTypeString")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{SumSq}).
		SetDefaultTimerAggregationTypes(AggregationTypes{SumSq}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{SumSq}).
		SetGlobalTypeStringOverrides(map[AggregationType][]byte{SumSq: newSumSqTypeString})
	require.Equal(t, newSumSqTypeString, o.TypeStringForCounter(SumSq))
	require.Equal(t, newSumSqTypeString, o.TypeStringForTimer(SumSq))
	require.Equal(t, newSumSqTypeString, o.TypeStringForGauge(SumSq))
	require.Equal(t, [][]byte{[]byte(newSumSqTypeString)}, o.DefaultCounterAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(newSumSqTypeString)}, o.DefaultTimerAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(newSumSqTypeString)}, o.DefaultGaugeAggregationTypeStrings())
	require.Equal(t, SumSq, o.AggregationTypeForCounter([]byte("testTimerSumSqTypeString")))
	require.Equal(t, SumSq, o.AggregationTypeForTimer([]byte("testTimerSumSqTypeString")))
	require.Equal(t, SumSq, o.AggregationTypeForGauge([]byte("testTimerSumSqTypeString")))
	require.NoError(t, o.Validate())
}

func TestOptionsSetTimerMeanTypeString(t *testing.T) {
	newMeanTypeString := []byte("testTimerMeanTypeString")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{Mean}).
		SetDefaultTimerAggregationTypes(AggregationTypes{Mean}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{Mean}).
		SetGlobalTypeStringOverrides(map[AggregationType][]byte{Mean: newMeanTypeString})
	require.Equal(t, newMeanTypeString, o.TypeStringForCounter(Mean))
	require.Equal(t, newMeanTypeString, o.TypeStringForTimer(Mean))
	require.Equal(t, newMeanTypeString, o.TypeStringForGauge(Mean))
	require.Equal(t, [][]byte{[]byte(newMeanTypeString)}, o.DefaultCounterAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(newMeanTypeString)}, o.DefaultTimerAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(newMeanTypeString)}, o.DefaultGaugeAggregationTypeStrings())
	require.Equal(t, Mean, o.AggregationTypeForCounter([]byte("testTimerMeanTypeString")))
	require.Equal(t, Mean, o.AggregationTypeForTimer([]byte("testTimerMeanTypeString")))
	require.Equal(t, Mean, o.AggregationTypeForGauge([]byte("testTimerMeanTypeString")))
	require.NoError(t, o.Validate())
}

func TestOptionsSetCounterSumTypeString(t *testing.T) {
	newSumTypeString := []byte("testSumTypeString")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{Sum}).
		SetDefaultTimerAggregationTypes(AggregationTypes{Sum}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{Sum}).
		SetGlobalTypeStringOverrides(map[AggregationType][]byte{Sum: newSumTypeString})
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Sum))
	require.Equal(t, newSumTypeString, o.TypeStringForTimer(Sum))
	require.Equal(t, newSumTypeString, o.TypeStringForGauge(Sum))
	require.Equal(t, [][]byte{[]byte(nil)}, o.DefaultCounterAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(newSumTypeString)}, o.DefaultTimerAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(newSumTypeString)}, o.DefaultGaugeAggregationTypeStrings())
	require.Equal(t, Sum, o.AggregationTypeForCounter([]byte(nil)))
	require.Equal(t, Sum, o.AggregationTypeForTimer([]byte("testSumTypeString")))
	require.Equal(t, Sum, o.AggregationTypeForGauge([]byte("testSumTypeString")))
	require.NoError(t, o.Validate())
}

func TestOptionsSetGaugeLastTypeString(t *testing.T) {
	newLastTypeString := []byte("testLastTypeString")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{Last}).
		SetDefaultTimerAggregationTypes(AggregationTypes{Last}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{Last}).
		SetGlobalTypeStringOverrides(map[AggregationType][]byte{Last: newLastTypeString})
	require.Equal(t, newLastTypeString, o.TypeStringForCounter(Last))
	require.Equal(t, newLastTypeString, o.TypeStringForTimer(Last))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Last))
	require.Equal(t, [][]byte{[]byte(newLastTypeString)}, o.DefaultCounterAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(newLastTypeString)}, o.DefaultTimerAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(nil)}, o.DefaultGaugeAggregationTypeStrings())
	require.Equal(t, Last, o.AggregationTypeForCounter([]byte("testLastTypeString")))
	require.Equal(t, Last, o.AggregationTypeForTimer([]byte("testLastTypeString")))
	require.Equal(t, Last, o.AggregationTypeForGauge([]byte(nil)))
	require.NoError(t, o.Validate())
}

func TestOptionsSetTimerCountTypeString(t *testing.T) {
	newCountTypeString := []byte("testTimerCountTypeString")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{Count}).
		SetDefaultTimerAggregationTypes(AggregationTypes{Count}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{Count}).
		SetGlobalTypeStringOverrides(map[AggregationType][]byte{Count: newCountTypeString})
	require.Equal(t, newCountTypeString, o.TypeStringForCounter(Count))
	require.Equal(t, newCountTypeString, o.TypeStringForTimer(Count))
	require.Equal(t, newCountTypeString, o.TypeStringForGauge(Count))
	require.Equal(t, [][]byte{[]byte(newCountTypeString)}, o.DefaultCounterAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(newCountTypeString)}, o.DefaultTimerAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(newCountTypeString)}, o.DefaultGaugeAggregationTypeStrings())
	require.Equal(t, Count, o.AggregationTypeForCounter([]byte("testTimerCountTypeString")))
	require.Equal(t, Count, o.AggregationTypeForTimer([]byte("testTimerCountTypeString")))
	require.Equal(t, Count, o.AggregationTypeForGauge([]byte("testTimerCountTypeString")))
	require.NoError(t, o.Validate())
}

func TestOptionsSetTimerStdevTypeString(t *testing.T) {
	newStdevTypeString := []byte("testTimerStdevTypeString")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{Stdev}).
		SetDefaultTimerAggregationTypes(AggregationTypes{Stdev}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{Stdev}).
		SetGlobalTypeStringOverrides(map[AggregationType][]byte{Stdev: newStdevTypeString})
	require.Equal(t, newStdevTypeString, o.TypeStringForCounter(Stdev))
	require.Equal(t, newStdevTypeString, o.TypeStringForTimer(Stdev))
	require.Equal(t, newStdevTypeString, o.TypeStringForGauge(Stdev))
	require.Equal(t, [][]byte{[]byte(newStdevTypeString)}, o.DefaultCounterAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(newStdevTypeString)}, o.DefaultTimerAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(newStdevTypeString)}, o.DefaultGaugeAggregationTypeStrings())
	require.Equal(t, Stdev, o.AggregationTypeForCounter([]byte("testTimerStdevTypeString")))
	require.Equal(t, Stdev, o.AggregationTypeForTimer([]byte("testTimerStdevTypeString")))
	require.Equal(t, Stdev, o.AggregationTypeForGauge([]byte("testTimerStdevTypeString")))
	require.NoError(t, o.Validate())
}

func TestOptionsSetTimerMedianTypeString(t *testing.T) {
	newMedianTypeString := []byte("testTimerMedianTypeString")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{Median}).
		SetDefaultTimerAggregationTypes(AggregationTypes{Median}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{Median}).
		SetGlobalTypeStringOverrides(map[AggregationType][]byte{Median: newMedianTypeString})
	require.Equal(t, newMedianTypeString, o.TypeStringForCounter(Median))
	require.Equal(t, newMedianTypeString, o.TypeStringForTimer(Median))
	require.Equal(t, newMedianTypeString, o.TypeStringForGauge(Median))
	require.Equal(t, [][]byte{[]byte(newMedianTypeString)}, o.DefaultCounterAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(newMedianTypeString)}, o.DefaultTimerAggregationTypeStrings())
	require.Equal(t, [][]byte{[]byte(newMedianTypeString)}, o.DefaultGaugeAggregationTypeStrings())
	require.Equal(t, Median, o.AggregationTypeForCounter([]byte("testTimerMedianTypeString")))
	require.Equal(t, Median, o.AggregationTypeForTimer([]byte("testTimerMedianTypeString")))
	require.Equal(t, Median, o.AggregationTypeForGauge([]byte("testTimerMedianTypeString")))
	require.NoError(t, o.Validate())
}

func TestOptionsSetTimerQuantileTypeStringFn(t *testing.T) {
	fn := func(q float64) []byte { return []byte(fmt.Sprintf("%1.2f", q)) }
	o := NewAggregationTypesOptions().SetTimerQuantileTypeStringFn(fn)
	require.Equal(t, []byte("0.96"), o.TimerQuantileTypeStringFn()(0.9582))
	validateQuantiles(t, o)
}

func TestOptionsCounterTypeString(t *testing.T) {
	o := NewAggregationTypesOptions()
	require.Equal(t, []byte(defaultLastTypeString), o.TypeStringForCounter(Last))
	require.Equal(t, []byte(defaultMinTypeString), o.TypeStringForCounter(Min))
	require.Equal(t, []byte(defaultMaxTypeString), o.TypeStringForCounter(Max))
	require.Equal(t, []byte(defaultMeanTypeString), o.TypeStringForCounter(Mean))
	require.Equal(t, []byte(defaultMedianTypeString), o.TypeStringForCounter(Median))
	require.Equal(t, []byte(defaultCountTypeString), o.TypeStringForCounter(Count))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Sum))
	require.Equal(t, []byte(defaultSumSqTypeString), o.TypeStringForCounter(SumSq))
	require.Equal(t, []byte(defaultStdevTypeString), o.TypeStringForCounter(Stdev))
}

func TestOptionsTimerTypeString(t *testing.T) {
	o := NewAggregationTypesOptions()
	require.Equal(t, []byte(defaultLastTypeString), o.TypeStringForTimer(Last))
	require.Equal(t, []byte(defaultMinTypeString), o.TypeStringForTimer(Min))
	require.Equal(t, []byte(defaultMaxTypeString), o.TypeStringForTimer(Max))
	require.Equal(t, []byte(defaultMeanTypeString), o.TypeStringForTimer(Mean))
	require.Equal(t, []byte(defaultMedianTypeString), o.TypeStringForTimer(Median))
	require.Equal(t, []byte(defaultCountTypeString), o.TypeStringForTimer(Count))
	require.Equal(t, []byte(defaultSumTypeString), o.TypeStringForTimer(Sum))
	require.Equal(t, []byte(defaultSumSqTypeString), o.TypeStringForTimer(SumSq))
	require.Equal(t, []byte(defaultStdevTypeString), o.TypeStringForTimer(Stdev))
}

func TestOptionsGaugeTypeString(t *testing.T) {
	o := NewAggregationTypesOptions()
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Last))
	require.Equal(t, []byte(defaultMinTypeString), o.TypeStringForGauge(Min))
	require.Equal(t, []byte(defaultMaxTypeString), o.TypeStringForGauge(Max))
	require.Equal(t, []byte(defaultMeanTypeString), o.TypeStringForGauge(Mean))
	require.Equal(t, []byte(defaultMedianTypeString), o.TypeStringForGauge(Median))
	require.Equal(t, []byte(defaultCountTypeString), o.TypeStringForGauge(Count))
	require.Equal(t, []byte(defaultSumTypeString), o.TypeStringForGauge(Sum))
	require.Equal(t, []byte(defaultSumSqTypeString), o.TypeStringForGauge(SumSq))
	require.Equal(t, []byte(defaultStdevTypeString), o.TypeStringForGauge(Stdev))
}

func TestOptionsTypeStringTransform(t *testing.T) {
	o := NewAggregationTypesOptions()
	for _, aggType := range o.DefaultTimerAggregationTypes() {
		require.False(t, strings.HasPrefix(string(o.TypeStringForTimer(aggType)), "."))
	}

	o = o.SetGlobalTypeStringTransformFn(suffixTransformFn)
	for _, aggType := range o.DefaultTimerAggregationTypes() {
		require.True(t, strings.HasPrefix(string(o.TypeStringForTimer(aggType)), "."))
	}

	o = o.SetTimerTypeStringOverrides(map[AggregationType][]byte{P95: []byte("no_dot")})
	require.Equal(t, []byte("no_dot"), o.TypeStringForTimer(P95))
}

func TestOptionTimerQuantileTypeString(t *testing.T) {
	o := NewAggregationTypesOptions()
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
		require.Equal(t, c.b, o.TimerQuantileTypeStringFn()(c.quantile))
	}
}

func TestSetQuantilesPool(t *testing.T) {
	p := pool.NewFloatsPool(nil, nil)
	o := NewAggregationTypesOptions().SetQuantilesPool(p)
	require.Equal(t, p, o.QuantilesPool())
}

func TestSetCounterTypeStringOverride(t *testing.T) {
	m := map[AggregationType][]byte{
		Sum:  nil,
		Mean: []byte("test"),
	}

	o := NewAggregationTypesOptions().SetCounterTypeStringOverrides(m)
	require.Equal(t, [][]byte{nil}, o.DefaultCounterAggregationTypeStrings())
	require.Equal(t, []byte("test"), o.TypeStringForCounter(Mean))
	require.Equal(t, []byte(defaultCountTypeString), o.TypeStringForCounter(Count))
	require.NoError(t, o.Validate())
}

func TestSetCounterTypeStringOverrideDuplicate(t *testing.T) {
	m := map[AggregationType][]byte{
		Sum:  nil,
		Mean: []byte("test"),
		Max:  nil,
	}

	o := NewAggregationTypesOptions().SetCounterTypeStringOverrides(m)
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Sum))
	require.Equal(t, []byte(nil), o.TypeStringForCounter(Max))
	require.Error(t, o.Validate())
}

func TestSetTimerTypeStringOverride(t *testing.T) {
	m := map[AggregationType][]byte{
		Min:  []byte(defaultMinTypeString),
		Max:  []byte(defaultMaxTypeString),
		Mean: []byte("test"),
	}

	o := NewAggregationTypesOptions().SetTimerTypeStringOverrides(m)
	require.Equal(t, []byte("test"), o.TypeStringForTimer(Mean))
	require.Equal(t, []byte(defaultCountTypeString), o.TypeStringForTimer(Count))
	require.Equal(t, []byte(defaultMinTypeString), o.TypeStringForTimer(Min))
	require.Equal(t, []byte(defaultMaxTypeString), o.TypeStringForTimer(Max))
	require.NoError(t, o.Validate())
}

func TestSetTimerTypeStringOverrideDuplicate(t *testing.T) {
	m := map[AggregationType][]byte{
		Min:  []byte(defaultMinTypeString),
		Max:  []byte(defaultMaxTypeString),
		Mean: []byte("test"),
		Sum:  []byte("test"),
	}

	o := NewAggregationTypesOptions().SetTimerTypeStringOverrides(m)
	require.Equal(t, []byte("test"), o.TypeStringForTimer(Mean))
	require.Equal(t, []byte("test"), o.TypeStringForTimer(Sum))
	require.Error(t, o.Validate())
}

func TestSetGaugeTypeStringOverride(t *testing.T) {
	m := map[AggregationType][]byte{
		Last: nil,
		Mean: []byte("test"),
	}

	o := NewAggregationTypesOptions().SetGaugeTypeStringOverrides(m)
	require.Equal(t, [][]byte{nil}, o.DefaultGaugeAggregationTypeStrings())
	require.Equal(t, []byte("test"), o.TypeStringForGauge(Mean))
	require.Equal(t, []byte(nil), o.TypeStringForGauge(Last))
	require.Equal(t, []byte(defaultCountTypeString), o.TypeStringForGauge(Count))
	require.NoError(t, o.Validate())
}

func TestSetGaugeTypeStringOverrideDuplicate(t *testing.T) {
	m := map[AggregationType][]byte{
		Last: nil,
		Mean: []byte("test"),
		Max:  []byte("test"),
	}

	o := NewAggregationTypesOptions().SetGaugeTypeStringOverrides(m)
	require.Equal(t, []byte("test"), o.TypeStringForGauge(Mean))
	require.Equal(t, []byte("test"), o.TypeStringForGauge(Max))
	require.Error(t, o.Validate())
}
