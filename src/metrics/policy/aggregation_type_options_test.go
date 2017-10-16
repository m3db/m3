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
	require.Equal(t, defaultAggregationSumSuffix, o.SumSuffix())
	require.Equal(t, defaultAggregationSumSqSuffix, o.SumSqSuffix())
	require.Equal(t, defaultAggregationMeanSuffix, o.MeanSuffix())
	require.Equal(t, defaultAggregationMinSuffix, o.MinSuffix())
	require.Equal(t, defaultAggregationMaxSuffix, o.MaxSuffix())
	require.Equal(t, defaultAggregationCountSuffix, o.CountSuffix())
	require.Equal(t, defaultAggregationStdevSuffix, o.StdevSuffix())
	require.Equal(t, defaultAggregationMedianSuffix, o.MedianSuffix())
	require.NotNil(t, o.TimerQuantileSuffixFn())

	// Validate derived options
	validateQuantiles(t, o)
	require.Equal(t, [][]byte{nil}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(".sum"), []byte(".sum_sq"), []byte(".mean"), []byte(".lower"), []byte(".upper"), []byte(".count"),
		[]byte(".stdev"), []byte(".median"), []byte(".p50"), []byte(".p95"), []byte(".p99")}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{nil}, o.DefaultGaugeAggregationSuffixes())
}

func validateQuantiles(t *testing.T, o AggregationTypesOptions) {
	suffixFn := o.TimerQuantileSuffixFn()
	quantiles, _ := o.DefaultTimerAggregationTypes().PooledQuantiles(nil)
	require.Equal(t, o.TimerQuantiles(), quantiles)

	for _, aggType := range o.DefaultTimerAggregationTypes() {
		q, ok := aggType.Quantile()
		if !ok || aggType == Median {
			continue
		}
		require.Equal(t, suffixFn(q), o.SuffixForTimer(aggType))
	}
}

func TestOptionsSetDefaultCounterAggregationTypes(t *testing.T) {
	aggTypes := AggregationTypes{Mean, SumSq}
	o := NewAggregationTypesOptions().SetDefaultCounterAggregationTypes(aggTypes)
	require.Equal(t, aggTypes, o.DefaultCounterAggregationTypes())
	require.Equal(t, [][]byte{[]byte(".mean"), []byte(".sum_sq")}, o.DefaultCounterAggregationSuffixes())
}

func TestOptionsSetDefaultTimerAggregationTypes(t *testing.T) {
	aggTypes := AggregationTypes{Mean, SumSq, P99, P9999}
	o := NewAggregationTypesOptions().SetDefaultTimerAggregationTypes(aggTypes)
	require.Equal(t, aggTypes, o.DefaultTimerAggregationTypes())
	require.Equal(t, []float64{0.99, 0.9999}, o.TimerQuantiles())
	require.Equal(t, [][]byte{[]byte(".mean"), []byte(".sum_sq"), []byte(".p99"), []byte(".p9999")}, o.DefaultTimerAggregationSuffixes())
}

func TestOptionsSetDefaultGaugeAggregationTypes(t *testing.T) {
	aggTypes := AggregationTypes{Mean, SumSq}
	o := NewAggregationTypesOptions().SetDefaultGaugeAggregationTypes(aggTypes)
	require.Equal(t, aggTypes, o.DefaultGaugeAggregationTypes())
	require.Equal(t, [][]byte{[]byte(".mean"), []byte(".sum_sq")}, o.DefaultGaugeAggregationSuffixes())
}
func TestOptionsSetTimerSumSuffix(t *testing.T) {
	newSumSuffix := []byte("testTimerSumSuffix")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{Sum}).
		SetDefaultTimerAggregationTypes(AggregationTypes{Sum}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{Sum}).
		SetSumSuffix(newSumSuffix)
	require.Equal(t, newSumSuffix, o.SumSuffix())
	require.Equal(t, []byte(nil), o.SuffixForCounter(Sum))
	require.Equal(t, newSumSuffix, o.SuffixForTimer(Sum))
	require.Equal(t, newSumSuffix, o.SuffixForGauge(Sum))
	require.Equal(t, [][]byte{[]byte(nil)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newSumSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newSumSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerSumSqSuffix(t *testing.T) {
	newSumSqSuffix := []byte("testTimerSumSqSuffix")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{SumSq}).
		SetDefaultTimerAggregationTypes(AggregationTypes{SumSq}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{SumSq}).
		SetSumSqSuffix(newSumSqSuffix)
	require.Equal(t, newSumSqSuffix, o.SumSqSuffix())
	require.Equal(t, newSumSqSuffix, o.SuffixForCounter(SumSq))
	require.Equal(t, newSumSqSuffix, o.SuffixForTimer(SumSq))
	require.Equal(t, newSumSqSuffix, o.SuffixForGauge(SumSq))
	require.Equal(t, [][]byte{[]byte(newSumSqSuffix)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newSumSqSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newSumSqSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerMeanSuffix(t *testing.T) {
	newMeanSuffix := []byte("testTimerMeanSuffix")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{Mean}).
		SetDefaultTimerAggregationTypes(AggregationTypes{Mean}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{Mean}).
		SetMeanSuffix(newMeanSuffix)
	require.Equal(t, newMeanSuffix, o.MeanSuffix())
	require.Equal(t, newMeanSuffix, o.SuffixForCounter(Mean))
	require.Equal(t, newMeanSuffix, o.SuffixForTimer(Mean))
	require.Equal(t, newMeanSuffix, o.SuffixForGauge(Mean))
	require.Equal(t, [][]byte{[]byte(newMeanSuffix)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newMeanSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newMeanSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetCounterSumSuffix(t *testing.T) {
	newSumSuffix := []byte("testSumSuffix")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{Sum}).
		SetDefaultTimerAggregationTypes(AggregationTypes{Sum}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{Sum}).
		SetSumSuffix(newSumSuffix)
	require.Equal(t, newSumSuffix, o.SumSuffix())
	require.Equal(t, []byte(nil), o.SuffixForCounter(Sum))
	require.Equal(t, newSumSuffix, o.SuffixForTimer(Sum))
	require.Equal(t, newSumSuffix, o.SuffixForGauge(Sum))
	require.Equal(t, [][]byte{[]byte(nil)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newSumSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newSumSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetGaugeLastSuffix(t *testing.T) {
	newLastSuffix := []byte("testLastSuffix")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{Last}).
		SetDefaultTimerAggregationTypes(AggregationTypes{Last}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{Last}).
		SetLastSuffix(newLastSuffix)
	require.Equal(t, newLastSuffix, o.LastSuffix())
	require.Equal(t, newLastSuffix, o.SuffixForCounter(Last))
	require.Equal(t, newLastSuffix, o.SuffixForTimer(Last))
	require.Equal(t, []byte(nil), o.SuffixForGauge(Last))
	require.Equal(t, [][]byte{[]byte(newLastSuffix)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newLastSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(nil)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerCountSuffix(t *testing.T) {
	newCountSuffix := []byte("testTimerCountSuffix")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{Count}).
		SetDefaultTimerAggregationTypes(AggregationTypes{Count}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{Count}).
		SetCountSuffix(newCountSuffix)
	require.Equal(t, newCountSuffix, o.CountSuffix())
	require.Equal(t, newCountSuffix, o.SuffixForCounter(Count))
	require.Equal(t, newCountSuffix, o.SuffixForTimer(Count))
	require.Equal(t, newCountSuffix, o.SuffixForGauge(Count))
	require.Equal(t, [][]byte{[]byte(newCountSuffix)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newCountSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newCountSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerStdevSuffix(t *testing.T) {
	newStdevSuffix := []byte("testTimerStdevSuffix")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{Stdev}).
		SetDefaultTimerAggregationTypes(AggregationTypes{Stdev}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{Stdev}).
		SetStdevSuffix(newStdevSuffix)
	require.Equal(t, newStdevSuffix, o.StdevSuffix())
	require.Equal(t, newStdevSuffix, o.SuffixForCounter(Stdev))
	require.Equal(t, newStdevSuffix, o.SuffixForTimer(Stdev))
	require.Equal(t, newStdevSuffix, o.SuffixForGauge(Stdev))
	require.Equal(t, [][]byte{[]byte(newStdevSuffix)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newStdevSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newStdevSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerMedianSuffix(t *testing.T) {
	newMedianSuffix := []byte("testTimerMedianSuffix")
	o := NewAggregationTypesOptions().
		SetDefaultCounterAggregationTypes(AggregationTypes{Median}).
		SetDefaultTimerAggregationTypes(AggregationTypes{Median}).
		SetDefaultGaugeAggregationTypes(AggregationTypes{Median}).
		SetMedianSuffix(newMedianSuffix)
	require.Equal(t, newMedianSuffix, o.MedianSuffix())
	require.Equal(t, newMedianSuffix, o.SuffixForCounter(Median))
	require.Equal(t, newMedianSuffix, o.SuffixForTimer(Median))
	require.Equal(t, newMedianSuffix, o.SuffixForGauge(Median))
	require.Equal(t, [][]byte{[]byte(newMedianSuffix)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newMedianSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newMedianSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerQuantileSuffixFn(t *testing.T) {
	fn := func(q float64) []byte { return []byte(fmt.Sprintf("%1.2f", q)) }
	o := NewAggregationTypesOptions().SetTimerQuantileSuffixFn(fn)
	require.Equal(t, []byte("0.96"), o.TimerQuantileSuffixFn()(0.9582))
	validateQuantiles(t, o)
}

func TestOptionsCounterSuffix(t *testing.T) {
	o := NewAggregationTypesOptions()
	require.Equal(t, []byte(".last"), o.SuffixForCounter(Last))
	require.Equal(t, []byte(".lower"), o.SuffixForCounter(Min))
	require.Equal(t, []byte(".upper"), o.SuffixForCounter(Max))
	require.Equal(t, []byte(".mean"), o.SuffixForCounter(Mean))
	require.Equal(t, []byte(".median"), o.SuffixForCounter(Median))
	require.Equal(t, []byte(".count"), o.SuffixForCounter(Count))
	require.Equal(t, []byte(nil), o.SuffixForCounter(Sum))
	require.Equal(t, []byte(".sum_sq"), o.SuffixForCounter(SumSq))
	require.Equal(t, []byte(".stdev"), o.SuffixForCounter(Stdev))
}

func TestOptionsTimerSuffix(t *testing.T) {
	o := NewAggregationTypesOptions()
	require.Equal(t, []byte(".last"), o.SuffixForTimer(Last))
	require.Equal(t, []byte(".lower"), o.SuffixForTimer(Min))
	require.Equal(t, []byte(".upper"), o.SuffixForTimer(Max))
	require.Equal(t, []byte(".mean"), o.SuffixForTimer(Mean))
	require.Equal(t, []byte(".median"), o.SuffixForTimer(Median))
	require.Equal(t, []byte(".count"), o.SuffixForTimer(Count))
	require.Equal(t, []byte(".sum"), o.SuffixForTimer(Sum))
	require.Equal(t, []byte(".sum_sq"), o.SuffixForTimer(SumSq))
	require.Equal(t, []byte(".stdev"), o.SuffixForTimer(Stdev))
}

func TestOptionsGaugeSuffix(t *testing.T) {
	o := NewAggregationTypesOptions()
	require.Equal(t, []byte(nil), o.SuffixForGauge(Last))
	require.Equal(t, []byte(".lower"), o.SuffixForGauge(Min))
	require.Equal(t, []byte(".upper"), o.SuffixForGauge(Max))
	require.Equal(t, []byte(".mean"), o.SuffixForGauge(Mean))
	require.Equal(t, []byte(".median"), o.SuffixForGauge(Median))
	require.Equal(t, []byte(".count"), o.SuffixForGauge(Count))
	require.Equal(t, []byte(".sum"), o.SuffixForGauge(Sum))
	require.Equal(t, []byte(".sum_sq"), o.SuffixForGauge(SumSq))
	require.Equal(t, []byte(".stdev"), o.SuffixForGauge(Stdev))
}

func TestOptionTimerQuantileSuffix(t *testing.T) {
	o := NewAggregationTypesOptions()
	cases := []struct {
		quantile float64
		b        []byte
	}{
		{
			quantile: 0.01,
			b:        []byte(".p1"),
		},
		{
			quantile: 0.1,
			b:        []byte(".p10"),
		},
		{
			quantile: 0.5,
			b:        []byte(".p50"),
		},
		{
			quantile: 0.9,
			b:        []byte(".p90"),
		},
		{
			quantile: 0.90,
			b:        []byte(".p90"),
		},
		{
			quantile: 0.90,
			b:        []byte(".p90"),
		},
		{
			quantile: 0.909,
			b:        []byte(".p909"),
		},
		{
			quantile: 0.999,
			b:        []byte(".p999"),
		},
		{
			quantile: 0.9990,
			b:        []byte(".p999"),
		},
		{
			quantile: 0.9999,
			b:        []byte(".p9999"),
		},
		{
			quantile: 0.99995,
			b:        []byte(".p99995"),
		},
		{
			quantile: 0.123,
			b:        []byte(".p123"),
		},
	}

	for _, c := range cases {
		require.Equal(t, c.b, o.TimerQuantileSuffixFn()(c.quantile))
	}
}

func TestSetQuantilesPool(t *testing.T) {
	p := pool.NewFloatsPool(nil, nil)
	o := NewAggregationTypesOptions().SetQuantilesPool(p)
	require.Equal(t, p, o.QuantilesPool())
}

func TestSetCounterSuffixOverride(t *testing.T) {
	m := map[AggregationType][]byte{
		Sum:  nil,
		Mean: []byte("test"),
	}

	o := NewAggregationTypesOptions().SetCounterSuffixOverrides(m)
	require.Equal(t, [][]byte{nil}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, []byte("test"), o.SuffixForCounter(Mean))
	require.Equal(t, []byte(".count"), o.SuffixForCounter(Count))
}

func TestSetTimerSuffixOverride(t *testing.T) {
	m := map[AggregationType][]byte{
		Min:  []byte(".lower"),
		Max:  []byte(".upper"),
		Mean: []byte("test"),
	}

	o := NewAggregationTypesOptions().SetTimerSuffixOverrides(m)
	require.Equal(t, []byte("test"), o.SuffixForTimer(Mean))
	require.Equal(t, []byte(".count"), o.SuffixForTimer(Count))
	require.Equal(t, []byte(".lower"), o.SuffixForTimer(Min))
	require.Equal(t, []byte(".upper"), o.SuffixForTimer(Max))
}

func TestSetGaugeSuffixOverride(t *testing.T) {
	m := map[AggregationType][]byte{
		Last: nil,
		Mean: []byte("test"),
	}

	o := NewAggregationTypesOptions().SetGaugeSuffixOverrides(m)
	require.Equal(t, [][]byte{nil}, o.DefaultGaugeAggregationSuffixes())
	require.Equal(t, []byte("test"), o.SuffixForGauge(Mean))
	require.Equal(t, []byte(nil), o.SuffixForGauge(Last))
	require.Equal(t, []byte(".count"), o.SuffixForGauge(Count))
}
