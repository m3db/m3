// Copyright (c) 2016 Uber Technologies, Inc.
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

package aggregator

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func validateDerivedPrefix(
	t *testing.T,
	expected []byte,
	part1 []byte,
	part2 []byte,
) {
	full := make([]byte, len(part1)+len(part2))
	n := copy(full, part1)
	copy(full[n:], part2)
	require.Equal(t, expected, full)
}

func validateQuantiles(t *testing.T, o Options) {
	suffixFn := o.TimerQuantileSuffixFn()
	quantiles, _ := o.DefaultTimerAggregationTypes().PooledQuantiles(nil)
	require.Equal(t, o.TimerQuantiles(), quantiles)

	for _, aggType := range o.DefaultTimerAggregationTypes() {
		q, ok := aggType.Quantile()
		if !ok || aggType == policy.Median {
			continue
		}
		require.Equal(t, suffixFn(q), o.SuffixForTimer(aggType))
	}
}

func TestOptionsValidateDefault(t *testing.T) {
	o := NewOptions()

	// Validate base options
	require.Equal(t, defaultDefaultCounterAggregationTypes, o.DefaultCounterAggregationTypes())
	require.Equal(t, defaultDefaultTimerAggregationTypes, o.DefaultTimerAggregationTypes())
	require.Equal(t, defaultDefaultGaugeAggregationTypes, o.DefaultGaugeAggregationTypes())
	require.Equal(t, defaultMetricPrefix, o.MetricPrefix())
	require.Equal(t, defaultCounterPrefix, o.CounterPrefix())
	require.Equal(t, defaultTimerPrefix, o.TimerPrefix())
	require.Equal(t, defaultAggregationSumSuffix, o.AggregationSumSuffix())
	require.Equal(t, defaultAggregationSumSqSuffix, o.AggregationSumSqSuffix())
	require.Equal(t, defaultAggregationMeanSuffix, o.AggregationMeanSuffix())
	require.Equal(t, defaultAggregationMinSuffix, o.AggregationMinSuffix())
	require.Equal(t, defaultAggregationMaxSuffix, o.AggregationMaxSuffix())
	require.Equal(t, defaultAggregationCountSuffix, o.AggregationCountSuffix())
	require.Equal(t, defaultAggregationStdevSuffix, o.AggregationStdevSuffix())
	require.Equal(t, defaultAggregationMedianSuffix, o.AggregationMedianSuffix())
	require.Equal(t, defaultGaugePrefix, o.GaugePrefix())
	require.Equal(t, defaultMinFlushInterval, o.MinFlushInterval())
	require.Equal(t, defaultEntryTTL, o.EntryTTL())
	require.Equal(t, defaultEntryCheckInterval, o.EntryCheckInterval())
	require.Equal(t, defaultEntryCheckBatchPercent, o.EntryCheckBatchPercent())
	require.NotNil(t, o.TimerQuantileSuffixFn())
	require.NotNil(t, o.ClockOptions())
	require.NotNil(t, o.InstrumentOptions())
	require.NotNil(t, o.TimeLock())
	require.NotNil(t, o.StreamOptions())
	require.NotNil(t, o.EntryPool())
	require.NotNil(t, o.CounterElemPool())
	require.NotNil(t, o.TimerElemPool())
	require.NotNil(t, o.GaugeElemPool())

	// Validate derived options
	validateDerivedPrefix(t, o.FullCounterPrefix(), o.MetricPrefix(), o.CounterPrefix())
	validateDerivedPrefix(t, o.FullTimerPrefix(), o.MetricPrefix(), o.TimerPrefix())
	validateDerivedPrefix(t, o.FullGaugePrefix(), o.MetricPrefix(), o.GaugePrefix())
	validateQuantiles(t, o)
	require.Equal(t, [][]byte{nil}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(".sum"), []byte(".sum_sq"), []byte(".mean"), []byte(".lower"), []byte(".upper"), []byte(".count"),
		[]byte(".stdev"), []byte(".median"), []byte(".p50"), []byte(".p95"), []byte(".p99")}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{nil}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetDefaultCounterAggregationTypes(t *testing.T) {
	aggTypes := policy.AggregationTypes{policy.Mean, policy.SumSq}
	o := NewOptions().SetDefaultCounterAggregationTypes(aggTypes)
	require.Equal(t, aggTypes, o.DefaultCounterAggregationTypes())
	require.Equal(t, [][]byte{[]byte(".mean"), []byte(".sum_sq")}, o.DefaultCounterAggregationSuffixes())
}

func TestOptionsSetDefaultTimerAggregationTypes(t *testing.T) {
	aggTypes := policy.AggregationTypes{policy.Mean, policy.SumSq, policy.P99, policy.P9999}
	o := NewOptions().SetDefaultTimerAggregationTypes(aggTypes)
	require.Equal(t, aggTypes, o.DefaultTimerAggregationTypes())
	require.Equal(t, []float64{0.99, 0.9999}, o.TimerQuantiles())
	require.Equal(t, [][]byte{[]byte(".mean"), []byte(".sum_sq"), []byte(".p99"), []byte(".p99.99")}, o.DefaultTimerAggregationSuffixes())
}

func TestOptionsSetDefaultGaugeAggregationTypes(t *testing.T) {
	aggTypes := policy.AggregationTypes{policy.Mean, policy.SumSq}
	o := NewOptions().SetDefaultGaugeAggregationTypes(aggTypes)
	require.Equal(t, aggTypes, o.DefaultGaugeAggregationTypes())
	require.Equal(t, [][]byte{[]byte(".mean"), []byte(".sum_sq")}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetMetricPrefix(t *testing.T) {
	newPrefix := []byte("testMetricPrefix")
	o := NewOptions().SetMetricPrefix(newPrefix)
	require.Equal(t, newPrefix, o.MetricPrefix())
	validateDerivedPrefix(t, o.FullCounterPrefix(), o.MetricPrefix(), o.CounterPrefix())
	validateDerivedPrefix(t, o.FullTimerPrefix(), o.MetricPrefix(), o.TimerPrefix())
	validateDerivedPrefix(t, o.FullGaugePrefix(), o.MetricPrefix(), o.GaugePrefix())
}

func TestOptionsSetCounterPrefix(t *testing.T) {
	newPrefix := []byte("testCounterPrefix")
	o := NewOptions().SetCounterPrefix(newPrefix)
	require.Equal(t, newPrefix, o.CounterPrefix())
	validateDerivedPrefix(t, o.FullCounterPrefix(), o.MetricPrefix(), o.CounterPrefix())
}

func TestOptionsSetTimerPrefix(t *testing.T) {
	newPrefix := []byte("testTimerPrefix")
	o := NewOptions().SetTimerPrefix(newPrefix)
	require.Equal(t, newPrefix, o.TimerPrefix())
	validateDerivedPrefix(t, o.FullTimerPrefix(), o.MetricPrefix(), o.TimerPrefix())
}

func TestOptionsSetTimerSumSuffix(t *testing.T) {
	newSumSuffix := []byte("testTimerSumSuffix")
	o := NewOptions().
		SetDefaultCounterAggregationTypes(policy.AggregationTypes{policy.Sum}).
		SetDefaultTimerAggregationTypes(policy.AggregationTypes{policy.Sum}).
		SetDefaultGaugeAggregationTypes(policy.AggregationTypes{policy.Sum}).
		SetAggregationSumSuffix(newSumSuffix)
	require.Equal(t, newSumSuffix, o.AggregationSumSuffix())
	require.Equal(t, []byte(nil), o.SuffixForCounter(policy.Sum))
	require.Equal(t, newSumSuffix, o.SuffixForTimer(policy.Sum))
	require.Equal(t, newSumSuffix, o.SuffixForGauge(policy.Sum))
	require.Equal(t, [][]byte{[]byte(nil)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newSumSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newSumSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerSumSqSuffix(t *testing.T) {
	newSumSqSuffix := []byte("testTimerSumSqSuffix")
	o := NewOptions().
		SetDefaultCounterAggregationTypes(policy.AggregationTypes{policy.SumSq}).
		SetDefaultTimerAggregationTypes(policy.AggregationTypes{policy.SumSq}).
		SetDefaultGaugeAggregationTypes(policy.AggregationTypes{policy.SumSq}).
		SetAggregationSumSqSuffix(newSumSqSuffix)
	require.Equal(t, newSumSqSuffix, o.AggregationSumSqSuffix())
	require.Equal(t, newSumSqSuffix, o.SuffixForCounter(policy.SumSq))
	require.Equal(t, newSumSqSuffix, o.SuffixForTimer(policy.SumSq))
	require.Equal(t, newSumSqSuffix, o.SuffixForGauge(policy.SumSq))
	require.Equal(t, [][]byte{[]byte(newSumSqSuffix)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newSumSqSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newSumSqSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerMeanSuffix(t *testing.T) {
	newMeanSuffix := []byte("testTimerMeanSuffix")
	o := NewOptions().
		SetDefaultCounterAggregationTypes(policy.AggregationTypes{policy.Mean}).
		SetDefaultTimerAggregationTypes(policy.AggregationTypes{policy.Mean}).
		SetDefaultGaugeAggregationTypes(policy.AggregationTypes{policy.Mean}).
		SetAggregationMeanSuffix(newMeanSuffix)
	require.Equal(t, newMeanSuffix, o.AggregationMeanSuffix())
	require.Equal(t, newMeanSuffix, o.SuffixForCounter(policy.Mean))
	require.Equal(t, newMeanSuffix, o.SuffixForTimer(policy.Mean))
	require.Equal(t, newMeanSuffix, o.SuffixForGauge(policy.Mean))
	require.Equal(t, [][]byte{[]byte(newMeanSuffix)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newMeanSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newMeanSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerMinSuffix(t *testing.T) {
	newMinSuffix := []byte("testTimerMinSuffix")
	o := NewOptions().
		SetDefaultCounterAggregationTypes(policy.AggregationTypes{policy.Min}).
		SetDefaultTimerAggregationTypes(policy.AggregationTypes{policy.Min}).
		SetDefaultGaugeAggregationTypes(policy.AggregationTypes{policy.Min}).
		SetAggregationMinSuffix(newMinSuffix)
	require.Equal(t, newMinSuffix, o.AggregationMinSuffix())
	require.Equal(t, newMinSuffix, o.SuffixForCounter(policy.Min))
	require.Equal(t, []byte(".lower"), o.SuffixForTimer(policy.Min))
	require.Equal(t, newMinSuffix, o.SuffixForGauge(policy.Min))
	require.Equal(t, [][]byte{[]byte(newMinSuffix)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(".lower")}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newMinSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerMaxSuffix(t *testing.T) {
	newMaxSuffix := []byte("testTimerMaxSuffix")
	o := NewOptions().
		SetDefaultCounterAggregationTypes(policy.AggregationTypes{policy.Max}).
		SetDefaultTimerAggregationTypes(policy.AggregationTypes{policy.Max}).
		SetDefaultGaugeAggregationTypes(policy.AggregationTypes{policy.Max}).
		SetAggregationMaxSuffix(newMaxSuffix)
	require.Equal(t, newMaxSuffix, o.AggregationMaxSuffix())
	require.Equal(t, newMaxSuffix, o.SuffixForCounter(policy.Max))
	require.Equal(t, []byte(".upper"), o.SuffixForTimer(policy.Max))
	require.Equal(t, newMaxSuffix, o.SuffixForGauge(policy.Max))
	require.Equal(t, [][]byte{[]byte(newMaxSuffix)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(".upper")}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newMaxSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerCountSuffix(t *testing.T) {
	newCountSuffix := []byte("testTimerCountSuffix")
	o := NewOptions().
		SetDefaultCounterAggregationTypes(policy.AggregationTypes{policy.Count}).
		SetDefaultTimerAggregationTypes(policy.AggregationTypes{policy.Count}).
		SetDefaultGaugeAggregationTypes(policy.AggregationTypes{policy.Count}).
		SetAggregationCountSuffix(newCountSuffix)
	require.Equal(t, newCountSuffix, o.AggregationCountSuffix())
	require.Equal(t, newCountSuffix, o.SuffixForCounter(policy.Count))
	require.Equal(t, newCountSuffix, o.SuffixForTimer(policy.Count))
	require.Equal(t, newCountSuffix, o.SuffixForGauge(policy.Count))
	require.Equal(t, [][]byte{[]byte(newCountSuffix)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newCountSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newCountSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerStdevSuffix(t *testing.T) {
	newStdevSuffix := []byte("testTimerStdevSuffix")
	o := NewOptions().
		SetDefaultCounterAggregationTypes(policy.AggregationTypes{policy.Stdev}).
		SetDefaultTimerAggregationTypes(policy.AggregationTypes{policy.Stdev}).
		SetDefaultGaugeAggregationTypes(policy.AggregationTypes{policy.Stdev}).
		SetAggregationStdevSuffix(newStdevSuffix)
	require.Equal(t, newStdevSuffix, o.AggregationStdevSuffix())
	require.Equal(t, newStdevSuffix, o.SuffixForCounter(policy.Stdev))
	require.Equal(t, newStdevSuffix, o.SuffixForTimer(policy.Stdev))
	require.Equal(t, newStdevSuffix, o.SuffixForGauge(policy.Stdev))
	require.Equal(t, [][]byte{[]byte(newStdevSuffix)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newStdevSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newStdevSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerMedianSuffix(t *testing.T) {
	newMedianSuffix := []byte("testTimerMedianSuffix")
	o := NewOptions().
		SetDefaultCounterAggregationTypes(policy.AggregationTypes{policy.Median}).
		SetDefaultTimerAggregationTypes(policy.AggregationTypes{policy.Median}).
		SetDefaultGaugeAggregationTypes(policy.AggregationTypes{policy.Median}).
		SetAggregationMedianSuffix(newMedianSuffix)
	require.Equal(t, newMedianSuffix, o.AggregationMedianSuffix())
	require.Equal(t, newMedianSuffix, o.SuffixForCounter(policy.Median))
	require.Equal(t, newMedianSuffix, o.SuffixForTimer(policy.Median))
	require.Equal(t, newMedianSuffix, o.SuffixForGauge(policy.Median))
	require.Equal(t, [][]byte{[]byte(newMedianSuffix)}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newMedianSuffix)}, o.DefaultTimerAggregationSuffixes())
	require.Equal(t, [][]byte{[]byte(newMedianSuffix)}, o.DefaultGaugeAggregationSuffixes())
}

func TestOptionsSetTimerQuantileSuffixFn(t *testing.T) {
	fn := func(q float64) []byte { return []byte(fmt.Sprintf("%1.2f", q)) }
	o := NewOptions().SetTimerQuantileSuffixFn(fn)
	require.Equal(t, []byte("0.96"), o.TimerQuantileSuffixFn()(0.9582))
	validateQuantiles(t, o)
}

func TestOptionsCounterSuffix(t *testing.T) {
	o := NewOptions()
	require.Equal(t, []byte(".last"), o.SuffixForCounter(policy.Last))
	require.Equal(t, []byte(".min"), o.SuffixForCounter(policy.Min))
	require.Equal(t, []byte(".max"), o.SuffixForCounter(policy.Max))
	require.Equal(t, []byte(".mean"), o.SuffixForCounter(policy.Mean))
	require.Equal(t, []byte(".median"), o.SuffixForCounter(policy.Median))
	require.Equal(t, []byte(".count"), o.SuffixForCounter(policy.Count))
	require.Equal(t, []byte(nil), o.SuffixForCounter(policy.Sum))
	require.Equal(t, []byte(".sum_sq"), o.SuffixForCounter(policy.SumSq))
	require.Equal(t, []byte(".stdev"), o.SuffixForCounter(policy.Stdev))
}

func TestOptionsTimerSuffix(t *testing.T) {
	o := NewOptions()
	require.Equal(t, []byte(".last"), o.SuffixForTimer(policy.Last))
	require.Equal(t, []byte(".lower"), o.SuffixForTimer(policy.Min))
	require.Equal(t, []byte(".upper"), o.SuffixForTimer(policy.Max))
	require.Equal(t, []byte(".mean"), o.SuffixForTimer(policy.Mean))
	require.Equal(t, []byte(".median"), o.SuffixForTimer(policy.Median))
	require.Equal(t, []byte(".count"), o.SuffixForTimer(policy.Count))
	require.Equal(t, []byte(".sum"), o.SuffixForTimer(policy.Sum))
	require.Equal(t, []byte(".sum_sq"), o.SuffixForTimer(policy.SumSq))
	require.Equal(t, []byte(".stdev"), o.SuffixForTimer(policy.Stdev))
}

func TestOptionsGaugeSuffix(t *testing.T) {
	o := NewOptions()
	require.Equal(t, []byte(nil), o.SuffixForGauge(policy.Last))
	require.Equal(t, []byte(".min"), o.SuffixForGauge(policy.Min))
	require.Equal(t, []byte(".max"), o.SuffixForGauge(policy.Max))
	require.Equal(t, []byte(".mean"), o.SuffixForGauge(policy.Mean))
	require.Equal(t, []byte(".median"), o.SuffixForGauge(policy.Median))
	require.Equal(t, []byte(".count"), o.SuffixForGauge(policy.Count))
	require.Equal(t, []byte(".sum"), o.SuffixForGauge(policy.Sum))
	require.Equal(t, []byte(".sum_sq"), o.SuffixForGauge(policy.SumSq))
	require.Equal(t, []byte(".stdev"), o.SuffixForGauge(policy.Stdev))
}

func TestOptionTimerQuantileSuffix(t *testing.T) {
	o := NewOptions()
	require.Equal(t, []byte(".p1"), o.TimerQuantileSuffixFn()(0.01))
	require.Equal(t, []byte(".p10"), o.TimerQuantileSuffixFn()(0.1))
	require.Equal(t, []byte(".p50"), o.TimerQuantileSuffixFn()(0.5))
	require.Equal(t, []byte(".p90"), o.TimerQuantileSuffixFn()(0.9))
	require.Equal(t, []byte(".p90"), o.TimerQuantileSuffixFn()(0.90))
	require.Equal(t, []byte(".p90.9"), o.TimerQuantileSuffixFn()(0.909))
	require.Equal(t, []byte(".p99.9"), o.TimerQuantileSuffixFn()(0.999))
	require.Equal(t, []byte(".p99.9"), o.TimerQuantileSuffixFn()(0.9990))
	require.Equal(t, []byte(".p99.99"), o.TimerQuantileSuffixFn()(0.9999))
	require.Equal(t, []byte(".p0.001"), o.TimerQuantileSuffixFn()(0.00001))
	require.Equal(t, []byte(".p12.3456789"), o.TimerQuantileSuffixFn()(0.123456789))
}

func TestOptionsSetGaugePrefix(t *testing.T) {
	newPrefix := []byte("testGaugePrefix")
	o := NewOptions().SetGaugePrefix(newPrefix)
	require.Equal(t, newPrefix, o.GaugePrefix())
	validateDerivedPrefix(t, o.FullGaugePrefix(), o.MetricPrefix(), o.GaugePrefix())
}

func TestSetClockOptions(t *testing.T) {
	value := clock.NewOptions()
	o := NewOptions().SetClockOptions(value)
	require.Equal(t, value, o.ClockOptions())
}

func TestSetInstrumentOptions(t *testing.T) {
	value := instrument.NewOptions()
	o := NewOptions().SetInstrumentOptions(value)
	require.Equal(t, value, o.InstrumentOptions())
}

func TestSetStreamOptions(t *testing.T) {
	value := cm.NewOptions()
	o := NewOptions().SetStreamOptions(value)
	require.Equal(t, value, o.StreamOptions())
	validateQuantiles(t, o)
}

func TestSetTimeLock(t *testing.T) {
	value := &sync.RWMutex{}
	o := NewOptions().SetTimeLock(value)
	require.Equal(t, value, o.TimeLock())
}

func TestSetMinFlushInterval(t *testing.T) {
	value := time.Second * 15
	o := NewOptions().SetMinFlushInterval(value)
	require.Equal(t, value, o.MinFlushInterval())
}

func TestSetFlushHandler(t *testing.T) {
	h := &mockHandler{}
	o := NewOptions().SetFlushHandler(h)
	require.Equal(t, h, o.FlushHandler())
}

func TestSetEntryTTL(t *testing.T) {
	value := time.Minute
	o := NewOptions().SetEntryTTL(value)
	require.Equal(t, value, o.EntryTTL())
}

func TestSetEntryCheckInterval(t *testing.T) {
	value := time.Minute
	o := NewOptions().SetEntryCheckInterval(value)
	require.Equal(t, value, o.EntryCheckInterval())
}

func TestSetEntryCheckBatchPercent(t *testing.T) {
	value := 0.05
	o := NewOptions().SetEntryCheckBatchPercent(value)
	require.Equal(t, value, o.EntryCheckBatchPercent())
}

func TestSetEntryPool(t *testing.T) {
	value := NewEntryPool(nil)
	o := NewOptions().SetEntryPool(value)
	require.Equal(t, value, o.EntryPool())
}

func TestSetCounterElemPool(t *testing.T) {
	value := NewCounterElemPool(nil)
	o := NewOptions().SetCounterElemPool(value)
	require.Equal(t, value, o.CounterElemPool())
}

func TestSetTimerElemPool(t *testing.T) {
	value := NewTimerElemPool(nil)
	o := NewOptions().SetTimerElemPool(value)
	require.Equal(t, value, o.TimerElemPool())
}

func TestSetGaugeElemPool(t *testing.T) {
	value := NewGaugeElemPool(nil)
	o := NewOptions().SetGaugeElemPool(value)
	require.Equal(t, value, o.GaugeElemPool())
}

func TestSetQuantilesPool(t *testing.T) {
	p := pool.NewFloatsPool(nil, nil)
	o := NewOptions().SetQuantilesPool(p)
	require.Equal(t, p, o.QuantilesPool())
}

func TestSetCounterSuffixOverride(t *testing.T) {
	m := map[policy.AggregationType][]byte{
		policy.Sum:  nil,
		policy.Mean: []byte("test"),
	}

	o := NewOptions().SetCounterSuffixOverride(m)
	require.Equal(t, [][]byte{nil}, o.DefaultCounterAggregationSuffixes())
	require.Equal(t, []byte("test"), o.SuffixForCounter(policy.Mean))
	require.Equal(t, []byte(".count"), o.SuffixForCounter(policy.Count))
}

func TestSetTimerSuffixOverride(t *testing.T) {
	m := map[policy.AggregationType][]byte{
		policy.Min:  []byte(".lower"),
		policy.Max:  []byte(".upper"),
		policy.Mean: []byte("test"),
	}

	o := NewOptions().SetTimerSuffixOverride(m)
	require.Equal(t, []byte("test"), o.SuffixForTimer(policy.Mean))
	require.Equal(t, []byte(".count"), o.SuffixForTimer(policy.Count))
	require.Equal(t, []byte(".lower"), o.SuffixForTimer(policy.Min))
	require.Equal(t, []byte(".upper"), o.SuffixForTimer(policy.Max))
}

func TestSetGaugeSuffixOverride(t *testing.T) {
	m := map[policy.AggregationType][]byte{
		policy.Last: nil,
		policy.Mean: []byte("test"),
	}

	o := NewOptions().SetGaugeSuffixOverride(m)
	require.Equal(t, [][]byte{nil}, o.DefaultGaugeAggregationSuffixes())
	require.Equal(t, []byte("test"), o.SuffixForGauge(policy.Mean))
	require.Equal(t, []byte(nil), o.SuffixForGauge(policy.Last))
	require.Equal(t, []byte(".count"), o.SuffixForGauge(policy.Count))
}
