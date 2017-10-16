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
	"testing"
	"time"

	"github.com/m3db/m3aggregator/aggregation"
	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testCounterID                 = id.RawID("testCounter")
	testBatchTimerID              = id.RawID("testBatchTimer")
	testGaugeID                   = id.RawID("testGauge")
	testStoragePolicy             = policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour)
	testAggregationTypes          = policy.AggregationTypes{policy.Mean, policy.Sum}
	testAggregationTypesExpensive = policy.AggregationTypes{policy.SumSq}
	testTimerAggregationTypes     = policy.AggregationTypes{policy.SumSq, policy.P99}
	testTimestamps                = []time.Time{
		time.Unix(216, 0), time.Unix(217, 0), time.Unix(221, 0),
	}
	testAlignedStarts = []int64{
		time.Unix(210, 0).UnixNano(), time.Unix(220, 0).UnixNano(), time.Unix(230, 0).UnixNano(),
	}
	testCounter = unaggregated.MetricUnion{
		Type:       unaggregated.CounterType,
		ID:         testCounterID,
		CounterVal: 1234,
	}
	testBatchTimer = unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            testBatchTimerID,
		BatchTimerVal: []float64{1.0, 3.5, 2.2, 6.5, 4.8},
	}
	testGauge = unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
		ID:       testGaugeID,
		GaugeVal: 123.456,
	}
	testOpts = NewOptions()
)

func TestElemBaseID(t *testing.T) {
	e := &elemBase{}
	e.resetSetData(testCounterID, testStoragePolicy, policy.DefaultAggregationTypes, true)
	require.Equal(t, testCounterID, e.ID())
}

func TestElemBaseResetSetData(t *testing.T) {
	e := &elemBase{}
	e.resetSetData(testCounterID, testStoragePolicy, testAggregationTypesExpensive, false)
	require.Equal(t, testCounterID, e.id)
	require.Equal(t, testStoragePolicy, e.sp)
	require.False(t, e.tombstoned)
	require.False(t, e.closed)
	require.False(t, e.useDefaultAggregation)
	require.True(t, e.aggOpts.HasExpensiveAggregations)
}

func TestCounterResetSetData(t *testing.T) {
	opts := NewOptions()
	ce := NewCounterElem(nil, policy.EmptyStoragePolicy, policy.DefaultAggregationTypes, opts)
	require.Equal(t, opts.AggregationTypesOptions().DefaultCounterAggregationTypes(), ce.aggTypes)
	require.True(t, ce.useDefaultAggregation)
	require.False(t, ce.aggOpts.HasExpensiveAggregations)

	sp := policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour)
	ce.ResetSetData(testCounterID, sp, testAggregationTypesExpensive)

	require.Equal(t, testCounterID, ce.id)
	require.Equal(t, sp, ce.sp)
	require.Equal(t, testAggregationTypesExpensive, ce.aggTypes)
	require.False(t, ce.tombstoned)
	require.False(t, ce.closed)
	require.False(t, ce.useDefaultAggregation)
	require.True(t, ce.aggOpts.HasExpensiveAggregations)
}

func TestTimerResetSetData(t *testing.T) {
	opts := NewOptions()
	te := NewTimerElem(nil, policy.EmptyStoragePolicy, policy.DefaultAggregationTypes, opts)
	require.False(t, te.isQuantilesPooled)
	require.True(t, te.aggOpts.HasExpensiveAggregations)
	require.Equal(t, opts.AggregationTypesOptions().DefaultTimerAggregationTypes(), te.aggTypes)
	require.True(t, te.useDefaultAggregation)

	sp := policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour)
	te.ResetSetData(testBatchTimerID, sp, policy.AggregationTypes{policy.Max, policy.P999})

	require.Equal(t, testBatchTimerID, te.id)
	require.Equal(t, sp, te.sp)
	require.Equal(t, policy.AggregationTypes{policy.Max, policy.P999}, te.aggTypes)
	require.False(t, te.tombstoned)
	require.False(t, te.closed)
	require.False(t, te.useDefaultAggregation)
	require.False(t, te.aggOpts.HasExpensiveAggregations)
	require.Equal(t, []float64{0.999}, te.quantiles)
	require.True(t, te.isQuantilesPooled)
}

func TestGaugeResetSetData(t *testing.T) {
	opts := NewOptions()
	ge := NewGaugeElem(nil, policy.EmptyStoragePolicy, policy.DefaultAggregationTypes, opts)
	require.Equal(t, opts.AggregationTypesOptions().DefaultGaugeAggregationTypes(), ge.aggTypes)
	require.True(t, ge.useDefaultAggregation)
	require.False(t, ge.aggOpts.HasExpensiveAggregations)

	sp := policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour)
	ge.ResetSetData(testGaugeID, sp, testAggregationTypesExpensive)

	require.Equal(t, testGaugeID, ge.id)
	require.Equal(t, sp, ge.sp)
	require.Equal(t, testAggregationTypesExpensive, ge.aggTypes)
	require.False(t, ge.tombstoned)
	require.False(t, ge.closed)
	require.False(t, ge.useDefaultAggregation)
	require.True(t, ge.aggOpts.HasExpensiveAggregations)
}

func TestElemBaseMarkAsTombStoned(t *testing.T) {
	e := &elemBase{}
	require.False(t, e.tombstoned)

	// Marking a closed element tombstoned has no impact
	e.closed = true
	e.MarkAsTombstoned()
	require.False(t, e.tombstoned)

	e.closed = false
	e.MarkAsTombstoned()
	require.True(t, e.tombstoned)
}

func TestCounterElemAddMetric(t *testing.T) {
	e := NewCounterElem(testCounterID, testStoragePolicy, policy.DefaultAggregationTypes, testOptions())

	// Add a counter metric
	require.NoError(t, e.AddMetric(testTimestamps[0], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, testCounter.CounterVal, e.values[0].counter.Sum())
	require.Equal(t, int64(1), e.values[0].counter.Count())
	require.Equal(t, int64(0), e.values[0].counter.SumSq())

	// Add the counter metric at slightly different time
	// but still within the same aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[1], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, 2*testCounter.CounterVal, e.values[0].counter.Sum())
	require.Equal(t, int64(2), e.values[0].counter.Count())
	require.Equal(t, int64(0), e.values[0].counter.SumSq())

	// Add the counter metric in the next aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[2], testCounter))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].timeNanos)
	}
	require.Equal(t, testCounter.CounterVal, e.values[1].counter.Sum())
	require.Equal(t, int64(2), e.values[0].counter.Count())
	require.Equal(t, int64(0), e.values[0].counter.SumSq())

	// Adding the counter metric to a closed element results in an error
	e.closed = true
	require.Equal(t, errCounterElemClosed, e.AddMetric(testTimestamps[2], testCounter))
}

func TestCounterElemAddMetricWithCustomAggregation(t *testing.T) {
	e := NewCounterElem(testCounterID, testStoragePolicy, testAggregationTypesExpensive, testOptions())

	// Add a counter metric
	require.NoError(t, e.AddMetric(testTimestamps[0], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, testCounter.CounterVal, e.values[0].counter.Sum())
	require.Equal(t, testCounter.CounterVal, e.values[0].counter.Max())
	require.Equal(t, int64(testCounter.CounterVal*testCounter.CounterVal), e.values[0].counter.SumSq())

	// Add the counter metric at slightly different time
	// but still within the same aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[1], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, 2*testCounter.CounterVal, e.values[0].counter.Sum())
	require.Equal(t, testCounter.CounterVal, e.values[0].counter.Max())

	// Add the counter metric in the next aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[2], testCounter))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].timeNanos)
	}
	require.Equal(t, testCounter.CounterVal, e.values[1].counter.Sum())
	require.Equal(t, testCounter.CounterVal, e.values[0].counter.Max())

	// Adding the counter metric to a closed element results in an error
	e.closed = true
	require.Equal(t, errCounterElemClosed, e.AddMetric(testTimestamps[2], testCounter))
}

func TestCounterElemReadAndDiscard(t *testing.T) {
	e := testCounterElem(policy.DefaultAggregationTypes)

	// Read and discard values before an early-enough time
	fn, res := testAggMetricFn()
	require.False(t, e.Consume(0, fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 2, len(e.values))

	// Read and discard one value
	fn, res = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[1], fn))
	require.Equal(t, expectedAggMetricsForCounter(testAlignedStarts[1], testStoragePolicy, policy.DefaultAggregationTypes), *res)
	require.Equal(t, 1, len(e.values))

	// Read and discard all values
	fn, res = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, expectedAggMetricsForCounter(testAlignedStarts[2], testStoragePolicy, policy.DefaultAggregationTypes), *res)
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values
	e.tombstoned = true
	fn, res = testAggMetricFn()
	require.True(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op
	e.closed = true
	fn, _ = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(e.values))
}

func TestCounterElemReadAndDiscardWithCustomAggregation(t *testing.T) {
	e := testCounterElem(testAggregationTypes)

	// Read and discard values before an early-enough time
	fn, res := testAggMetricFn()
	require.False(t, e.Consume(0, fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 2, len(e.values))

	// Read and discard one value
	fn, res = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[1], fn))
	require.Equal(t, expectedAggMetricsForCounter(testAlignedStarts[1], testStoragePolicy, testAggregationTypes), *res)
	require.Equal(t, 1, len(e.values))

	// Read and discard all values
	fn, res = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, expectedAggMetricsForCounter(testAlignedStarts[2], testStoragePolicy, testAggregationTypes), *res)
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values
	e.tombstoned = true
	fn, res = testAggMetricFn()
	require.True(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op
	e.closed = true
	fn, _ = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(e.values))
}

func TestCounterElemClose(t *testing.T) {
	e := testCounterElem(policy.DefaultAggregationTypes)
	require.False(t, e.closed)

	// Closing the element
	e.Close()

	// Closing a second time should have no impact
	e.Close()

	require.True(t, e.closed)
	require.Nil(t, e.id)
	require.Equal(t, 0, len(e.values))
	require.NotNil(t, e.values)
}

func TestCounterFindOrInsert(t *testing.T) {
	e := NewCounterElem(testCounterID, testStoragePolicy, policy.DefaultAggregationTypes, testOptions())
	inputs := []int64{10, 10, 20, 10, 15}
	expected := []testIndexData{
		{index: 0, data: []int64{10}},
		{index: 0, data: []int64{10}},
		{index: 1, data: []int64{10, 20}},
		{index: 0, data: []int64{10, 20}},
		{index: 1, data: []int64{10, 15, 20}},
	}
	for idx, input := range inputs {
		res, err := e.findOrCreate(input)
		require.NoError(t, err)
		var times []int64
		for _, v := range e.values {
			times = append(times, v.timeNanos)
		}
		require.Equal(t, e.values[expected[idx].index].counter, res)
		require.Equal(t, expected[idx].data, times)
	}
}

func TestTimerElemAddMetric(t *testing.T) {
	e := NewTimerElem(testBatchTimerID, testStoragePolicy, policy.DefaultAggregationTypes, testOptions())

	// Add a timer metric
	require.NoError(t, e.AddMetric(testTimestamps[0], testBatchTimer))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	timer := e.values[0].timer
	require.Equal(t, int64(5), timer.Count())
	require.Equal(t, 18.0, timer.Sum())
	require.Equal(t, 3.5, timer.Quantile(0.5))
	require.Equal(t, 6.5, timer.Quantile(0.95))
	require.Equal(t, 6.5, timer.Quantile(0.99))

	// Add the timer metric at slightly different time
	// but still within the same aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[1], testBatchTimer))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	timer = e.values[0].timer
	require.Equal(t, int64(10), timer.Count())
	require.Equal(t, 36.0, timer.Sum())
	require.Equal(t, 3.5, timer.Quantile(0.5))
	require.Equal(t, 6.5, timer.Quantile(0.95))
	require.Equal(t, 6.5, timer.Quantile(0.99))

	// Add the timer metric in the next aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[2], testBatchTimer))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].timeNanos)
	}
	timer = e.values[1].timer
	require.Equal(t, int64(5), timer.Count())
	require.Equal(t, 18.0, timer.Sum())
	require.Equal(t, 3.5, timer.Quantile(0.5))
	require.Equal(t, 6.5, timer.Quantile(0.95))
	require.Equal(t, 6.5, timer.Quantile(0.99))

	// Adding the timer metric to a closed element results in an error
	e.closed = true
	require.Equal(t, errTimerElemClosed, e.AddMetric(testTimestamps[2], testBatchTimer))
}

func TestTimerElemConsume(t *testing.T) {
	// Set up stream options
	streamOpts, p, numAlloc := testStreamOptions(t, len(testAlignedStarts)-1)

	// Verify the pool is big enough to supply all the streams
	opts := testOptions().SetStreamOptions(streamOpts)
	e := testTimerElem(policy.DefaultAggregationTypes, opts)
	verifyStreamPoolSize(t, p, 0, numAlloc)

	// Read and discard values before an early-enough time
	fn, res := testAggMetricFn()
	require.False(t, e.Consume(0, fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 2, len(e.values))

	// Read and discard one value
	fn, res = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[1], fn))
	require.Equal(t, expectedAggMetricsForTimer(testAlignedStarts[1], testStoragePolicy, policy.DefaultAggregationTypes), *res)
	require.Equal(t, 1, len(e.values))

	// Read and discard all values
	fn, res = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, expectedAggMetricsForTimer(testAlignedStarts[2], testStoragePolicy, policy.DefaultAggregationTypes), *res)
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values
	e.tombstoned = true
	fn, res = testAggMetricFn()
	require.True(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op
	e.closed = true
	fn, _ = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(e.values))

	// Verify the streams have been returned to pool
	verifyStreamPoolSize(t, p, len(testAlignedStarts)-1, numAlloc)
}

func TestTimerElemReadAndDiscardWithCustomAggregation(t *testing.T) {
	// Set up stream options
	streamOpts, p, numAlloc := testStreamOptions(t, len(testAlignedStarts)-1)

	// Verify the pool is big enough to supply all the streams
	opts := testOptions().SetStreamOptions(streamOpts)
	e := testTimerElem(testTimerAggregationTypes, opts)
	verifyStreamPoolSize(t, p, 0, numAlloc)

	// Read and discard values before an early-enough time
	fn, res := testAggMetricFn()
	require.False(t, e.Consume(0, fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 2, len(e.values))

	// Read and discard one value
	fn, res = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[1], fn))
	require.Equal(t, expectedAggMetricsForTimer(testAlignedStarts[1], testStoragePolicy, testTimerAggregationTypes), *res)
	require.Equal(t, 1, len(e.values))

	// Read and discard all values
	fn, res = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, expectedAggMetricsForTimer(testAlignedStarts[2], testStoragePolicy, testTimerAggregationTypes), *res)
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values
	e.tombstoned = true
	fn, res = testAggMetricFn()
	require.True(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op
	e.closed = true
	fn, _ = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(e.values))

	// Verify the streams have been returned to pool
	verifyStreamPoolSize(t, p, len(testAlignedStarts)-1, numAlloc)
}

func TestTimerElemClose(t *testing.T) {
	// Set up stream options
	streamOpts, p, numAlloc := testStreamOptions(t, len(testAlignedStarts)-1)

	// Verify the pool is big enough to supply all the streams
	opts := testOptions().SetStreamOptions(streamOpts)
	e := testTimerElem(policy.DefaultAggregationTypes, opts)
	verifyStreamPoolSize(t, p, 0, numAlloc)

	require.False(t, e.closed)

	// Closing the element
	e.Close()

	// Closing a second time should have no impact
	e.Close()

	require.True(t, e.closed)
	require.Nil(t, e.id)
	require.Equal(t, 0, len(e.values))
	require.NotNil(t, e.values)

	// Verify the streams have been returned to pool
	verifyStreamPoolSize(t, p, len(testAlignedStarts)-1, numAlloc)
}

func TestTimerFindOrInsert(t *testing.T) {
	e := NewTimerElem(testBatchTimerID, testStoragePolicy, policy.DefaultAggregationTypes, testOptions())
	inputs := []int64{10, 10, 20, 10, 15}
	expected := []testIndexData{
		{index: 0, data: []int64{10}},
		{index: 0, data: []int64{10}},
		{index: 1, data: []int64{10, 20}},
		{index: 0, data: []int64{10, 20}},
		{index: 1, data: []int64{10, 15, 20}},
	}
	for idx, input := range inputs {
		res, err := e.findOrCreate(input)
		require.NoError(t, err)
		var times []int64
		for _, v := range e.values {
			times = append(times, v.timeNanos)
		}
		require.Equal(t, e.values[expected[idx].index].timer, res)
		require.Equal(t, expected[idx].data, times)
	}
}

func TestGaugeElemAddMetric(t *testing.T) {
	e := NewGaugeElem(testGaugeID, testStoragePolicy, policy.DefaultAggregationTypes, testOptions())

	// Add a gauge metric
	require.NoError(t, e.AddMetric(testTimestamps[0], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, testGauge.GaugeVal, e.values[0].gauge.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[0].gauge.Sum())
	require.Equal(t, 0.0, e.values[0].gauge.SumSq())

	// Add the gauge metric at slightly different time
	// but still within the same aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[1], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, testGauge.GaugeVal, e.values[0].gauge.Last())
	require.Equal(t, 2*testGauge.GaugeVal, e.values[0].gauge.Sum())
	require.Equal(t, 0.0, e.values[0].gauge.SumSq())

	// Add the gauge metric in the next aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[2], testGauge))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].timeNanos)
	}
	require.Equal(t, testGauge.GaugeVal, e.values[1].gauge.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[1].gauge.Sum())
	require.Equal(t, 0.0, e.values[1].gauge.SumSq())

	// Adding the gauge metric to a closed element results in an error
	e.closed = true
	require.Equal(t, errGaugeElemClosed, e.AddMetric(testTimestamps[2], testGauge))
}

func TestGaugeElemAddMetricWithCustomAggregation(t *testing.T) {
	e := NewGaugeElem(testGaugeID, testStoragePolicy, testAggregationTypesExpensive, testOptions())

	// Add a gauge metric
	require.NoError(t, e.AddMetric(testTimestamps[0], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, testGauge.GaugeVal, e.values[0].gauge.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[0].gauge.Sum())
	require.Equal(t, testGauge.GaugeVal, e.values[0].gauge.Mean())
	require.Equal(t, testGauge.GaugeVal, e.values[0].gauge.Sum())
	require.Equal(t, testGauge.GaugeVal*testGauge.GaugeVal, e.values[0].gauge.SumSq())

	// Add the gauge metric at slightly different time
	// but still within the same aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[1], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, testGauge.GaugeVal, e.values[0].gauge.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[0].gauge.Max())
	require.Equal(t, 2*testGauge.GaugeVal, e.values[0].gauge.Sum())
	require.Equal(t, 2*testGauge.GaugeVal*testGauge.GaugeVal, e.values[0].gauge.SumSq())

	// Add the gauge metric in the next aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[2], testGauge))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].timeNanos)
	}
	require.Equal(t, testGauge.GaugeVal, e.values[1].gauge.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[1].gauge.Max())

	// Adding the gauge metric to a closed element results in an error
	e.closed = true
	require.Equal(t, errGaugeElemClosed, e.AddMetric(testTimestamps[2], testGauge))
}

func TestGaugeElemReadAndDiscard(t *testing.T) {
	e := testGaugeElem(policy.DefaultAggregationTypes)

	// Read and discard values before an early-enough time
	fn, res := testAggMetricFn()
	require.False(t, e.Consume(0, fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 2, len(e.values))

	// Read and discard one value
	fn, res = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[1], fn))
	require.Equal(t, expectedAggMetricsForGauge(testAlignedStarts[1], testStoragePolicy, policy.DefaultAggregationTypes), *res)
	require.Equal(t, 1, len(e.values))

	// Read and discard all values
	fn, res = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, expectedAggMetricsForGauge(testAlignedStarts[2], testStoragePolicy, policy.DefaultAggregationTypes), *res)
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values
	e.tombstoned = true
	fn, res = testAggMetricFn()
	require.True(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 0, len(e.values))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op
	e.closed = true
	fn, _ = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(e.values))
}

func TestGaugeElemReadAndDiscardWithCustomAggregation(t *testing.T) {
	e := testGaugeElem(testAggregationTypes)

	// Read and discard values before an early-enough time
	fn, res := testAggMetricFn()
	require.False(t, e.Consume(0, fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 2, len(e.values))

	// Read and discard one value
	fn, res = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[1], fn))
	require.Equal(t, expectedAggMetricsForGauge(testAlignedStarts[1], testStoragePolicy, testAggregationTypes), *res)
	require.Equal(t, 1, len(e.values))

	// Read and discard all values
	fn, res = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, expectedAggMetricsForGauge(testAlignedStarts[2], testStoragePolicy, testAggregationTypes), *res)
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values
	e.tombstoned = true
	fn, res = testAggMetricFn()
	require.True(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 0, len(e.values))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op
	e.closed = true
	fn, _ = testAggMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(e.values))
}

func TestGaugeElemClose(t *testing.T) {
	e := testGaugeElem(policy.DefaultAggregationTypes)
	require.False(t, e.closed)

	// Closing the element
	e.Close()

	// Closing a second time should have no impact
	e.Close()

	require.True(t, e.closed)
	require.Nil(t, e.id)
	require.Equal(t, 0, len(e.values))
	require.NotNil(t, e.values)
}

func TestGaugeFindOrInsert(t *testing.T) {
	e := NewGaugeElem(testGaugeID, testStoragePolicy, policy.DefaultAggregationTypes, testOptions())
	inputs := []int64{10, 10, 20, 10, 15}
	expected := []testIndexData{
		{index: 0, data: []int64{10}},
		{index: 0, data: []int64{10}},
		{index: 1, data: []int64{10, 20}},
		{index: 0, data: []int64{10, 20}},
		{index: 1, data: []int64{10, 15, 20}},
	}
	for idx, input := range inputs {
		res, err := e.findOrCreate(input)
		require.NoError(t, err)
		var times []int64
		for _, v := range e.values {
			times = append(times, v.timeNanos)
		}
		require.Equal(t, e.values[expected[idx].index].gauge, res)
		require.Equal(t, expected[idx].data, times)
	}
}

type testIndexData struct {
	index int
	data  []int64
}

type testSuffixAndValue struct {
	aggType policy.AggregationType
	value   float64
}

type testAggMetric struct {
	idPrefix  []byte
	id        id.RawID
	idSuffix  []byte
	timeNanos int64
	value     float64
	sp        policy.StoragePolicy
}

func testAggMetricFn() (aggMetricFn, *[]testAggMetric) {
	var result []testAggMetric
	return func(
		idPrefix []byte,
		id id.RawID,
		idSuffix []byte,
		timeNanos int64,
		value float64,
		sp policy.StoragePolicy,
	) {
		result = append(result, testAggMetric{
			idPrefix:  idPrefix,
			id:        id,
			idSuffix:  idSuffix,
			timeNanos: timeNanos,
			value:     value,
			sp:        sp,
		})
	}, &result
}

func testStreamOptions(t *testing.T, size int) (cm.Options, cm.StreamPool, *int) {
	var numAlloc int
	p := cm.NewStreamPool(pool.NewObjectPoolOptions().SetSize(size))
	streamOpts := cm.NewOptions().SetStreamPool(p)
	p.Init(func() cm.Stream {
		numAlloc++
		return cm.NewStream(nil, streamOpts)
	})
	require.Equal(t, numAlloc, len(testAlignedStarts)-1)
	return streamOpts, p, &numAlloc
}

func testCounterElem(aggTypes policy.AggregationTypes) *CounterElem {
	e := NewCounterElem(testCounterID, testStoragePolicy, aggTypes, testOptions())
	for _, aligned := range testAlignedStarts[:len(testAlignedStarts)-1] {
		counter := aggregation.NewLockedCounter(aggregation.NewCounter(e.aggOpts))
		counter.Update(testCounter.CounterVal)
		e.values = append(e.values, timedCounter{
			timeNanos: aligned,
			counter:   counter,
		})
	}
	return e
}

func testTimerElem(aggTypes policy.AggregationTypes, opts Options) *TimerElem {
	e := NewTimerElem(testBatchTimerID, testStoragePolicy, aggTypes, opts)
	for _, aligned := range testAlignedStarts[:len(testAlignedStarts)-1] {
		newTimer := aggregation.NewTimer(opts.AggregationTypesOptions().TimerQuantiles(), opts.StreamOptions(), e.aggOpts)
		timer := aggregation.NewLockedTimer(newTimer)
		timer.AddBatch(testBatchTimer.BatchTimerVal)
		e.values = append(e.values, timedTimer{
			timeNanos: aligned,
			timer:     timer,
		})
	}
	return e
}

func testGaugeElem(aggTypes policy.AggregationTypes) *GaugeElem {
	e := NewGaugeElem(testGaugeID, testStoragePolicy, aggTypes, testOptions())
	for _, aligned := range testAlignedStarts[:len(testAlignedStarts)-1] {
		gauge := aggregation.NewLockedGauge(aggregation.NewGauge(e.aggOpts))
		gauge.Update(testGauge.GaugeVal)
		e.values = append(e.values, timedGauge{
			timeNanos: aligned,
			gauge:     gauge,
		})
	}
	return e
}

func expectCounterSuffix(aggType policy.AggregationType) []byte {
	return testOpts.AggregationTypesOptions().SuffixForCounter(aggType)
}

func expectTimerSuffix(aggType policy.AggregationType) []byte {
	return testOpts.AggregationTypesOptions().SuffixForTimer(aggType)
}

func expectGaugeSuffix(aggType policy.AggregationType) []byte {
	return testOpts.AggregationTypesOptions().SuffixForGauge(aggType)
}

func expectedAggMetricsForCounter(
	timeNanos int64,
	sp policy.StoragePolicy,
	aggTypes policy.AggregationTypes,
) []testAggMetric {
	if !aggTypes.IsDefault() {
		var res []testAggMetric
		for _, aggType := range aggTypes {
			res = append(res, testAggMetric{
				idPrefix:  []byte("stats.counts."),
				id:        testCounterID,
				idSuffix:  expectCounterSuffix(aggType),
				timeNanos: timeNanos,
				value:     float64(testCounter.CounterVal),
				sp:        sp,
			})
		}
		return res
	}
	return []testAggMetric{
		{
			idPrefix:  []byte("stats.counts."),
			id:        testCounterID,
			idSuffix:  nil,
			timeNanos: timeNanos,
			value:     float64(testCounter.CounterVal),
			sp:        sp,
		},
	}
}

func expectedAggMetricsForTimer(
	timeNanos int64,
	sp policy.StoragePolicy,
	aggTypes policy.AggregationTypes,
) []testAggMetric {
	// this needs to be a list as the order of the result matters in some test
	data := []testSuffixAndValue{
		{policy.Sum, 18.0},
		{policy.SumSq, 83.38},
		{policy.Mean, 3.6},
		{policy.Min, 1.0},
		{policy.Max, 6.5},
		{policy.Count, 5.0},
		{policy.Stdev, 2.15522620622523},
		{policy.Median, 3.5},
		{policy.P50, 3.5},
		{policy.P95, 6.5},
		{policy.P99, 6.5},
	}
	var expected []testAggMetric
	if !aggTypes.IsDefault() {
		for _, aggType := range aggTypes {
			for _, d := range data {
				if d.aggType == aggType {
					expected = append(expected, testAggMetric{
						idPrefix:  []byte("stats.timers."),
						id:        testBatchTimerID,
						idSuffix:  expectTimerSuffix(aggType),
						timeNanos: timeNanos,
						value:     d.value,
						sp:        sp,
					})
				}
			}
		}
		return expected
	}

	for _, d := range data {
		expected = append(expected, testAggMetric{
			idPrefix:  []byte("stats.timers."),
			id:        testBatchTimerID,
			idSuffix:  expectTimerSuffix(d.aggType),
			timeNanos: timeNanos,
			value:     d.value,
			sp:        sp,
		})
	}
	return expected
}

func expectedAggMetricsForGauge(
	timeNanos int64,
	sp policy.StoragePolicy,
	aggTypes policy.AggregationTypes,
) []testAggMetric {
	if !aggTypes.IsDefault() {
		var res []testAggMetric
		for _, aggType := range aggTypes {
			res = append(res, testAggMetric{
				idPrefix:  []byte("stats.gauges."),
				id:        testGaugeID,
				idSuffix:  expectGaugeSuffix(aggType),
				timeNanos: timeNanos,
				value:     float64(testGauge.GaugeVal),
				sp:        sp,
			})
		}
		return res
	}
	return []testAggMetric{
		{
			idPrefix:  []byte("stats.gauges."),
			id:        testGaugeID,
			idSuffix:  nil,
			timeNanos: timeNanos,
			value:     float64(testGauge.GaugeVal),
			sp:        sp,
		},
	}
}

func verifyStreamPoolSize(t *testing.T, p cm.StreamPool, expected int, numAlloc *int) {
	*numAlloc = 0
	for i := 0; i < expected; i++ {
		p.Get()
	}
	require.Equal(t, 0, *numAlloc)
	p.Get()
	require.Equal(t, 1, *numAlloc)
}
