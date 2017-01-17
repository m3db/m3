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
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testID     = metric.ID("foo")
	testPolicy = policy.Policy{
		Resolution: policy.Resolution{Window: 10 * time.Second, Precision: xtime.Second},
		Retention:  policy.Retention(6 * time.Hour),
	}
	testTimestamps = []time.Time{
		time.Unix(216, 0), time.Unix(217, 0), time.Unix(221, 0),
	}
	testAlignedStarts = []time.Time{
		time.Unix(210, 0), time.Unix(220, 0), time.Unix(230, 0),
	}
	testCounter = unaggregated.MetricUnion{
		Type:       unaggregated.CounterType,
		ID:         testID,
		CounterVal: 1234,
	}
	testBatchTimer = unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            testID,
		BatchTimerVal: []float64{1.0, 3.5, 2.2, 6.5, 4.8},
	}
	testGauge = unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
		ID:       testID,
		GaugeVal: 123.456,
	}
	testInvalidMetric = unaggregated.MetricUnion{
		Type: unaggregated.UnknownType,
		ID:   testID,
	}
)

type testIndexData struct {
	index int
	data  []int64
}

type testSuffixAndValue struct {
	suffix []byte
	value  float64
}

type testAggMetric struct {
	idPrefix  []byte
	id        metric.ID
	idSuffix  []byte
	timestamp time.Time
	value     float64
	policy    policy.Policy
}

type testAggMetricsByTimeAscending []testAggMetric

func (m testAggMetricsByTimeAscending) Len() int      { return len(m) }
func (m testAggMetricsByTimeAscending) Swap(i, j int) { m[i], m[j] = m[j], m[i] }

func (m testAggMetricsByTimeAscending) Less(i, j int) bool {
	return m[i].timestamp.Before(m[j].timestamp)
}

func testAggMetricFn() (aggMetricFn, *[]testAggMetric) {
	var result []testAggMetric
	return func(
		idPrefix []byte,
		id metric.ID,
		idSuffix []byte,
		timestamp time.Time,
		value float64,
		policy policy.Policy,
	) {
		result = append(result, testAggMetric{
			idPrefix:  idPrefix,
			id:        id,
			idSuffix:  idSuffix,
			timestamp: timestamp,
			value:     value,
			policy:    policy,
		})
	}, &result
}

func testStreamOptions(t *testing.T, size int) (cm.Options, cm.StreamPool, *int) {
	var numAlloc int
	p := cm.NewStreamPool(pool.NewObjectPoolOptions().SetSize(size))
	streamOpts := cm.NewOptions().SetStreamPool(p)
	p.Init(func() cm.Stream {
		numAlloc++
		return cm.NewStream(streamOpts)
	})
	require.Equal(t, numAlloc, len(testAlignedStarts)-1)
	return streamOpts, p, &numAlloc
}

func testOptions() Options {
	return NewOptions()
}

func testCounterElem() *CounterElem {
	e := NewCounterElem(testID, testPolicy, testOptions())
	for _, aligned := range testAlignedStarts[:len(testAlignedStarts)-1] {
		counter := aggregation.NewCounter()
		counter.Add(testCounter.CounterVal)
		e.values = append(e.values, timedCounter{
			timeNs:  aligned.UnixNano(),
			counter: counter,
		})
	}
	return e
}

func testTimerElem(opts Options) *TimerElem {
	e := NewTimerElem(testID, testPolicy, opts)
	for _, aligned := range testAlignedStarts[:len(testAlignedStarts)-1] {
		timer := aggregation.NewTimer(opts.StreamOptions())
		timer.AddBatch(testBatchTimer.BatchTimerVal)
		e.values = append(e.values, timedTimer{
			timeNs: aligned.UnixNano(),
			timer:  timer,
		})
	}
	return e
}

func testGaugeElem() *GaugeElem {
	e := NewGaugeElem(testID, testPolicy, testOptions())
	for _, aligned := range testAlignedStarts[:len(testAlignedStarts)-1] {
		gauge := aggregation.NewGauge()
		gauge.Add(testGauge.GaugeVal)
		e.values = append(e.values, timedGauge{
			timeNs: aligned.UnixNano(),
			gauge:  gauge,
		})
	}
	return e
}

func expectedAggMetricsForCounter(
	timestamp time.Time,
	policy policy.Policy,
) []testAggMetric {
	return []testAggMetric{
		{
			idPrefix:  []byte("stats.counts."),
			id:        testID,
			idSuffix:  nil,
			timestamp: timestamp,
			value:     float64(testCounter.CounterVal),
			policy:    policy,
		},
	}
}

func expectedAggMetricsForTimer(
	timestamp time.Time,
	policy policy.Policy,
) []testAggMetric {
	data := []testSuffixAndValue{
		{[]byte(".sum"), 18.0},
		{[]byte(".sum_sq"), 83.38},
		{[]byte(".mean"), 3.6},
		{[]byte(".lower"), 1.0},
		{[]byte(".upper"), 6.5},
		{[]byte(".count"), 5.0},
		{[]byte(".stdev"), 2.15522620622523},
		{[]byte(".median"), 3.5},
		{[]byte(".p50"), 3.5},
		{[]byte(".p95"), 6.5},
		{[]byte(".p99"), 6.5},
	}
	var expected []testAggMetric
	for _, d := range data {
		expected = append(expected, testAggMetric{
			idPrefix:  []byte("stats.timers."),
			id:        testID,
			idSuffix:  d.suffix,
			timestamp: timestamp,
			value:     d.value,
			policy:    policy,
		})
	}
	return expected
}

func expectedAggMetricsForGauge(
	timestamp time.Time,
	policy policy.Policy,
) []testAggMetric {
	return []testAggMetric{
		{
			idPrefix:  []byte("stats.gauges."),
			id:        testID,
			idSuffix:  nil,
			timestamp: timestamp,
			value:     float64(testGauge.GaugeVal),
			policy:    policy,
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

func TestElemBaseResetSetData(t *testing.T) {
	e := &elemBase{}
	e.ResetSetData(testID, testPolicy)
	require.Equal(t, testID, e.id)
	require.Equal(t, testPolicy, e.policy)
	require.False(t, e.tombstoned)
	require.False(t, e.closed)
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
	e := NewCounterElem(testID, testPolicy, testOptions())

	// Add a counter metric
	require.NoError(t, e.AddMetric(testTimestamps[0], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0].UnixNano(), e.values[0].timeNs)
	require.Equal(t, testCounter.CounterVal, e.values[0].counter.Sum())

	// Add the counter metric at slightly different time
	// but still within the same aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[1], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0].UnixNano(), e.values[0].timeNs)
	require.Equal(t, 2*testCounter.CounterVal, e.values[0].counter.Sum())

	// Add the counter metric in the next aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[2], testCounter))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i].UnixNano(), e.values[i].timeNs)
	}
	require.Equal(t, testCounter.CounterVal, e.values[1].counter.Sum())

	// Adding the counter metric to a closed element results in an error
	e.closed = true
	require.Equal(t, errCounterElemClosed, e.AddMetric(testTimestamps[2], testCounter))
}

func TestCounterElemReadAndDiscard(t *testing.T) {
	e := testCounterElem()

	// Read and discard values before an early-enough time
	fn, res := testAggMetricFn()
	require.False(t, e.ReadAndDiscard(time.Unix(0, 0), fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 2, len(e.values))

	// Read and discard one value
	fn, res = testAggMetricFn()
	require.False(t, e.ReadAndDiscard(testAlignedStarts[1], fn))
	require.Equal(t, expectedAggMetricsForCounter(testAlignedStarts[1], testPolicy), *res)
	require.Equal(t, 1, len(e.values))

	// Read and discard all values
	fn, res = testAggMetricFn()
	require.False(t, e.ReadAndDiscard(testAlignedStarts[2], fn))
	require.Equal(t, expectedAggMetricsForCounter(testAlignedStarts[2], testPolicy), *res)
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values
	e.tombstoned = true
	fn, res = testAggMetricFn()
	require.True(t, e.ReadAndDiscard(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op
	e.closed = true
	fn, res = testAggMetricFn()
	require.False(t, e.ReadAndDiscard(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(e.values))
}

func TestCounterElemClose(t *testing.T) {
	e := testCounterElem()
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
	e := NewCounterElem(testID, testPolicy, testOptions())
	inputs := []int64{10, 10, 20, 10, 15}
	expected := []testIndexData{
		{index: 0, data: []int64{10}},
		{index: 0, data: []int64{10}},
		{index: 1, data: []int64{10, 20}},
		{index: 0, data: []int64{10, 20}},
		{index: 1, data: []int64{10, 15, 20}},
	}
	for idx, input := range inputs {
		res := e.findOrInsertWithLock(input)
		var times []int64
		for _, v := range e.values {
			times = append(times, v.timeNs)
		}
		require.Equal(t, expected[idx].index, res)
		require.Equal(t, expected[idx].data, times)
	}
}

func TestTimerElemAddMetric(t *testing.T) {
	e := NewTimerElem(testID, testPolicy, testOptions())

	// Add a timer metric
	require.NoError(t, e.AddMetric(testTimestamps[0], testBatchTimer))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0].UnixNano(), e.values[0].timeNs)
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
	require.Equal(t, testAlignedStarts[0].UnixNano(), e.values[0].timeNs)
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
		require.Equal(t, testAlignedStarts[i].UnixNano(), e.values[i].timeNs)
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

func TestTimerElemReadAndDiscard(t *testing.T) {
	// Set up stream options
	streamOpts, p, numAlloc := testStreamOptions(t, len(testAlignedStarts)-1)

	// Verify the pool is big enough to supply all the streams
	opts := testOptions().SetStreamOptions(streamOpts)
	e := testTimerElem(opts)
	verifyStreamPoolSize(t, p, 0, numAlloc)

	// Read and discard values before an early-enough time
	fn, res := testAggMetricFn()
	require.False(t, e.ReadAndDiscard(time.Unix(0, 0), fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 2, len(e.values))

	// Read and discard one value
	fn, res = testAggMetricFn()
	require.False(t, e.ReadAndDiscard(testAlignedStarts[1], fn))
	require.Equal(t, expectedAggMetricsForTimer(testAlignedStarts[1], testPolicy), *res)
	require.Equal(t, 1, len(e.values))

	// Read and discard all values
	fn, res = testAggMetricFn()
	require.False(t, e.ReadAndDiscard(testAlignedStarts[2], fn))
	require.Equal(t, expectedAggMetricsForTimer(testAlignedStarts[2], testPolicy), *res)
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values
	e.tombstoned = true
	fn, res = testAggMetricFn()
	require.True(t, e.ReadAndDiscard(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op
	e.closed = true
	fn, res = testAggMetricFn()
	require.False(t, e.ReadAndDiscard(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(e.values))

	// Verify the streams have been returned to pool
	verifyStreamPoolSize(t, p, len(testAlignedStarts)-1, numAlloc)
}

func TestTimerElemClose(t *testing.T) {
	// Set up stream options
	streamOpts, p, numAlloc := testStreamOptions(t, len(testAlignedStarts)-1)

	// Verify the pool is big enough to supply all the streams
	opts := testOptions().SetStreamOptions(streamOpts)
	e := testTimerElem(opts)
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
	e := NewTimerElem(testID, testPolicy, testOptions())
	inputs := []int64{10, 10, 20, 10, 15}
	expected := []testIndexData{
		{index: 0, data: []int64{10}},
		{index: 0, data: []int64{10}},
		{index: 1, data: []int64{10, 20}},
		{index: 0, data: []int64{10, 20}},
		{index: 1, data: []int64{10, 15, 20}},
	}
	for idx, input := range inputs {
		res := e.findOrInsertWithLock(input)
		var times []int64
		for _, v := range e.values {
			times = append(times, v.timeNs)
		}
		require.Equal(t, expected[idx].index, res)
		require.Equal(t, expected[idx].data, times)
	}
}

func TestGaugeElemAddMetric(t *testing.T) {
	e := NewGaugeElem(testID, testPolicy, testOptions())

	// Add a gauge metric
	require.NoError(t, e.AddMetric(testTimestamps[0], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0].UnixNano(), e.values[0].timeNs)
	require.Equal(t, testGauge.GaugeVal, e.values[0].gauge.Value())

	// Add the gauge metric at slightly different time
	// but still within the same aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[1], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0].UnixNano(), e.values[0].timeNs)
	require.Equal(t, testGauge.GaugeVal, e.values[0].gauge.Value())

	// Add the gauge metric in the next aggregation interval
	require.NoError(t, e.AddMetric(testTimestamps[2], testGauge))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i].UnixNano(), e.values[i].timeNs)
	}
	require.Equal(t, testGauge.GaugeVal, e.values[1].gauge.Value())

	// Adding the gauge metric to a closed element results in an error
	e.closed = true
	require.Equal(t, errGaugeElemClosed, e.AddMetric(testTimestamps[2], testGauge))
}

func TestGaugeElemReadAndDiscard(t *testing.T) {
	e := testGaugeElem()

	// Read and discard values before an early-enough time
	fn, res := testAggMetricFn()
	require.False(t, e.ReadAndDiscard(time.Unix(0, 0), fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 2, len(e.values))

	// Read and discard one value
	fn, res = testAggMetricFn()
	require.False(t, e.ReadAndDiscard(testAlignedStarts[1], fn))
	require.Equal(t, expectedAggMetricsForGauge(testAlignedStarts[1], testPolicy), *res)
	require.Equal(t, 1, len(e.values))

	// Read and discard all values
	fn, res = testAggMetricFn()
	require.False(t, e.ReadAndDiscard(testAlignedStarts[2], fn))
	require.Equal(t, expectedAggMetricsForGauge(testAlignedStarts[2], testPolicy), *res)
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values
	e.tombstoned = true
	fn, res = testAggMetricFn()
	require.True(t, e.ReadAndDiscard(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(*res))
	require.Equal(t, 0, len(e.values))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op
	e.closed = true
	fn, res = testAggMetricFn()
	require.False(t, e.ReadAndDiscard(testAlignedStarts[2], fn))
	require.Equal(t, 0, len(e.values))
}

func TestGaugeElemClose(t *testing.T) {
	e := testGaugeElem()
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
	e := NewGaugeElem(testID, testPolicy, testOptions())
	inputs := []int64{10, 10, 20, 10, 15}
	expected := []testIndexData{
		{index: 0, data: []int64{10}},
		{index: 0, data: []int64{10}},
		{index: 1, data: []int64{10, 20}},
		{index: 0, data: []int64{10, 20}},
		{index: 1, data: []int64{10, 15, 20}},
	}
	for idx, input := range inputs {
		res := e.findOrInsertWithLock(input)
		var times []int64
		for _, v := range e.values {
			times = append(times, v.timeNs)
		}
		require.Equal(t, expected[idx].index, res)
		require.Equal(t, expected[idx].data, times)
	}
}
