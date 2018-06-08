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
	"math"
	"testing"
	"time"

	raggregation "github.com/m3db/m3aggregator/aggregation"
	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	maggregation "github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/op"
	"github.com/m3db/m3metrics/op/applied"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/transformation"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testCounterID                 = id.RawID("testCounter")
	testBatchTimerID              = id.RawID("testBatchTimer")
	testGaugeID                   = id.RawID("testGauge")
	testStoragePolicy             = policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour)
	testAggregationTypes          = maggregation.Types{maggregation.Mean, maggregation.Sum}
	testAggregationTypesExpensive = maggregation.Types{maggregation.SumSq}
	testTimerAggregationTypes     = maggregation.Types{maggregation.SumSq, maggregation.P99}
	testCounter                   = unaggregated.MetricUnion{
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
	testPipeline = applied.NewPipeline([]applied.Union{
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.Absolute},
		},
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.PerSecond},
		},
		{
			Type: op.RollupType,
			Rollup: applied.Rollup{
				ID:            []byte("foo.bar"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
			},
		},
		{
			Type: op.RollupType,
			Rollup: applied.Rollup{
				ID:            []byte("foo.baz"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Max),
			},
		},
	})
	testOpts       = NewOptions()
	testTimestamps = []time.Time{
		time.Unix(216, 0), time.Unix(217, 0), time.Unix(221, 0),
	}
	testAlignedStarts = []int64{
		time.Unix(210, 0).UnixNano(), time.Unix(220, 0).UnixNano(), time.Unix(230, 0).UnixNano(),
	}
	testCounterVals    = []int64{testCounter.CounterVal, testCounter.CounterVal}
	testBatchTimerVals = [][]float64{testBatchTimer.BatchTimerVal, testBatchTimer.BatchTimerVal}
	testGaugeVals      = []float64{testGauge.GaugeVal, testGauge.GaugeVal}
)

func TestCounterResetSetData(t *testing.T) {
	opts := NewOptions()
	ce, err := NewCounterElem(nil, policy.EmptyStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, opts)
	require.NoError(t, err)
	require.Equal(t, opts.AggregationTypesOptions().DefaultCounterAggregationTypes(), ce.aggTypes)
	require.True(t, ce.useDefaultAggregation)
	require.False(t, ce.aggOpts.HasExpensiveAggregations)

	// Reset element with a default pipeline.
	err = ce.ResetSetData(testCounterID, testStoragePolicy, testAggregationTypesExpensive, applied.DefaultPipeline)
	require.NoError(t, err)
	require.Equal(t, testCounterID, ce.id)
	require.Equal(t, testStoragePolicy, ce.sp)
	require.Equal(t, testAggregationTypesExpensive, ce.aggTypes)
	require.Equal(t, parsedPipeline{}, ce.parsedPipeline)
	require.False(t, ce.tombstoned)
	require.False(t, ce.closed)
	require.False(t, ce.useDefaultAggregation)
	require.True(t, ce.aggOpts.HasExpensiveAggregations)
	require.Nil(t, ce.lastConsumedValues)

	// Reset element with a pipeline containing a derivative transformation.
	expectedParsedPipeline := parsedPipeline{
		HasDerivativeTransform: true,
		Transformations: applied.NewPipeline([]applied.Union{
			{
				Type:           op.TransformationType,
				Transformation: op.Transformation{Type: transformation.Absolute},
			},
			{
				Type:           op.TransformationType,
				Transformation: op.Transformation{Type: transformation.PerSecond},
			},
		}),
		HasRollup: true,
		Rollup: applied.Rollup{
			ID:            []byte("foo.bar"),
			AggregationID: maggregation.MustCompressTypes(maggregation.Count),
		},
		Remainder: applied.NewPipeline([]applied.Union{
			{
				Type: op.RollupType,
				Rollup: applied.Rollup{
					ID:            []byte("foo.baz"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
		}),
	}
	err = ce.ResetSetData(testCounterID, testStoragePolicy, testAggregationTypesExpensive, testPipeline)
	require.NoError(t, err)
	require.Equal(t, expectedParsedPipeline, ce.parsedPipeline)
	require.Equal(t, len(testAggregationTypesExpensive), len(ce.lastConsumedValues))
	for i := 0; i < len(ce.lastConsumedValues); i++ {
		require.True(t, math.IsNaN(ce.lastConsumedValues[i]))
	}
}

func TestCounterResetSetDataInvalidAggregationType(t *testing.T) {
	opts := NewOptions()
	ce := MustNewCounterElem(nil, policy.EmptyStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, opts)
	err := ce.ResetSetData(testCounterID, testStoragePolicy, maggregation.Types{maggregation.Last}, applied.DefaultPipeline)
	require.Error(t, err)
}

func TestCounterResetSetDataInvalidPipeline(t *testing.T) {
	opts := NewOptions()
	ce := MustNewCounterElem(nil, policy.EmptyStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, opts)

	invalidPipeline := applied.NewPipeline([]applied.Union{
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.Absolute},
		},
	})
	err := ce.ResetSetData(testCounterID, testStoragePolicy, maggregation.DefaultTypes, invalidPipeline)
	require.Error(t, err)
}

func TestCounterElemAddMetric(t *testing.T) {
	e, err := NewCounterElem(testCounterID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, NewOptions())
	require.NoError(t, err)

	// Add a counter metric.
	require.NoError(t, e.AddMetric(testTimestamps[0], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, testCounter.CounterVal, e.values[0].aggregation.Sum())
	require.Equal(t, int64(1), e.values[0].aggregation.Count())
	require.Equal(t, int64(0), e.values[0].aggregation.SumSq())

	// Add the counter metric at slightly different time
	// but still within the same aggregation interval.
	require.NoError(t, e.AddMetric(testTimestamps[1], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, 2*testCounter.CounterVal, e.values[0].aggregation.Sum())
	require.Equal(t, int64(2), e.values[0].aggregation.Count())
	require.Equal(t, int64(0), e.values[0].aggregation.SumSq())

	// Add the counter metric in the next aggregation interval.
	require.NoError(t, e.AddMetric(testTimestamps[2], testCounter))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].timeNanos)
	}
	require.Equal(t, testCounter.CounterVal, e.values[1].aggregation.Sum())
	require.Equal(t, int64(2), e.values[0].aggregation.Count())
	require.Equal(t, int64(0), e.values[0].aggregation.SumSq())

	// Adding the counter metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddMetric(testTimestamps[2], testCounter))
}

func TestCounterElemAddMetricWithCustomAggregation(t *testing.T) {
	e, err := NewCounterElem(testCounterID, testStoragePolicy, testAggregationTypesExpensive, applied.DefaultPipeline, NewOptions())
	require.NoError(t, err)

	// Add a counter metric.
	require.NoError(t, e.AddMetric(testTimestamps[0], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, testCounter.CounterVal, e.values[0].aggregation.Sum())
	require.Equal(t, testCounter.CounterVal, e.values[0].aggregation.Max())
	require.Equal(t, int64(testCounter.CounterVal*testCounter.CounterVal), e.values[0].aggregation.SumSq())

	// Add the counter metric at slightly different time
	// but still within the same aggregation interval.
	require.NoError(t, e.AddMetric(testTimestamps[1], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, 2*testCounter.CounterVal, e.values[0].aggregation.Sum())
	require.Equal(t, testCounter.CounterVal, e.values[0].aggregation.Max())

	// Add the counter metric in the next aggregation interval.
	require.NoError(t, e.AddMetric(testTimestamps[2], testCounter))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].timeNanos)
	}
	require.Equal(t, testCounter.CounterVal, e.values[1].aggregation.Sum())
	require.Equal(t, testCounter.CounterVal, e.values[0].aggregation.Max())

	// Adding the counter metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddMetric(testTimestamps[2], testCounter))
}

func TestCounterElemConsumeDefaultAggregationDefaultPipeline(t *testing.T) {
	e := testCounterElem(testAlignedStarts[:len(testAlignedStarts)-1], testCounterVals, maggregation.DefaultTypes, applied.DefaultPipeline)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardMetricFn()
	require.False(t, e.Consume(0, localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[1], localFn, forwardFn))
	require.Equal(t, expectedLocalMetricsForCounter(testAlignedStarts[1], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 1, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, expectedLocalMetricsForCounter(testAlignedStarts[2], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.True(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))
}

func TestCounterElemConsumeCustomAggregationDefaultPipeline(t *testing.T) {
	e := testCounterElem(testAlignedStarts[:len(testAlignedStarts)-1], testCounterVals, testAggregationTypes, applied.DefaultPipeline)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardMetricFn()
	require.False(t, e.Consume(0, localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[1], localFn, forwardFn))
	require.Equal(t, expectedLocalMetricsForCounter(testAlignedStarts[1], testStoragePolicy, testAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 1, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, expectedLocalMetricsForCounter(testAlignedStarts[2], testStoragePolicy, testAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.True(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))
}

func TestCounterElemConsumeCustomAggregationCustomPipeline(t *testing.T) {
	alignedTimeNanos := []int64{
		time.Unix(210, 0).UnixNano(),
		time.Unix(220, 0).UnixNano(),
		time.Unix(230, 0).UnixNano(),
		time.Unix(240, 0).UnixNano(),
	}
	counterVals := []int64{-123, -456, -589}
	aggregationTypes := maggregation.Types{maggregation.Sum}
	e := testCounterElem(alignedTimeNanos[:3], counterVals, aggregationTypes, testPipeline)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardMetricFn()
	require.False(t, e.Consume(0, localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 3, len(e.values))

	// Consume one value.
	expectedRes := []testForwardMetric{
		{
			metric: aggregated.Metric{
				ID:        id.RawID("foo.bar"),
				TimeNanos: time.Unix(220, 0).UnixNano(),
				Value:     nan,
			},
			meta: metadata.ForwardMetadata{
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
				StoragePolicy: testStoragePolicy,
				Pipeline: applied.NewPipeline([]applied.Union{
					{
						Type: op.RollupType,
						Rollup: applied.Rollup{
							ID:            []byte("foo.baz"),
							AggregationID: maggregation.MustCompressTypes(maggregation.Max),
						},
					},
				}),
			},
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(alignedTimeNanos[1], localFn, forwardFn))
	verifyForwardMetrics(t, expectedRes, *forwardRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 2, len(e.values))
	require.Equal(t, time.Unix(220, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, []float64{123.0}, e.lastConsumedValues)

	// Consume all values.
	expectedRes = []testForwardMetric{
		{
			metric: aggregated.Metric{
				ID:        id.RawID("foo.bar"),
				TimeNanos: time.Unix(230, 0).UnixNano(),
				Value:     33.3,
			},
			meta: metadata.ForwardMetadata{
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
				StoragePolicy: testStoragePolicy,
				Pipeline: applied.NewPipeline([]applied.Union{
					{
						Type: op.RollupType,
						Rollup: applied.Rollup{
							ID:            []byte("foo.baz"),
							AggregationID: maggregation.MustCompressTypes(maggregation.Max),
						},
					},
				}),
			},
		},
		{
			metric: aggregated.Metric{
				ID:        id.RawID("foo.bar"),
				TimeNanos: time.Unix(240, 0).UnixNano(),
				Value:     13.3,
			},
			meta: metadata.ForwardMetadata{
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
				StoragePolicy: testStoragePolicy,
				Pipeline: applied.NewPipeline([]applied.Union{
					{
						Type: op.RollupType,
						Rollup: applied.Rollup{
							ID:            []byte("foo.baz"),
							AggregationID: maggregation.MustCompressTypes(maggregation.Max),
						},
					},
				}),
			},
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(alignedTimeNanos[3], localFn, forwardFn))
	verifyForwardMetrics(t, expectedRes, *forwardRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(e.values))
	require.Equal(t, time.Unix(240, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, []float64{589.0}, e.lastConsumedValues)

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.True(t, e.Consume(alignedTimeNanos[3], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(alignedTimeNanos[3], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))
}

func TestCounterElemClose(t *testing.T) {
	e := testCounterElem(testAlignedStarts[:len(testAlignedStarts)-1], testCounterVals, maggregation.DefaultTypes, applied.DefaultPipeline)
	require.False(t, e.closed)

	// Closing the element.
	e.Close()

	// Closing a second time should have no impact.
	e.Close()

	require.True(t, e.closed)
	require.Nil(t, e.id)
	require.Equal(t, parsedPipeline{}, e.parsedPipeline)
	require.Equal(t, 0, len(e.values))
	require.Equal(t, 0, len(e.toConsume))
	require.Equal(t, 0, len(e.lastConsumedValues))
	require.NotNil(t, e.values)
}

func TestCounterFindOrInsert(t *testing.T) {
	e, err := NewCounterElem(testCounterID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, NewOptions())
	require.NoError(t, err)

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
		require.Equal(t, e.values[expected[idx].index].aggregation, res)
		require.Equal(t, expected[idx].data, times)
	}
}

func TestTimerResetSetData(t *testing.T) {
	opts := NewOptions()
	te, err := NewTimerElem(nil, policy.EmptyStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, opts)
	require.NoError(t, err)
	require.Nil(t, te.quantilesPool)
	require.NotNil(t, te.quantiles)
	require.True(t, te.aggOpts.HasExpensiveAggregations)
	require.Equal(t, opts.AggregationTypesOptions().DefaultTimerAggregationTypes(), te.aggTypes)
	require.True(t, te.useDefaultAggregation)

	// Reset element with a default pipeline.
	err = te.ResetSetData(testBatchTimerID, testStoragePolicy, maggregation.Types{maggregation.Max, maggregation.P999}, applied.DefaultPipeline)
	require.NoError(t, err)
	require.Equal(t, testBatchTimerID, te.id)
	require.Equal(t, testStoragePolicy, te.sp)
	require.Equal(t, maggregation.Types{maggregation.Max, maggregation.P999}, te.aggTypes)
	require.Equal(t, parsedPipeline{}, te.parsedPipeline)
	require.False(t, te.tombstoned)
	require.False(t, te.closed)
	require.False(t, te.useDefaultAggregation)
	require.False(t, te.aggOpts.HasExpensiveAggregations)
	require.Equal(t, []float64{0.999}, te.quantiles)
	require.NotNil(t, te.quantilesPool)
	require.Nil(t, te.lastConsumedValues)

	// Reset element with a pipeline containing a derivative transformation.
	expectedParsedPipeline := parsedPipeline{
		HasDerivativeTransform: true,
		Transformations: applied.NewPipeline([]applied.Union{
			{
				Type:           op.TransformationType,
				Transformation: op.Transformation{Type: transformation.Absolute},
			},
			{
				Type:           op.TransformationType,
				Transformation: op.Transformation{Type: transformation.PerSecond},
			},
		}),
		HasRollup: true,
		Rollup: applied.Rollup{
			ID:            []byte("foo.bar"),
			AggregationID: maggregation.MustCompressTypes(maggregation.Count),
		},
		Remainder: applied.NewPipeline([]applied.Union{
			{
				Type: op.RollupType,
				Rollup: applied.Rollup{
					ID:            []byte("foo.baz"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
		}),
	}
	err = te.ResetSetData(testCounterID, testStoragePolicy, testAggregationTypesExpensive, testPipeline)
	require.NoError(t, err)
	require.Equal(t, expectedParsedPipeline, te.parsedPipeline)
	require.Equal(t, len(testAggregationTypesExpensive), len(te.lastConsumedValues))
	for i := 0; i < len(te.lastConsumedValues); i++ {
		require.True(t, math.IsNaN(te.lastConsumedValues[i]))
	}
}

func TestTimerResetSetDataInvalidAggregationType(t *testing.T) {
	opts := NewOptions()
	te := MustNewTimerElem(nil, policy.EmptyStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, opts)
	err := te.ResetSetData(testBatchTimerID, testStoragePolicy, maggregation.Types{maggregation.Last}, applied.DefaultPipeline)
	require.Error(t, err)
}

func TestTimerResetSetDataInvalidPipeline(t *testing.T) {
	opts := NewOptions()
	te := MustNewTimerElem(nil, policy.EmptyStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, opts)

	invalidPipeline := applied.NewPipeline([]applied.Union{
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.Absolute},
		},
	})
	err := te.ResetSetData(testBatchTimerID, testStoragePolicy, maggregation.DefaultTypes, invalidPipeline)
	require.Error(t, err)
}

func TestTimerElemAddMetric(t *testing.T) {
	e, err := NewTimerElem(testBatchTimerID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, NewOptions())
	require.NoError(t, err)

	// Add a timer metric.
	require.NoError(t, e.AddMetric(testTimestamps[0], testBatchTimer))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	timer := e.values[0].aggregation
	require.Equal(t, int64(5), timer.Count())
	require.Equal(t, 18.0, timer.Sum())
	require.Equal(t, 3.5, timer.Quantile(0.5))
	require.Equal(t, 6.5, timer.Quantile(0.95))
	require.Equal(t, 6.5, timer.Quantile(0.99))

	// Add the timer metric at slightly different time
	// but still within the same aggregation interval.
	require.NoError(t, e.AddMetric(testTimestamps[1], testBatchTimer))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	timer = e.values[0].aggregation
	require.Equal(t, int64(10), timer.Count())
	require.Equal(t, 36.0, timer.Sum())
	require.Equal(t, 3.5, timer.Quantile(0.5))
	require.Equal(t, 6.5, timer.Quantile(0.95))
	require.Equal(t, 6.5, timer.Quantile(0.99))

	// Add the timer metric in the next aggregation interval.
	require.NoError(t, e.AddMetric(testTimestamps[2], testBatchTimer))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].timeNanos)
	}
	timer = e.values[1].aggregation
	require.Equal(t, int64(5), timer.Count())
	require.Equal(t, 18.0, timer.Sum())
	require.Equal(t, 3.5, timer.Quantile(0.5))
	require.Equal(t, 6.5, timer.Quantile(0.95))
	require.Equal(t, 6.5, timer.Quantile(0.99))

	// Adding the timer metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddMetric(testTimestamps[2], testBatchTimer))
}

func TestTimerElemConsumeDefaultAggregationDefaultPipeline(t *testing.T) {
	// Set up stream options.
	streamOpts, p, numAlloc := testStreamOptions(t, len(testAlignedStarts)-1)

	// Verify the pool is big enough to supply all the streams.
	opts := NewOptions().SetStreamOptions(streamOpts)
	e := testTimerElem(testAlignedStarts[:len(testAlignedStarts)-1], testBatchTimerVals, maggregation.DefaultTypes, applied.DefaultPipeline, opts)
	verifyStreamPoolSize(t, p, 0, numAlloc)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardMetricFn()
	require.False(t, e.Consume(0, localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[1], localFn, forwardFn))
	require.Equal(t, expectedLocalMetricsForTimer(testAlignedStarts[1], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 1, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, expectedLocalMetricsForTimer(testAlignedStarts[2], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.True(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Verify the streams have been returned to pool.
	verifyStreamPoolSize(t, p, len(testAlignedStarts)-1, numAlloc)
}

func TestTimerElemConsumeCustomAggregationDefaultPipeline(t *testing.T) {
	// Set up stream options.
	streamOpts, p, numAlloc := testStreamOptions(t, len(testAlignedStarts)-1)

	// Verify the pool is big enough to supply all the streams.
	opts := NewOptions().SetStreamOptions(streamOpts)
	e := testTimerElem(testAlignedStarts[:len(testAlignedStarts)-1], testBatchTimerVals, testTimerAggregationTypes, applied.DefaultPipeline, opts)
	verifyStreamPoolSize(t, p, 0, numAlloc)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardMetricFn()
	require.False(t, e.Consume(0, localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[1], localFn, forwardFn))
	require.Equal(t, expectedLocalMetricsForTimer(testAlignedStarts[1], testStoragePolicy, testTimerAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 1, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, expectedLocalMetricsForTimer(testAlignedStarts[2], testStoragePolicy, testTimerAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.True(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Verify the streams have been returned to pool.
	verifyStreamPoolSize(t, p, len(testAlignedStarts)-1, numAlloc)
}

func TestTimerElemConsumeCustomAggregationCustomPipeline(t *testing.T) {
	alignedTimeNanos := []int64{
		time.Unix(210, 0).UnixNano(),
		time.Unix(220, 0).UnixNano(),
		time.Unix(230, 0).UnixNano(),
		time.Unix(240, 0).UnixNano(),
	}
	timerVals := [][]float64{
		{123, 1245},
		{456},
		{589, 1120},
	}
	aggregationTypes := maggregation.Types{maggregation.Min}
	e := testTimerElem(alignedTimeNanos[:3], timerVals, aggregationTypes, testPipeline, NewOptions())

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardMetricFn()
	require.False(t, e.Consume(0, localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 3, len(e.values))

	// Consume one value.
	expectedRes := []testForwardMetric{
		{
			metric: aggregated.Metric{
				ID:        id.RawID("foo.bar"),
				TimeNanos: time.Unix(220, 0).UnixNano(),
				Value:     nan,
			},
			meta: metadata.ForwardMetadata{
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
				StoragePolicy: testStoragePolicy,
				Pipeline: applied.NewPipeline([]applied.Union{
					{
						Type: op.RollupType,
						Rollup: applied.Rollup{
							ID:            []byte("foo.baz"),
							AggregationID: maggregation.MustCompressTypes(maggregation.Max),
						},
					},
				}),
			},
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(alignedTimeNanos[1], localFn, forwardFn))
	verifyForwardMetrics(t, expectedRes, *forwardRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 2, len(e.values))
	require.Equal(t, time.Unix(220, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, []float64{123.0}, e.lastConsumedValues)

	// Consume all values.
	expectedRes = []testForwardMetric{
		{
			metric: aggregated.Metric{
				ID:        id.RawID("foo.bar"),
				TimeNanos: time.Unix(230, 0).UnixNano(),
				Value:     33.3,
			},
			meta: metadata.ForwardMetadata{
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
				StoragePolicy: testStoragePolicy,
				Pipeline: applied.NewPipeline([]applied.Union{
					{
						Type: op.RollupType,
						Rollup: applied.Rollup{
							ID:            []byte("foo.baz"),
							AggregationID: maggregation.MustCompressTypes(maggregation.Max),
						},
					},
				}),
			},
		},
		{
			metric: aggregated.Metric{
				ID:        id.RawID("foo.bar"),
				TimeNanos: time.Unix(240, 0).UnixNano(),
				Value:     13.3,
			},
			meta: metadata.ForwardMetadata{
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
				StoragePolicy: testStoragePolicy,
				Pipeline: applied.NewPipeline([]applied.Union{
					{
						Type: op.RollupType,
						Rollup: applied.Rollup{
							ID:            []byte("foo.baz"),
							AggregationID: maggregation.MustCompressTypes(maggregation.Max),
						},
					},
				}),
			},
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(alignedTimeNanos[3], localFn, forwardFn))
	verifyForwardMetrics(t, expectedRes, *forwardRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(e.values))
	require.Equal(t, time.Unix(240, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, []float64{589.0}, e.lastConsumedValues)

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.True(t, e.Consume(alignedTimeNanos[3], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(alignedTimeNanos[3], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))
}

func TestTimerElemClose(t *testing.T) {
	// Set up stream options.
	streamOpts, p, numAlloc := testStreamOptions(t, len(testAlignedStarts)-1)

	// Verify the pool is big enough to supply all the streams.
	opts := NewOptions().SetStreamOptions(streamOpts)
	e := testTimerElem(testAlignedStarts[:len(testAlignedStarts)-1], testBatchTimerVals, maggregation.DefaultTypes, applied.DefaultPipeline, opts)
	verifyStreamPoolSize(t, p, 0, numAlloc)

	require.False(t, e.closed)

	// Closing the element.
	e.Close()

	// Closing a second time should have no impact.
	e.Close()

	require.True(t, e.closed)
	require.Nil(t, e.id)
	require.Equal(t, parsedPipeline{}, e.parsedPipeline)
	require.Equal(t, 0, len(e.values))
	require.Equal(t, 0, len(e.toConsume))
	require.Equal(t, 0, len(e.lastConsumedValues))
	require.NotNil(t, e.values)

	// Verify the streams have been returned to pool.
	verifyStreamPoolSize(t, p, len(testAlignedStarts)-1, numAlloc)
}

func TestTimerFindOrInsert(t *testing.T) {
	e, err := NewTimerElem(testBatchTimerID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, NewOptions())
	require.NoError(t, err)

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
		require.Equal(t, e.values[expected[idx].index].aggregation, res)
		require.Equal(t, expected[idx].data, times)
	}
}

func TestGaugeResetSetData(t *testing.T) {
	opts := NewOptions()
	ge, err := NewGaugeElem(nil, policy.EmptyStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, opts)
	require.NoError(t, err)
	require.Equal(t, opts.AggregationTypesOptions().DefaultGaugeAggregationTypes(), ge.aggTypes)
	require.True(t, ge.useDefaultAggregation)
	require.False(t, ge.aggOpts.HasExpensiveAggregations)

	// Reset element with a default pipeline.
	err = ge.ResetSetData(testGaugeID, testStoragePolicy, testAggregationTypesExpensive, applied.DefaultPipeline)
	require.NoError(t, err)
	require.Equal(t, testGaugeID, ge.id)
	require.Equal(t, testStoragePolicy, ge.sp)
	require.Equal(t, testAggregationTypesExpensive, ge.aggTypes)
	require.Equal(t, parsedPipeline{}, ge.parsedPipeline)
	require.False(t, ge.tombstoned)
	require.False(t, ge.closed)
	require.False(t, ge.useDefaultAggregation)
	require.True(t, ge.aggOpts.HasExpensiveAggregations)
	require.Nil(t, ge.lastConsumedValues)

	// Reset element with a pipeline containing a derivative transformation.
	expectedParsedPipeline := parsedPipeline{
		HasDerivativeTransform: true,
		Transformations: applied.NewPipeline([]applied.Union{
			{
				Type:           op.TransformationType,
				Transformation: op.Transformation{Type: transformation.Absolute},
			},
			{
				Type:           op.TransformationType,
				Transformation: op.Transformation{Type: transformation.PerSecond},
			},
		}),
		HasRollup: true,
		Rollup: applied.Rollup{
			ID:            []byte("foo.bar"),
			AggregationID: maggregation.MustCompressTypes(maggregation.Count),
		},
		Remainder: applied.NewPipeline([]applied.Union{
			{
				Type: op.RollupType,
				Rollup: applied.Rollup{
					ID:            []byte("foo.baz"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
		}),
	}
	err = ge.ResetSetData(testGaugeID, testStoragePolicy, testAggregationTypesExpensive, testPipeline)
	require.NoError(t, err)
	require.Equal(t, expectedParsedPipeline, ge.parsedPipeline)
	require.Equal(t, len(testAggregationTypesExpensive), len(ge.lastConsumedValues))
	for i := 0; i < len(ge.lastConsumedValues); i++ {
		require.True(t, math.IsNaN(ge.lastConsumedValues[i]))
	}
}

func TestGaugeElemAddMetric(t *testing.T) {
	e, err := NewGaugeElem(testGaugeID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, NewOptions())
	require.NoError(t, err)

	// Add a gauge metric.
	require.NoError(t, e.AddMetric(testTimestamps[0], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, testGauge.GaugeVal, e.values[0].aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[0].aggregation.Sum())
	require.Equal(t, 0.0, e.values[0].aggregation.SumSq())

	// Add the gauge metric at slightly different time
	// but still within the same aggregation interval.
	require.NoError(t, e.AddMetric(testTimestamps[1], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, testGauge.GaugeVal, e.values[0].aggregation.Last())
	require.Equal(t, 2*testGauge.GaugeVal, e.values[0].aggregation.Sum())
	require.Equal(t, 0.0, e.values[0].aggregation.SumSq())

	// Add the gauge metric in the next aggregation interval.
	require.NoError(t, e.AddMetric(testTimestamps[2], testGauge))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].timeNanos)
	}
	require.Equal(t, testGauge.GaugeVal, e.values[1].aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[1].aggregation.Sum())
	require.Equal(t, 0.0, e.values[1].aggregation.SumSq())

	// Adding the gauge metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddMetric(testTimestamps[2], testGauge))
}

func TestGaugeElemAddMetricWithCustomAggregation(t *testing.T) {
	e, err := NewGaugeElem(testGaugeID, testStoragePolicy, testAggregationTypesExpensive, applied.DefaultPipeline, NewOptions())
	require.NoError(t, err)

	// Add a gauge metric.
	require.NoError(t, e.AddMetric(testTimestamps[0], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, testGauge.GaugeVal, e.values[0].aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[0].aggregation.Sum())
	require.Equal(t, testGauge.GaugeVal, e.values[0].aggregation.Mean())
	require.Equal(t, testGauge.GaugeVal, e.values[0].aggregation.Sum())
	require.Equal(t, testGauge.GaugeVal*testGauge.GaugeVal, e.values[0].aggregation.SumSq())

	// Add the gauge metric at slightly different time
	// but still within the same aggregation interval.
	require.NoError(t, e.AddMetric(testTimestamps[1], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].timeNanos)
	require.Equal(t, testGauge.GaugeVal, e.values[0].aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[0].aggregation.Max())
	require.Equal(t, 2*testGauge.GaugeVal, e.values[0].aggregation.Sum())
	require.Equal(t, 2*testGauge.GaugeVal*testGauge.GaugeVal, e.values[0].aggregation.SumSq())

	// Add the gauge metric in the next aggregation interval.
	require.NoError(t, e.AddMetric(testTimestamps[2], testGauge))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].timeNanos)
	}
	require.Equal(t, testGauge.GaugeVal, e.values[1].aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[1].aggregation.Max())

	// Adding the gauge metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddMetric(testTimestamps[2], testGauge))
}

func TestGaugeElemConsumeDefaultAggregationDefaultPipeline(t *testing.T) {
	e := testGaugeElem(testAlignedStarts[:len(testAlignedStarts)-1], testGaugeVals, maggregation.DefaultTypes, applied.DefaultPipeline)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardMetricFn()
	require.False(t, e.Consume(0, localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[1], localFn, forwardFn))
	require.Equal(t, expectedLocalMetricsForGauge(testAlignedStarts[1], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 1, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, expectedLocalMetricsForGauge(testAlignedStarts[2], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.True(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))
}

func TestGaugeElemConsumeCustomAggregationDefaultPipeline(t *testing.T) {
	e := testGaugeElem(testAlignedStarts[:len(testAlignedStarts)-1], testGaugeVals, testAggregationTypes, applied.DefaultPipeline)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardMetricFn()
	require.False(t, e.Consume(0, localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[1], localFn, forwardFn))
	require.Equal(t, expectedLocalMetricsForGauge(testAlignedStarts[1], testStoragePolicy, testAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 1, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, expectedLocalMetricsForGauge(testAlignedStarts[2], testStoragePolicy, testAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.True(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(testAlignedStarts[2], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))
}

func TestGaugeElemConsumeCustomAggregationCustomPipeline(t *testing.T) {
	alignedTimeNanos := []int64{
		time.Unix(210, 0).UnixNano(),
		time.Unix(220, 0).UnixNano(),
		time.Unix(230, 0).UnixNano(),
		time.Unix(240, 0).UnixNano(),
	}
	gaugeVals := []float64{-123.0, -456.0, -589.0}
	aggregationTypes := maggregation.Types{maggregation.Last}
	e := testGaugeElem(alignedTimeNanos[:3], gaugeVals, aggregationTypes, testPipeline)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardMetricFn()
	require.False(t, e.Consume(0, localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 3, len(e.values))

	// Consume one value.
	expectedRes := []testForwardMetric{
		{
			metric: aggregated.Metric{
				ID:        id.RawID("foo.bar"),
				TimeNanos: time.Unix(220, 0).UnixNano(),
				Value:     nan,
			},
			meta: metadata.ForwardMetadata{
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
				StoragePolicy: testStoragePolicy,
				Pipeline: applied.NewPipeline([]applied.Union{
					{
						Type: op.RollupType,
						Rollup: applied.Rollup{
							ID:            []byte("foo.baz"),
							AggregationID: maggregation.MustCompressTypes(maggregation.Max),
						},
					},
				}),
			},
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(alignedTimeNanos[1], localFn, forwardFn))
	verifyForwardMetrics(t, expectedRes, *forwardRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 2, len(e.values))
	require.Equal(t, time.Unix(220, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, []float64{123.0}, e.lastConsumedValues)

	// Consume all values.
	expectedRes = []testForwardMetric{
		{
			metric: aggregated.Metric{
				ID:        id.RawID("foo.bar"),
				TimeNanos: time.Unix(230, 0).UnixNano(),
				Value:     33.3,
			},
			meta: metadata.ForwardMetadata{
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
				StoragePolicy: testStoragePolicy,
				Pipeline: applied.NewPipeline([]applied.Union{
					{
						Type: op.RollupType,
						Rollup: applied.Rollup{
							ID:            []byte("foo.baz"),
							AggregationID: maggregation.MustCompressTypes(maggregation.Max),
						},
					},
				}),
			},
		},
		{
			metric: aggregated.Metric{
				ID:        id.RawID("foo.bar"),
				TimeNanos: time.Unix(240, 0).UnixNano(),
				Value:     13.3,
			},
			meta: metadata.ForwardMetadata{
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
				StoragePolicy: testStoragePolicy,
				Pipeline: applied.NewPipeline([]applied.Union{
					{
						Type: op.RollupType,
						Rollup: applied.Rollup{
							ID:            []byte("foo.baz"),
							AggregationID: maggregation.MustCompressTypes(maggregation.Max),
						},
					},
				}),
			},
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(alignedTimeNanos[3], localFn, forwardFn))
	verifyForwardMetrics(t, expectedRes, *forwardRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(e.values))
	require.Equal(t, time.Unix(240, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, []float64{589.0}, e.lastConsumedValues)

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.True(t, e.Consume(alignedTimeNanos[3], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardMetricFn()
	require.False(t, e.Consume(alignedTimeNanos[3], localFn, forwardFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))
}

func TestGaugeElemClose(t *testing.T) {
	e := testGaugeElem(testAlignedStarts[:len(testAlignedStarts)-1], testGaugeVals, maggregation.DefaultTypes, applied.DefaultPipeline)
	require.False(t, e.closed)

	// Closing the element.
	e.Close()

	// Closing a second time should have no impact.
	e.Close()

	require.True(t, e.closed)
	require.Nil(t, e.id)
	require.Equal(t, parsedPipeline{}, e.parsedPipeline)
	require.Equal(t, 0, len(e.values))
	require.Equal(t, 0, len(e.toConsume))
	require.Equal(t, 0, len(e.lastConsumedValues))
	require.NotNil(t, e.values)
}

func TestGaugeFindOrInsert(t *testing.T) {
	e, err := NewGaugeElem(testGaugeID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, NewOptions())
	require.NoError(t, err)

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
		require.Equal(t, e.values[expected[idx].index].aggregation, res)
		require.Equal(t, expected[idx].data, times)
	}
}

type testIndexData struct {
	index int
	data  []int64
}

type testSuffixAndValue struct {
	aggType maggregation.Type
	value   float64
}

type testLocalMetric struct {
	idPrefix  []byte
	id        id.RawID
	idSuffix  []byte
	timeNanos int64
	value     float64
	sp        policy.StoragePolicy
}

func testFlushLocalMetricFn() (
	flushLocalMetricFn,
	*[]testLocalMetric,
) {
	var result []testLocalMetric
	return func(
		idPrefix []byte,
		id id.RawID,
		idSuffix []byte,
		timeNanos int64,
		value float64,
		sp policy.StoragePolicy,
	) {
		result = append(result, testLocalMetric{
			idPrefix:  idPrefix,
			id:        id,
			idSuffix:  idSuffix,
			timeNanos: timeNanos,
			value:     value,
			sp:        sp,
		})
	}, &result
}

type testForwardMetric struct {
	metric aggregated.Metric
	meta   metadata.ForwardMetadata
}

func testFlushForwardMetricFn() (
	flushForwardMetricFn,
	*[]testForwardMetric,
) {
	var result []testForwardMetric
	return func(
		metric aggregated.Metric,
		meta metadata.ForwardMetadata,
	) {
		result = append(result, testForwardMetric{
			metric: metric,
			meta:   meta,
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

func testCounterElem(
	alignedTimeNanos []int64,
	counterVals []int64,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
) *CounterElem {
	e := MustNewCounterElem(testCounterID, testStoragePolicy, aggTypes, pipeline, NewOptions())
	for i, aligned := range alignedTimeNanos {
		counter := newLockedCounter(raggregation.NewCounter(e.aggOpts))
		counter.Update(counterVals[i])
		e.values = append(e.values, timedCounter{
			timeNanos:   aligned,
			aggregation: counter,
		})
	}
	return e
}

func testTimerElem(
	alignedTimeNanos []int64,
	timerBatches [][]float64,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
	opts Options,
) *TimerElem {
	e := MustNewTimerElem(testBatchTimerID, testStoragePolicy, aggTypes, pipeline, opts)
	for i, aligned := range alignedTimeNanos {
		newTimer := raggregation.NewTimer(opts.AggregationTypesOptions().TimerQuantiles(), opts.StreamOptions(), e.aggOpts)
		timer := newLockedTimer(newTimer)
		timer.AddBatch(timerBatches[i])
		e.values = append(e.values, timedTimer{
			timeNanos:   aligned,
			aggregation: timer,
		})
	}
	return e
}

func testGaugeElem(
	alignedTimeNanos []int64,
	gaugeVals []float64,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
) *GaugeElem {
	e := MustNewGaugeElem(testGaugeID, testStoragePolicy, aggTypes, pipeline, NewOptions())
	for i, aligned := range alignedTimeNanos {
		gauge := newLockedGauge(raggregation.NewGauge(e.aggOpts))
		gauge.Update(gaugeVals[i])
		e.values = append(e.values, timedGauge{
			timeNanos:   aligned,
			aggregation: gauge,
		})
	}
	return e
}

func expectCounterSuffix(aggType maggregation.Type) []byte {
	return testOpts.AggregationTypesOptions().TypeStringForCounter(aggType)
}

func expectTimerSuffix(aggType maggregation.Type) []byte {
	return testOpts.AggregationTypesOptions().TypeStringForTimer(aggType)
}

func expectGaugeSuffix(aggType maggregation.Type) []byte {
	return testOpts.AggregationTypesOptions().TypeStringForGauge(aggType)
}

func expectedLocalMetricsForCounter(
	timeNanos int64,
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
) []testLocalMetric {
	if !aggTypes.IsDefault() {
		var res []testLocalMetric
		for _, aggType := range aggTypes {
			res = append(res, testLocalMetric{
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
	return []testLocalMetric{
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

func expectedLocalMetricsForTimer(
	timeNanos int64,
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
) []testLocalMetric {
	// This needs to be a list as the order of the result matters in some test.
	data := []testSuffixAndValue{
		{maggregation.Sum, 18.0},
		{maggregation.SumSq, 83.38},
		{maggregation.Mean, 3.6},
		{maggregation.Min, 1.0},
		{maggregation.Max, 6.5},
		{maggregation.Count, 5.0},
		{maggregation.Stdev, 2.15522620622523},
		{maggregation.Median, 3.5},
		{maggregation.P50, 3.5},
		{maggregation.P95, 6.5},
		{maggregation.P99, 6.5},
	}
	var expected []testLocalMetric
	if !aggTypes.IsDefault() {
		for _, aggType := range aggTypes {
			for _, d := range data {
				if d.aggType == aggType {
					expected = append(expected, testLocalMetric{
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
		expected = append(expected, testLocalMetric{
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

func expectedLocalMetricsForGauge(
	timeNanos int64,
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
) []testLocalMetric {
	if !aggTypes.IsDefault() {
		var res []testLocalMetric
		for _, aggType := range aggTypes {
			res = append(res, testLocalMetric{
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
	return []testLocalMetric{
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

func verifyForwardMetrics(t *testing.T, expected, actual []testForwardMetric) {
	require.Equal(t, len(expected), len(actual))
	for i := 0; i < len(expected); i++ {
		require.Equal(t, expected[i].metric.ID, actual[i].metric.ID)
		require.Equal(t, expected[i].metric.TimeNanos, actual[i].metric.TimeNanos)
		if math.IsNaN(expected[i].metric.Value) {
			require.True(t, math.IsNaN(actual[i].metric.Value))
		} else {
			require.Equal(t, expected[i].metric.Value, actual[i].metric.Value)
		}
		require.Equal(t, expected[i].meta, actual[i].meta)
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
