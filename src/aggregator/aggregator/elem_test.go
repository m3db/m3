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

	raggregation "github.com/m3db/m3/src/aggregator/aggregation"
	"github.com/m3db/m3/src/aggregator/aggregation/quantile/cm"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	"github.com/willf/bitset"
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
		Type:       metric.CounterType,
		ID:         testCounterID,
		CounterVal: 1234,
	}
	testBatchTimer = unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            testBatchTimerID,
		BatchTimerVal: []float64{1.0, 3.5, 2.2, 6.5, 4.8},
	}
	testGauge = unaggregated.MetricUnion{
		Type:     metric.GaugeType,
		ID:       testGaugeID,
		GaugeVal: 123.456,
	}
	testPipeline = applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
		},
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: applied.RollupOp{
				ID:            []byte("foo.bar"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
			},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: applied.RollupOp{
				ID:            []byte("foo.baz"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Max),
			},
		},
	})
	testNumForwardedTimes = 0
	testOpts              = NewOptions()
	testTimestamps        = []time.Time{
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
	ce, err := NewCounterElem(nil, policy.EmptyStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, 1, NoPrefixNoSuffix, opts)
	require.NoError(t, err)
	require.Equal(t, opts.AggregationTypesOptions().DefaultCounterAggregationTypes(), ce.aggTypes)
	require.True(t, ce.useDefaultAggregation)
	require.False(t, ce.aggOpts.HasExpensiveAggregations)
	require.Equal(t, 1, ce.numForwardedTimes)

	// Reset element with a default pipeline.
	err = ce.ResetSetData(testCounterID, testStoragePolicy, testAggregationTypesExpensive, applied.DefaultPipeline, 2, NoPrefixNoSuffix)
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
	require.Equal(t, 2, ce.numForwardedTimes)

	// Reset element with a pipeline containing a derivative transformation.
	expectedParsedPipeline := parsedPipeline{
		HasDerivativeTransform: true,
		Transformations: []transformation.Op{
			mustNewOp(t, transformation.Absolute),
			mustNewOp(t, transformation.PerSecond),
		},
		HasRollup: true,
		Rollup: applied.RollupOp{
			ID:            []byte("foo.bar"),
			AggregationID: maggregation.MustCompressTypes(maggregation.Count),
		},
		Remainder: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("foo.baz"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
		}),
	}
	err = ce.ResetSetData(testCounterID, testStoragePolicy, testAggregationTypesExpensive, testPipeline, 0, NoPrefixNoSuffix)
	require.NoError(t, err)
	requirePipelinesMatch(t, expectedParsedPipeline, ce.parsedPipeline)
	require.Equal(t, len(testAggregationTypesExpensive), len(ce.lastConsumedValues))
	for i := 0; i < len(ce.lastConsumedValues); i++ {
		require.True(t, math.IsNaN(ce.lastConsumedValues[i].Value))
	}
}

func TestCounterResetSetDataInvalidAggregationType(t *testing.T) {
	opts := NewOptions()
	ce := MustNewCounterElem(nil, policy.EmptyStoragePolicy,
		maggregation.DefaultTypes, applied.DefaultPipeline,
		testNumForwardedTimes, NoPrefixNoSuffix, opts)
	err := ce.ResetSetData(testCounterID, testStoragePolicy,
		maggregation.Types{maggregation.Last}, applied.DefaultPipeline,
		0, NoPrefixNoSuffix)
	require.Error(t, err)
}

func TestCounterResetSetDataNoRollup(t *testing.T) {
	opts := NewOptions()
	ce := MustNewCounterElem(nil, policy.EmptyStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, opts)

	pipelineNoRollup := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
		},
	})
	err := ce.ResetSetData(testCounterID, testStoragePolicy, maggregation.DefaultTypes, pipelineNoRollup, 0, NoPrefixNoSuffix)
	require.NoError(t, err)
}

func TestCounterElemAddUnion(t *testing.T) {
	e, err := NewCounterElem(testCounterID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
	require.NoError(t, err)

	// Add a counter metric.
	require.NoError(t, e.AddUnion(testTimestamps[0], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, testCounter.CounterVal, e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(1), e.values[0].lockedAgg.aggregation.Count())
	require.Equal(t, int64(0), e.values[0].lockedAgg.aggregation.SumSq())

	// Add the counter metric at slightly different time
	// but still within the same aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[1], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, 2*testCounter.CounterVal, e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(2), e.values[0].lockedAgg.aggregation.Count())
	require.Equal(t, int64(0), e.values[0].lockedAgg.aggregation.SumSq())

	// Add the counter metric in the next aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[2], testCounter))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, testCounter.CounterVal, e.values[1].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(2), e.values[0].lockedAgg.aggregation.Count())
	require.Equal(t, int64(0), e.values[0].lockedAgg.aggregation.SumSq())

	// Adding the counter metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnion(testTimestamps[2], testCounter))
}

func TestCounterElemAddUnionWithCustomAggregation(t *testing.T) {
	e, err := NewCounterElem(testCounterID, testStoragePolicy, testAggregationTypesExpensive, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
	require.NoError(t, err)

	// Add a counter metric.
	require.NoError(t, e.AddUnion(testTimestamps[0], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, testCounter.CounterVal, e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, testCounter.CounterVal, e.values[0].lockedAgg.aggregation.Max())
	require.Equal(t, int64(testCounter.CounterVal*testCounter.CounterVal), e.values[0].lockedAgg.aggregation.SumSq())

	// Add the counter metric at slightly different time
	// but still within the same aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[1], testCounter))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, 2*testCounter.CounterVal, e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, testCounter.CounterVal, e.values[0].lockedAgg.aggregation.Max())

	// Add the counter metric in the next aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[2], testCounter))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, testCounter.CounterVal, e.values[1].lockedAgg.aggregation.Sum())
	require.Equal(t, testCounter.CounterVal, e.values[1].lockedAgg.aggregation.Max())

	// Adding the counter metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnion(testTimestamps[2], testCounter))
}

func TestCounterElemAddUnique(t *testing.T) {
	e, err := NewCounterElem(testCounterID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
	require.NoError(t, err)

	// Add a metric.
	source1 := uint32(1234)
	require.NoError(t, e.AddUnique(testTimestamps[0], []float64{345}, source1))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, int64(345), e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(1), e.values[0].lockedAgg.aggregation.Count())
	require.Equal(t, int64(0), e.values[0].lockedAgg.aggregation.SumSq())
	require.True(t, e.values[0].lockedAgg.sourcesSeen.Test(uint(source1)))

	// Add another metric at slightly different time but still within the
	// same aggregation interval with a different source.
	source2 := uint32(5678)
	require.NoError(t, e.AddUnique(testTimestamps[1], []float64{500}, source2))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, int64(845), e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(2), e.values[0].lockedAgg.aggregation.Count())
	require.Equal(t, int64(0), e.values[0].lockedAgg.aggregation.SumSq())
	require.True(t, e.values[0].lockedAgg.sourcesSeen.Test(uint(source2)))

	// Add the counter metric in the next aggregation interval.
	require.NoError(t, e.AddUnique(testTimestamps[2], []float64{278}, source1))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, int64(278), e.values[1].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(1), e.values[1].lockedAgg.aggregation.Count())
	require.Equal(t, int64(0), e.values[1].lockedAgg.aggregation.SumSq())
	require.True(t, e.values[1].lockedAgg.sourcesSeen.Test(uint(source1)))

	// Add the counter metric in the same aggregation interval with the same
	// source results in an error.
	require.Equal(t, errDuplicateForwardingSource, e.AddUnique(testTimestamps[2], []float64{278}, source1))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, int64(278), e.values[1].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(1), e.values[1].lockedAgg.aggregation.Count())
	require.Equal(t, int64(0), e.values[1].lockedAgg.aggregation.SumSq())
	require.True(t, e.values[1].lockedAgg.sourcesSeen.Test(uint(source1)))

	// Adding the counter metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnique(testTimestamps[2], []float64{100}, 1376))
}

func TestCounterElemAddUniqueWithCustomAggregation(t *testing.T) {
	e, err := NewCounterElem(testCounterID, testStoragePolicy, testAggregationTypesExpensive, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
	require.NoError(t, err)

	// Add a counter metric.
	source1 := uint32(1234)
	require.NoError(t, e.AddUnique(testTimestamps[0], []float64{12}, source1))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, int64(12), e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(12), e.values[0].lockedAgg.aggregation.Max())
	require.Equal(t, int64(144), e.values[0].lockedAgg.aggregation.SumSq())
	require.True(t, e.values[0].lockedAgg.sourcesSeen.Test(uint(source1)))

	// Add the counter metric at slightly different time
	// but still within the same aggregation interval.
	source2 := uint32(5678)
	require.NoError(t, e.AddUnique(testTimestamps[1], []float64{14}, source2))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, int64(26), e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(14), e.values[0].lockedAgg.aggregation.Max())

	// Add the counter metric in the next aggregation interval.
	require.NoError(t, e.AddUnique(testTimestamps[2], []float64{20}, source1))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, int64(20), e.values[1].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(20), e.values[1].lockedAgg.aggregation.Max())
	require.Equal(t, int64(400), e.values[1].lockedAgg.aggregation.SumSq())

	// Add the counter metric in the same aggregation interval with the same
	// source results in an error.
	require.Equal(t, errDuplicateForwardingSource, e.AddUnique(testTimestamps[2], []float64{30}, source1))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, int64(20), e.values[1].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(1), e.values[1].lockedAgg.aggregation.Count())
	require.Equal(t, int64(400), e.values[1].lockedAgg.aggregation.SumSq())
	require.True(t, e.values[1].lockedAgg.sourcesSeen.Test(uint(source1)))

	// Adding the counter metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnique(testTimestamps[2], []float64{40}, 1376))
}

func TestCounterElemConsumeDefaultAggregationDefaultPipeline(t *testing.T) {
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos
	opts := NewOptions()
	e := testCounterElem(testAlignedStarts[:len(testAlignedStarts)-1], testCounterVals, maggregation.DefaultTypes, applied.DefaultPipeline, opts)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, expectedLocalMetricsForCounter(testAlignedStarts[1], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, expectedLocalMetricsForCounter(testAlignedStarts[2], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))
}

func TestCounterElemConsumeCustomAggregationDefaultPipeline(t *testing.T) {
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos
	opts := NewOptions()
	e := testCounterElem(testAlignedStarts[:len(testAlignedStarts)-1], testCounterVals, testAggregationTypes, applied.DefaultPipeline, opts)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, expectedLocalMetricsForCounter(testAlignedStarts[1], testStoragePolicy, testAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, expectedLocalMetricsForCounter(testAlignedStarts[2], testStoragePolicy, testAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))
}

func TestCounterElemConsumeCustomAggregationCustomPipeline(t *testing.T) {
	alignedstartAtNanos := []int64{
		time.Unix(210, 0).UnixNano(),
		time.Unix(220, 0).UnixNano(),
		time.Unix(230, 0).UnixNano(),
		time.Unix(240, 0).UnixNano(),
	}
	counterVals := []int64{-123, -456, -589}
	aggregationTypes := maggregation.Types{maggregation.Sum}
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos
	opts := NewOptions().SetDiscardNaNAggregatedValues(false)
	e := testCounterElem(alignedstartAtNanos[:3], counterVals, aggregationTypes, testPipeline, opts)

	aggKey := aggregationKey{
		aggregationID: maggregation.MustCompressTypes(maggregation.Count),
		storagePolicy: testStoragePolicy,
		pipeline: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("foo.baz"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
		}),
		numForwardedTimes: testNumForwardedTimes + 1,
	}
	expectedOnFlushedRes := []testOnForwardedFlushedData{
		{
			aggregationKey: aggKey,
		},
	}

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 3, len(e.values))

	// Consume one value.
	expectedForwardedRes := []testForwardedMetricWithMetadata{
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(220, 0).UnixNano(),
			value:          nan,
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[1], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 2, len(e.values))
	require.Equal(t, time.Unix(220, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, 1, len(e.lastConsumedValues))
	require.Equal(t, 123.0, e.lastConsumedValues[0].Value)

	// Consume all values.
	expectedForwardedRes = []testForwardedMetricWithMetadata{
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(230, 0).UnixNano(),
			value:          33.3,
		},
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(240, 0).UnixNano(),
			value:          13.3,
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(e.values))
	require.Equal(t, time.Unix(240, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, 1, len(e.lastConsumedValues))
	require.Equal(t, 589.0, e.lastConsumedValues[0].Value)

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))
}

func TestCounterElemClose(t *testing.T) {
	e := testCounterElem(testAlignedStarts[:len(testAlignedStarts)-1], testCounterVals, maggregation.DefaultTypes, applied.DefaultPipeline, NewOptions())
	require.False(t, e.closed)

	// Closing the element.
	e.Close()

	// Closing a second time should have no impact.
	e.Close()

	require.True(t, e.closed)
	require.Nil(t, e.id)
	require.Equal(t, parsedPipeline{}, e.parsedPipeline)
	require.Nil(t, e.writeForwardedMetricFn)
	require.Nil(t, e.onForwardedAggregationWrittenFn)
	require.Nil(t, e.cachedSourceSets)
	require.Equal(t, 0, len(e.values))
	require.Equal(t, 0, len(e.toConsume))
	require.Equal(t, 0, len(e.lastConsumedValues))
	require.NotNil(t, e.values)
}

func TestCounterFindOrCreateNoSourceSet(t *testing.T) {
	e, err := NewCounterElem(testCounterID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
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
		res, err := e.findOrCreate(input, createAggregationOptions{initSourceSet: false})
		require.NoError(t, err)
		var times []int64
		for _, v := range e.values {
			times = append(times, v.startAtNanos)
		}
		require.Equal(t, e.values[expected[idx].index].lockedAgg, res)
		require.Nil(t, e.values[expected[idx].index].lockedAgg.sourcesSeen)
		require.Equal(t, expected[idx].data, times)
	}
}

func TestCounterFindOrCreateWithSourceSet(t *testing.T) {
	e, err := NewCounterElem(testCounterID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
	require.NoError(t, err)
	e.cachedSourceSets = []*bitset.BitSet{bitset.New(0)}

	inputs := []int64{10, 20}
	expected := []testIndexData{
		{index: 0, data: []int64{10}},
		{index: 1, data: []int64{10, 20}},
	}
	for idx, input := range inputs {
		res, err := e.findOrCreate(input, createAggregationOptions{initSourceSet: true})
		require.NoError(t, err)
		var times []int64
		for _, v := range e.values {
			times = append(times, v.startAtNanos)
		}
		require.Equal(t, e.values[expected[idx].index].lockedAgg, res)
		require.Equal(t, expected[idx].data, times)
		require.NotNil(t, e.values[expected[idx].index].lockedAgg.sourcesSeen)
	}
	require.Equal(t, 0, len(e.cachedSourceSets))
}

func TestTimerResetSetData(t *testing.T) {
	opts := NewOptions()
	te, err := NewTimerElem(nil, policy.EmptyStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, opts)
	require.NoError(t, err)
	require.Nil(t, te.quantilesPool)
	require.NotNil(t, te.quantiles)
	require.True(t, te.aggOpts.HasExpensiveAggregations)
	require.Equal(t, opts.AggregationTypesOptions().DefaultTimerAggregationTypes(), te.aggTypes)
	require.True(t, te.useDefaultAggregation)

	// Reset element with a default pipeline.
	err = te.ResetSetData(testBatchTimerID, testStoragePolicy, maggregation.Types{maggregation.Max, maggregation.P999}, applied.DefaultPipeline, 0, NoPrefixNoSuffix)
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
		Transformations: []transformation.Op{
			mustNewOp(t, transformation.Absolute),
			mustNewOp(t, transformation.PerSecond),
		},
		HasRollup: true,
		Rollup: applied.RollupOp{
			ID:            []byte("foo.bar"),
			AggregationID: maggregation.MustCompressTypes(maggregation.Count),
		},
		Remainder: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("foo.baz"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
		}),
	}
	err = te.ResetSetData(testBatchTimerID, testStoragePolicy, testAggregationTypesExpensive, testPipeline, 0, NoPrefixNoSuffix)
	require.NoError(t, err)
	requirePipelinesMatch(t, expectedParsedPipeline, te.parsedPipeline)
	require.Equal(t, len(testAggregationTypesExpensive), len(te.lastConsumedValues))
	for i := 0; i < len(te.lastConsumedValues); i++ {
		require.True(t, math.IsNaN(te.lastConsumedValues[i].Value))
	}
}

func TestTimerResetSetDataInvalidAggregationType(t *testing.T) {
	opts := NewOptions()
	te := MustNewTimerElem(nil, policy.EmptyStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, opts)
	err := te.ResetSetData(testBatchTimerID, testStoragePolicy, maggregation.Types{maggregation.Last}, applied.DefaultPipeline, 0, NoPrefixNoSuffix)
	require.Error(t, err)
}

func TestTimerResetSetDataNoRollup(t *testing.T) {
	opts := NewOptions()
	te := MustNewTimerElem(nil, policy.EmptyStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, opts)

	pipelineNoRollup := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
		},
	})
	err := te.ResetSetData(testBatchTimerID, testStoragePolicy, maggregation.DefaultTypes, pipelineNoRollup, 0, NoPrefixNoSuffix)
	require.NoError(t, err)
}

func TestTimerElemAddUnion(t *testing.T) {
	e, err := NewTimerElem(testBatchTimerID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
	require.NoError(t, err)

	// Add a timer metric.
	require.NoError(t, e.AddUnion(testTimestamps[0], testBatchTimer))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	timer := e.values[0].lockedAgg.aggregation
	require.Equal(t, int64(5), timer.Count())
	require.Equal(t, 18.0, timer.Sum())
	require.Equal(t, 3.5, timer.Quantile(0.5))
	require.Equal(t, 6.5, timer.Quantile(0.95))
	require.Equal(t, 6.5, timer.Quantile(0.99))

	// Add the timer metric at slightly different time
	// but still within the same aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[1], testBatchTimer))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	timer = e.values[0].lockedAgg.aggregation
	require.Equal(t, int64(10), timer.Count())
	require.Equal(t, 36.0, timer.Sum())
	require.Equal(t, 3.5, timer.Quantile(0.5))
	require.Equal(t, 6.5, timer.Quantile(0.95))
	require.Equal(t, 6.5, timer.Quantile(0.99))

	// Add the timer metric in the next aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[2], testBatchTimer))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	timer = e.values[1].lockedAgg.aggregation
	require.Equal(t, int64(5), timer.Count())
	require.Equal(t, 18.0, timer.Sum())
	require.Equal(t, 3.5, timer.Quantile(0.5))
	require.Equal(t, 6.5, timer.Quantile(0.95))
	require.Equal(t, 6.5, timer.Quantile(0.99))

	// Adding the timer metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnion(testTimestamps[2], testBatchTimer))
}

func TestTimerElemAddUnique(t *testing.T) {
	e, err := NewTimerElem(testBatchTimerID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
	require.NoError(t, err)

	// Add a metric.
	require.NoError(t, e.AddUnique(testTimestamps[0], []float64{11.1}, 1))
	require.NoError(t, e.AddUnique(testTimestamps[0], []float64{12.2}, 2))
	require.NoError(t, e.AddUnique(testTimestamps[0], []float64{13.3}, 3))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	timer := e.values[0].lockedAgg.aggregation
	require.Equal(t, int64(3), timer.Count())
	require.InEpsilon(t, 36.6, timer.Sum(), 1e-10)
	require.Equal(t, 12.2, timer.Quantile(0.5))

	// Add another metric at slightly different time but still within the
	// same aggregation interval with a different source.
	require.NoError(t, e.AddUnique(testTimestamps[1], []float64{14.4}, 4))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	timer = e.values[0].lockedAgg.aggregation
	require.Equal(t, int64(4), timer.Count())
	require.InEpsilon(t, 51, timer.Sum(), 1e-10)

	// Add the metric in the next aggregation interval.
	require.NoError(t, e.AddUnique(testTimestamps[2], []float64{20.0}, 1))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, 20.0, e.values[1].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(1), e.values[1].lockedAgg.aggregation.Count())
	require.Equal(t, 20.0, e.values[1].lockedAgg.aggregation.Sum())

	// Add the metric in the same aggregation interval with the same
	// source results in an error.
	require.Equal(t, errDuplicateForwardingSource, e.AddUnique(testTimestamps[2], []float64{30.0}, 1))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, 20.0, e.values[1].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(1), e.values[1].lockedAgg.aggregation.Count())
	require.InEpsilon(t, 400.0, e.values[1].lockedAgg.aggregation.SumSq(), 1e-10)
	require.True(t, e.values[1].lockedAgg.sourcesSeen.Test(1))

	// Adding the timer metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnique(testTimestamps[2], []float64{100}, 3))
}

func TestTimerElemConsumeDefaultAggregationDefaultPipeline(t *testing.T) {
	// Set up stream options.
	streamOpts, p, numAlloc := testStreamOptions(t, len(testAlignedStarts)-1)
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos

	// Verify the pool is big enough to supply all the streams.
	opts := NewOptions().SetStreamOptions(streamOpts)
	e := testTimerElem(testAlignedStarts[:len(testAlignedStarts)-1], testBatchTimerVals, maggregation.DefaultTypes, applied.DefaultPipeline, opts)
	verifyStreamPoolSize(t, p, 0, numAlloc)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, expectedLocalMetricsForTimer(testAlignedStarts[1], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, expectedLocalMetricsForTimer(testAlignedStarts[2], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Verify the streams have been returned to pool.
	verifyStreamPoolSize(t, p, len(testAlignedStarts)-1, numAlloc)
}

func TestTimerElemConsumeCustomAggregationDefaultPipeline(t *testing.T) {
	// Set up stream options.
	streamOpts, p, numAlloc := testStreamOptions(t, len(testAlignedStarts)-1)
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos

	// Verify the pool is big enough to supply all the streams.
	opts := NewOptions().SetStreamOptions(streamOpts)
	e := testTimerElem(testAlignedStarts[:len(testAlignedStarts)-1], testBatchTimerVals, testTimerAggregationTypes, applied.DefaultPipeline, opts)
	verifyStreamPoolSize(t, p, 0, numAlloc)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, expectedLocalMetricsForTimer(testAlignedStarts[1], testStoragePolicy, testTimerAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, expectedLocalMetricsForTimer(testAlignedStarts[2], testStoragePolicy, testTimerAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Verify the streams have been returned to pool.
	verifyStreamPoolSize(t, p, len(testAlignedStarts)-1, numAlloc)
}

func TestTimerElemConsumeCustomAggregationCustomPipeline(t *testing.T) {
	alignedstartAtNanos := []int64{
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
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos
	opts := NewOptions().SetDiscardNaNAggregatedValues(false)
	e := testTimerElem(alignedstartAtNanos[:3], timerVals, aggregationTypes, testPipeline, opts)

	aggKey := aggregationKey{
		aggregationID: maggregation.MustCompressTypes(maggregation.Count),
		storagePolicy: testStoragePolicy,
		pipeline: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("foo.baz"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
		}),
		numForwardedTimes: testNumForwardedTimes + 1,
	}
	expectedOnFlushedRes := []testOnForwardedFlushedData{
		{
			aggregationKey: aggKey,
		},
	}

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 3, len(e.values))

	// Consume one value.
	expectedForwardedRes := []testForwardedMetricWithMetadata{
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(220, 0).UnixNano(),
			value:          nan,
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[1], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 2, len(e.values))
	require.Equal(t, time.Unix(220, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, 1, len(e.lastConsumedValues))
	require.Equal(t, 123.0, e.lastConsumedValues[0].Value)

	// Consume all values.
	expectedForwardedRes = []testForwardedMetricWithMetadata{
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(230, 0).UnixNano(),
			value:          33.3,
		},
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(240, 0).UnixNano(),
			value:          13.3,
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(e.values))
	require.Equal(t, time.Unix(240, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, 1, len(e.lastConsumedValues))
	require.Equal(t, 589.0, e.lastConsumedValues[0].Value)

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
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
	require.Nil(t, e.writeForwardedMetricFn)
	require.Nil(t, e.onForwardedAggregationWrittenFn)
	require.Nil(t, e.cachedSourceSets)
	require.Equal(t, 0, len(e.values))
	require.Equal(t, 0, len(e.toConsume))
	require.Equal(t, 0, len(e.lastConsumedValues))
	require.NotNil(t, e.values)

	// Verify the streams have been returned to pool.
	verifyStreamPoolSize(t, p, len(testAlignedStarts)-1, numAlloc)
}

func TestTimerFindOrCreateNoSourceSet(t *testing.T) {
	e, err := NewTimerElem(testBatchTimerID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
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
		res, err := e.findOrCreate(input, createAggregationOptions{initSourceSet: false})
		require.NoError(t, err)
		var times []int64
		for _, v := range e.values {
			times = append(times, v.startAtNanos)
		}
		require.Equal(t, e.values[expected[idx].index].lockedAgg, res)
		require.Equal(t, expected[idx].data, times)
	}
}

func TestTimerFindOrCreateWithSourceSet(t *testing.T) {
	e, err := NewTimerElem(testBatchTimerID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
	require.NoError(t, err)
	e.cachedSourceSets = []*bitset.BitSet{bitset.New(0)}

	inputs := []int64{10, 20}
	expected := []testIndexData{
		{index: 0, data: []int64{10}},
		{index: 1, data: []int64{10, 20}},
	}
	for idx, input := range inputs {
		res, err := e.findOrCreate(input, createAggregationOptions{initSourceSet: true})
		require.NoError(t, err)
		var times []int64
		for _, v := range e.values {
			times = append(times, v.startAtNanos)
		}
		require.Equal(t, e.values[expected[idx].index].lockedAgg, res)
		require.Equal(t, expected[idx].data, times)
		require.NotNil(t, e.values[expected[idx].index].lockedAgg.sourcesSeen)
	}
	require.Equal(t, 0, len(e.cachedSourceSets))
}

func TestGaugeResetSetData(t *testing.T) {
	opts := NewOptions()
	ge, err := NewGaugeElem(nil, policy.EmptyStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, opts)
	require.NoError(t, err)
	require.Equal(t, opts.AggregationTypesOptions().DefaultGaugeAggregationTypes(), ge.aggTypes)
	require.True(t, ge.useDefaultAggregation)
	require.False(t, ge.aggOpts.HasExpensiveAggregations)

	// Reset element with a default pipeline.
	err = ge.ResetSetData(testGaugeID, testStoragePolicy, testAggregationTypesExpensive, applied.DefaultPipeline, 0, NoPrefixNoSuffix)
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
		Transformations: []transformation.Op{
			mustNewOp(t, transformation.Absolute),
			mustNewOp(t, transformation.PerSecond),
		},
		HasRollup: true,
		Rollup: applied.RollupOp{
			ID:            []byte("foo.bar"),
			AggregationID: maggregation.MustCompressTypes(maggregation.Count),
		},
		Remainder: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("foo.baz"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
		}),
	}
	err = ge.ResetSetData(testGaugeID, testStoragePolicy, testAggregationTypesExpensive, testPipeline, 0, NoPrefixNoSuffix)
	require.NoError(t, err)
	requirePipelinesMatch(t, expectedParsedPipeline, ge.parsedPipeline)
	require.Equal(t, len(testAggregationTypesExpensive), len(ge.lastConsumedValues))
	for i := 0; i < len(ge.lastConsumedValues); i++ {
		require.True(t, math.IsNaN(ge.lastConsumedValues[i].Value))
	}
}

func TestGaugeElemAddUnion(t *testing.T) {
	e, err := NewGaugeElem(testGaugeID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
	require.NoError(t, err)

	// Add a gauge metric.
	require.NoError(t, e.AddUnion(testTimestamps[0], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, testGauge.GaugeVal, e.values[0].lockedAgg.aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, 0.0, e.values[0].lockedAgg.aggregation.SumSq())

	// Add the gauge metric at slightly different time
	// but still within the same aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[1], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, testGauge.GaugeVal, e.values[0].lockedAgg.aggregation.Last())
	require.Equal(t, 2*testGauge.GaugeVal, e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, 0.0, e.values[0].lockedAgg.aggregation.SumSq())

	// Add the gauge metric in the next aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[2], testGauge))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, testGauge.GaugeVal, e.values[1].lockedAgg.aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[1].lockedAgg.aggregation.Sum())
	require.Equal(t, 0.0, e.values[1].lockedAgg.aggregation.SumSq())

	// Adding the gauge metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnion(testTimestamps[2], testGauge))
}

func TestGaugeElemAddUnionWithCustomAggregation(t *testing.T) {
	e, err := NewGaugeElem(testGaugeID, testStoragePolicy, testAggregationTypesExpensive, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
	require.NoError(t, err)

	// Add a gauge metric.
	require.NoError(t, e.AddUnion(testTimestamps[0], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, testGauge.GaugeVal, e.values[0].lockedAgg.aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, testGauge.GaugeVal, e.values[0].lockedAgg.aggregation.Mean())
	require.Equal(t, testGauge.GaugeVal, e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, testGauge.GaugeVal*testGauge.GaugeVal, e.values[0].lockedAgg.aggregation.SumSq())

	// Add the gauge metric at slightly different time
	// but still within the same aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[1], testGauge))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, testGauge.GaugeVal, e.values[0].lockedAgg.aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[0].lockedAgg.aggregation.Max())
	require.Equal(t, 2*testGauge.GaugeVal, e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, 2*testGauge.GaugeVal*testGauge.GaugeVal, e.values[0].lockedAgg.aggregation.SumSq())

	// Add the gauge metric in the next aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[2], testGauge))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, testGauge.GaugeVal, e.values[1].lockedAgg.aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, e.values[1].lockedAgg.aggregation.Max())

	// Adding the gauge metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnion(testTimestamps[2], testGauge))
}

func TestGaugeElemAddUnique(t *testing.T) {
	e, err := NewGaugeElem(testGaugeID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
	require.NoError(t, err)

	// Add a metric.
	source1 := uint32(1234)
	require.NoError(t, e.AddUnique(testTimestamps[0], []float64{12.3, 34.5}, source1))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, 46.8, e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(2), e.values[0].lockedAgg.aggregation.Count())
	require.Equal(t, 0.0, e.values[0].lockedAgg.aggregation.SumSq())
	require.True(t, e.values[0].lockedAgg.sourcesSeen.Test(uint(source1)))

	// Add another metric at slightly different time but still within the
	// same aggregation interval with a different source.
	source2 := uint32(5678)
	require.NoError(t, e.AddUnique(testTimestamps[1], []float64{50}, source2))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, 96.8, e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(3), e.values[0].lockedAgg.aggregation.Count())
	require.Equal(t, 0.0, e.values[0].lockedAgg.aggregation.SumSq())
	require.True(t, e.values[0].lockedAgg.sourcesSeen.Test(uint(source2)))

	// Add the metric in the next aggregation interval.
	require.NoError(t, e.AddUnique(testTimestamps[2], []float64{27.8}, source1))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, 27.8, e.values[1].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(1), e.values[1].lockedAgg.aggregation.Count())
	require.Equal(t, 0.0, e.values[1].lockedAgg.aggregation.SumSq())
	require.True(t, e.values[1].lockedAgg.sourcesSeen.Test(uint(source1)))

	// Add the gauge metric in the same aggregation interval with the same
	// source results in an error.
	require.Equal(t, errDuplicateForwardingSource, e.AddUnique(testTimestamps[2], []float64{27.8}, source1))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, 27.8, e.values[1].lockedAgg.aggregation.Sum())
	require.Equal(t, int64(1), e.values[1].lockedAgg.aggregation.Count())
	require.Equal(t, 0.0, e.values[1].lockedAgg.aggregation.SumSq())
	require.True(t, e.values[1].lockedAgg.sourcesSeen.Test(uint(source1)))

	// Adding the gauge metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnique(testTimestamps[2], []float64{10.0}, 3))
}

func TestGaugeElemAddUniqueWithCustomAggregation(t *testing.T) {
	e, err := NewGaugeElem(testGaugeID, testStoragePolicy, testAggregationTypesExpensive, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
	require.NoError(t, err)

	// Add a gauge metric.
	source1 := uint32(1234)
	require.NoError(t, e.AddUnique(testTimestamps[0], []float64{1.2}, source1))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.Equal(t, 1.2, e.values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, 1.2, e.values[0].lockedAgg.aggregation.Max())
	require.Equal(t, 1.44, e.values[0].lockedAgg.aggregation.SumSq())
	require.True(t, e.values[0].lockedAgg.sourcesSeen.Test(uint(source1)))

	// Add the gauge metric at slightly different time
	// but still within the same aggregation interval.
	source2 := uint32(5678)
	require.NoError(t, e.AddUnique(testTimestamps[1], []float64{1.4}, source2))
	require.Equal(t, 1, len(e.values))
	require.Equal(t, testAlignedStarts[0], e.values[0].startAtNanos)
	require.InEpsilon(t, 2.6, e.values[0].lockedAgg.aggregation.Sum(), 1e-10)
	require.Equal(t, 1.4, e.values[0].lockedAgg.aggregation.Max())

	// Add the gauge metric in the next aggregation interval.
	require.NoError(t, e.AddUnique(testTimestamps[2], []float64{2.0}, source1))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, 2.0, e.values[1].lockedAgg.aggregation.Sum())
	require.Equal(t, 2.0, e.values[1].lockedAgg.aggregation.Max())
	require.Equal(t, 4.0, e.values[1].lockedAgg.aggregation.SumSq())

	// Add the gauge metric in the same aggregation interval with the same
	// source results in an error.
	require.Equal(t, errDuplicateForwardingSource, e.AddUnique(testTimestamps[2], []float64{3.0}, source1))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, testAlignedStarts[i], e.values[i].startAtNanos)
	}
	require.Equal(t, 2.0, e.values[1].lockedAgg.aggregation.Sum())
	require.Equal(t, 2.0, e.values[1].lockedAgg.aggregation.Max())
	require.Equal(t, 4.0, e.values[1].lockedAgg.aggregation.SumSq())
	require.True(t, e.values[1].lockedAgg.sourcesSeen.Test(uint(source1)))

	// Adding the gauge metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnique(testTimestamps[2], []float64{4.0}, 3))
}

func TestGaugeElemConsumeDefaultAggregationDefaultPipeline(t *testing.T) {
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos
	opts := NewOptions()
	e := testGaugeElem(testAlignedStarts[:len(testAlignedStarts)-1], testGaugeVals, maggregation.DefaultTypes, applied.DefaultPipeline, opts)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, expectedLocalMetricsForGauge(testAlignedStarts[1], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, expectedLocalMetricsForGauge(testAlignedStarts[2], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))
}

func TestGaugeElemConsumeCustomAggregationDefaultPipeline(t *testing.T) {
	opts := NewOptions()
	e := testGaugeElem(testAlignedStarts[:len(testAlignedStarts)-1], testGaugeVals, testAggregationTypes, applied.DefaultPipeline, opts)
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, expectedLocalMetricsForGauge(testAlignedStarts[1], testStoragePolicy, testAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, expectedLocalMetricsForGauge(testAlignedStarts[2], testStoragePolicy, testAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))
}

func TestGaugeElemConsumeCustomAggregationCustomPipeline(t *testing.T) {
	alignedstartAtNanos := []int64{
		time.Unix(210, 0).UnixNano(),
		time.Unix(220, 0).UnixNano(),
		time.Unix(230, 0).UnixNano(),
		time.Unix(240, 0).UnixNano(),
	}
	gaugeVals := []float64{-123.0, -456.0, -589.0}
	aggregationTypes := maggregation.Types{maggregation.Last}
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos
	opts := NewOptions().SetDiscardNaNAggregatedValues(false)
	e := testGaugeElem(alignedstartAtNanos[:3], gaugeVals, aggregationTypes, testPipeline, opts)

	aggKey := aggregationKey{
		aggregationID: maggregation.MustCompressTypes(maggregation.Count),
		storagePolicy: testStoragePolicy,
		pipeline: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("foo.baz"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
		}),
		numForwardedTimes: testNumForwardedTimes + 1,
	}
	expectedOnFlushedRes := []testOnForwardedFlushedData{
		{
			aggregationKey: aggKey,
		},
	}

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 3, len(e.values))

	// Consume one value.
	expectedForwardedRes := []testForwardedMetricWithMetadata{
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(220, 0).UnixNano(),
			value:          nan,
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[1], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 2, len(e.values))
	require.Equal(t, time.Unix(220, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, 1, len(e.lastConsumedValues))
	require.Equal(t, 123.0, e.lastConsumedValues[0].Value)

	// Consume all values.
	expectedForwardedRes = []testForwardedMetricWithMetadata{
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(230, 0).UnixNano(),
			value:          33.3,
		},
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(240, 0).UnixNano(),
			value:          13.3,
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(e.values))
	require.Equal(t, time.Unix(240, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, 1, len(e.lastConsumedValues))
	require.Equal(t, 589.0, e.lastConsumedValues[0].Value)

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))
}

func TestGaugeElemReset(t *testing.T) {
	alignedstartAtNanos := []int64{
		time.Unix(210, 0).UnixNano(),
		time.Unix(220, 0).UnixNano(),
		time.Unix(230, 0).UnixNano(),
		time.Unix(240, 0).UnixNano(),
	}
	gaugeVals := []float64{123.0, 456.0, 589.0}
	aggregationTypes := maggregation.Types{maggregation.Sum}
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos
	opts := NewOptions().SetDiscardNaNAggregatedValues(false)

	testPipeline := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Reset},
		},
	})

	e := testGaugeElem(alignedstartAtNanos[:3], gaugeVals, aggregationTypes, testPipeline, opts)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 3, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[1], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, (*localRes)[0].timeNanos, alignedstartAtNanos[1])
	require.Equal(t, (*localRes)[0].value, 123.0)
	require.Equal(t, (*localRes)[1].timeNanos, alignedstartAtNanos[1]+int64(time.Second))
	require.Equal(t, (*localRes)[1].value, 0.0)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))
	require.Equal(t, time.Unix(220, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, 0, len(e.lastConsumedValues))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, (*localRes)[0].timeNanos, alignedstartAtNanos[2])
	require.Equal(t, (*localRes)[0].value, 456.0)
	require.Equal(t, (*localRes)[1].timeNanos, alignedstartAtNanos[2]+int64(time.Second))
	require.Equal(t, (*localRes)[1].value, 0.0)
	require.Equal(t, (*localRes)[2].timeNanos, alignedstartAtNanos[3])
	require.Equal(t, (*localRes)[2].value, 589.0)
	require.Equal(t, (*localRes)[3].timeNanos, alignedstartAtNanos[3]+int64(time.Second))
	require.Equal(t, (*localRes)[3].value, 0.0)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))
	require.Equal(t, time.Unix(240, 0).UnixNano(), e.lastConsumedAtNanos)
	require.Equal(t, 0, len(e.lastConsumedValues))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	//verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, localFn, forwardFn, onForwardedFlushedFn))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(e.values))
}

func TestGaugeElemClose(t *testing.T) {
	e := testGaugeElem(testAlignedStarts[:len(testAlignedStarts)-1], testGaugeVals, maggregation.DefaultTypes, applied.DefaultPipeline, NewOptions())
	require.False(t, e.closed)

	// Closing the element.
	e.Close()

	// Closing a second time should have no impact.
	e.Close()

	require.True(t, e.closed)
	require.Nil(t, e.id)
	require.Equal(t, parsedPipeline{}, e.parsedPipeline)
	require.Nil(t, e.writeForwardedMetricFn)
	require.Nil(t, e.onForwardedAggregationWrittenFn)
	require.Nil(t, e.cachedSourceSets)
	require.Equal(t, 0, len(e.values))
	require.Equal(t, 0, len(e.toConsume))
	require.Equal(t, 0, len(e.lastConsumedValues))
	require.NotNil(t, e.values)
}

func TestGaugeFindOrCreateNoSourceSet(t *testing.T) {
	e, err := NewGaugeElem(testGaugeID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
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
		res, err := e.findOrCreate(input, createAggregationOptions{initSourceSet: false})
		require.NoError(t, err)
		var times []int64
		for _, v := range e.values {
			times = append(times, v.startAtNanos)
		}
		require.Equal(t, e.values[expected[idx].index].lockedAgg, res)
		require.Equal(t, expected[idx].data, times)
	}
}

func TestGaugeFindOrCreateWithSourceSet(t *testing.T) {
	e, err := NewGaugeElem(testGaugeID, testStoragePolicy, maggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, NoPrefixNoSuffix, NewOptions())
	require.NoError(t, err)
	e.cachedSourceSets = []*bitset.BitSet{bitset.New(0)}

	inputs := []int64{10, 20}
	expected := []testIndexData{
		{index: 0, data: []int64{10}},
		{index: 1, data: []int64{10, 20}},
	}
	for idx, input := range inputs {
		res, err := e.findOrCreate(input, createAggregationOptions{initSourceSet: true})
		require.NoError(t, err)
		var times []int64
		for _, v := range e.values {
			times = append(times, v.startAtNanos)
		}
		require.Equal(t, e.values[expected[idx].index].lockedAgg, res)
		require.Equal(t, expected[idx].data, times)
		require.NotNil(t, e.values[expected[idx].index].lockedAgg.sourcesSeen)
	}
	require.Equal(t, 0, len(e.cachedSourceSets))
}

type testIndexData struct {
	index int
	data  []int64
}

type testSuffixAndValue struct {
	aggType maggregation.Type
	value   float64
}

type testLocalMetricWithMetadata struct {
	idPrefix  []byte
	id        id.RawID
	idSuffix  []byte
	timeNanos int64
	value     float64
	sp        policy.StoragePolicy
}

type testForwardedMetricWithMetadata struct {
	aggregationKey aggregationKey
	timeNanos      int64
	value          float64
}

type testOnForwardedFlushedData struct {
	aggregationKey aggregationKey
}

func testFlushLocalMetricFn() (
	flushLocalMetricFn,
	*[]testLocalMetricWithMetadata,
) {
	var result []testLocalMetricWithMetadata
	return func(
		idPrefix []byte,
		id id.RawID,
		idSuffix []byte,
		timeNanos int64,
		value float64,
		sp policy.StoragePolicy,
	) {
		result = append(result, testLocalMetricWithMetadata{
			idPrefix:  idPrefix,
			id:        id,
			idSuffix:  idSuffix,
			timeNanos: timeNanos,
			value:     value,
			sp:        sp,
		})
	}, &result
}

func testFlushForwardedMetricFn() (
	flushForwardedMetricFn,
	*[]testForwardedMetricWithMetadata,
) {
	var result []testForwardedMetricWithMetadata
	return func(
		writeFn writeForwardedMetricFn,
		aggregationKey aggregationKey,
		timeNanos int64,
		value float64,
	) {
		result = append(result, testForwardedMetricWithMetadata{
			aggregationKey: aggregationKey,
			timeNanos:      timeNanos,
			value:          value,
		})
	}, &result
}

func testOnForwardedFlushedFn() (
	onForwardingElemFlushedFn,
	*[]testOnForwardedFlushedData,
) {
	var result []testOnForwardedFlushedData
	return func(
		onDoneFn onForwardedAggregationDoneFn,
		aggregationKey aggregationKey,
	) {
		result = append(result, testOnForwardedFlushedData{
			aggregationKey: aggregationKey,
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
	alignedstartAtNanos []int64,
	counterVals []int64,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
	opts Options,
) *CounterElem {
	e := MustNewCounterElem(testCounterID, testStoragePolicy, aggTypes, pipeline, testNumForwardedTimes, WithPrefixWithSuffix, opts)
	for i, aligned := range alignedstartAtNanos {
		counter := &lockedCounterAggregation{aggregation: newCounterAggregation(raggregation.NewCounter(e.aggOpts))}
		counter.aggregation.Update(time.Unix(0, aligned), counterVals[i])
		e.values = append(e.values, timedCounter{
			startAtNanos: aligned,
			lockedAgg:    counter,
		})
	}
	return e
}

func testTimerElem(
	alignedstartAtNanos []int64,
	timerBatches [][]float64,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
	opts Options,
) *TimerElem {
	e := MustNewTimerElem(testBatchTimerID, testStoragePolicy, aggTypes, pipeline, testNumForwardedTimes, WithPrefixWithSuffix, opts)
	for i, aligned := range alignedstartAtNanos {
		newTimer := raggregation.NewTimer(opts.AggregationTypesOptions().Quantiles(), opts.StreamOptions(), e.aggOpts)
		timer := &lockedTimerAggregation{aggregation: newTimerAggregation(newTimer)}
		timer.aggregation.AddBatch(time.Now(), timerBatches[i])
		e.values = append(e.values, timedTimer{
			startAtNanos: aligned,
			lockedAgg:    timer,
		})
	}
	return e
}

func testGaugeElem(
	alignedstartAtNanos []int64,
	gaugeVals []float64,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
	opts Options,
) *GaugeElem {
	e := MustNewGaugeElem(testGaugeID, testStoragePolicy, aggTypes, pipeline, testNumForwardedTimes, WithPrefixWithSuffix, opts)
	for i, aligned := range alignedstartAtNanos {
		gauge := &lockedGaugeAggregation{aggregation: newGaugeAggregation(raggregation.NewGauge(e.aggOpts))}
		gauge.aggregation.Update(time.Unix(0, aligned), gaugeVals[i])
		e.values = append(e.values, timedGauge{
			startAtNanos: aligned,
			lockedAgg:    gauge,
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
) []testLocalMetricWithMetadata {
	if !aggTypes.IsDefault() {
		var res []testLocalMetricWithMetadata
		for _, aggType := range aggTypes {
			res = append(res, testLocalMetricWithMetadata{
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
	return []testLocalMetricWithMetadata{
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
) []testLocalMetricWithMetadata {
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
	var expected []testLocalMetricWithMetadata
	if !aggTypes.IsDefault() {
		for _, aggType := range aggTypes {
			for _, d := range data {
				if d.aggType == aggType {
					expected = append(expected, testLocalMetricWithMetadata{
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
		expected = append(expected, testLocalMetricWithMetadata{
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
) []testLocalMetricWithMetadata {
	if !aggTypes.IsDefault() {
		var res []testLocalMetricWithMetadata
		for _, aggType := range aggTypes {
			res = append(res, testLocalMetricWithMetadata{
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
	return []testLocalMetricWithMetadata{
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

func verifyForwardedMetrics(t *testing.T, expected, actual []testForwardedMetricWithMetadata) {
	require.Equal(t, len(expected), len(actual))
	for i := 0; i < len(expected); i++ {
		require.True(t, expected[i].aggregationKey.Equal(actual[i].aggregationKey))
		require.Equal(t, expected[i].timeNanos, actual[i].timeNanos)
		if math.IsNaN(expected[i].value) {
			require.True(t, math.IsNaN(actual[i].value))
		} else {
			require.Equal(t, expected[i].value, actual[i].value)
		}
	}
}

func verifyOnForwardedFlushResult(t *testing.T, expected, actual []testOnForwardedFlushedData) {
	require.Equal(t, len(expected), len(actual))
	for i := 0; i < len(expected); i++ {
		require.True(t, expected[i].aggregationKey.Equal(actual[i].aggregationKey))
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
