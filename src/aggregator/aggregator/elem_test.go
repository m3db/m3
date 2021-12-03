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
	"sync"
	"testing"
	"time"

	"github.com/uber-go/tally"
	"github.com/willf/bitset"
	"go.uber.org/atomic"

	raggregation "github.com/m3db/m3/src/aggregator/aggregation"
	"github.com/m3db/m3/src/aggregator/aggregation/quantile/cm"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testCounterID                 = id.RawID("testCounter")
	testBatchTimerID              = id.RawID("testBatchTimer")
	testGaugeID                   = id.RawID("testGauge")
	testAnnot                     = []byte("testAnnotation")
	testStoragePolicy             = policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour)
	testAggregationTypes          = maggregation.Types{maggregation.Mean, maggregation.Sum}
	testAggregationTypesExpensive = maggregation.Types{maggregation.SumSq}
	testTimerAggregationTypes     = maggregation.Types{maggregation.SumSq, maggregation.P99}
	testGaugeElemData             = ElemData{
		ID:                testGaugeID,
		StoragePolicy:     testStoragePolicy,
		Pipeline:          testPipeline,
		NumForwardedTimes: testNumForwardedTimes,
	}
	testCounterElemData = ElemData{
		ID:                testCounterID,
		StoragePolicy:     testStoragePolicy,
		Pipeline:          testPipeline,
		NumForwardedTimes: testNumForwardedTimes,
	}
	testTimerElemData = ElemData{
		ID:                testBatchTimerID,
		StoragePolicy:     testStoragePolicy,
		Pipeline:          testPipeline,
		NumForwardedTimes: testNumForwardedTimes,
	}
	testCounter = unaggregated.MetricUnion{
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
	testOpts              = newTestOptions()
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
	opts := newTestOptions()
	elemData := testCounterElemData
	elemData.NumForwardedTimes = 1
	ce, err := NewCounterElem(elemData, NewElemOptions(opts))
	require.NoError(t, err)
	require.Equal(t, opts.AggregationTypesOptions().DefaultCounterAggregationTypes(), ce.aggTypes)
	require.True(t, ce.useDefaultAggregation)
	require.False(t, ce.aggOpts.HasExpensiveAggregations)
	require.Equal(t, 1, ce.numForwardedTimes)

	// Reset element with a default pipeline.
	elemData.AggTypes = testAggregationTypesExpensive
	elemData.NumForwardedTimes = 2
	elemData.Pipeline = applied.DefaultPipeline
	err = ce.ResetSetData(elemData)
	require.NoError(t, err)
	require.Equal(t, testCounterID, ce.id)
	require.Equal(t, testStoragePolicy, ce.sp)
	require.Equal(t, testAggregationTypesExpensive, ce.aggTypes)
	require.Equal(t, parsedPipeline{}, ce.parsedPipeline)
	require.False(t, ce.tombstoned)
	require.False(t, ce.closed)
	require.False(t, ce.useDefaultAggregation)
	require.True(t, ce.aggOpts.HasExpensiveAggregations)
	require.Empty(t, ce.flushState)
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
	elemData.NumForwardedTimes = 0
	elemData.Pipeline = testPipeline
	err = ce.ResetSetData(elemData)
	require.NoError(t, err)
	requirePipelinesMatch(t, expectedParsedPipeline, ce.parsedPipeline)
	require.Empty(t, ce.flushState)
}

func TestCounterResetSetDataInvalidAggregationType(t *testing.T) {
	opts := newTestOptions()
	ce := MustNewCounterElem(testCounterElemData, NewElemOptions(opts))
	elemData := testCounterElemData
	elemData.AggTypes = maggregation.Types{maggregation.Last}
	err := ce.ResetSetData(elemData)
	require.Error(t, err)
}

func TestCounterResetSetDataNoRollup(t *testing.T) {
	opts := newTestOptions()
	ce := MustNewCounterElem(testCounterElemData, NewElemOptions(opts))

	pipelineNoRollup := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
		},
	})
	elemData := testCounterElemData
	elemData.Pipeline = pipelineNoRollup
	err := ce.ResetSetData(elemData)
	require.NoError(t, err)
}

func TestCounterElemAddUnion(t *testing.T) {
	e, err := NewCounterElem(testCounterElemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	// Add a counter metric.
	testCounter.Annotation = []byte{1}
	require.NoError(t, e.AddUnion(testTimestamps[0], testCounter, false))
	require.Equal(t, 1, len(e.values))
	a, err := e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v := a.lockedAgg
	require.Equal(t, testCounter.CounterVal, v.aggregation.Sum())
	require.Equal(t, int64(1), v.aggregation.Count())
	require.Equal(t, int64(0), v.aggregation.SumSq())
	require.Equal(t, []byte{1}, v.aggregation.Annotation())

	// Add the counter metric at slightly different time
	// but still within the same aggregation interval.
	testCounter.Annotation = []byte{}
	require.NoError(t, e.AddUnion(testTimestamps[1], testCounter, false))
	require.Equal(t, 1, len(e.values))
	a, err = e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v = a.lockedAgg
	require.Equal(t, 2*testCounter.CounterVal, v.aggregation.Sum())
	require.Equal(t, int64(2), v.aggregation.Count())
	require.Equal(t, int64(0), v.aggregation.SumSq())
	require.Equal(t, []byte{1}, v.aggregation.Annotation())

	// Add the counter metric in the next aggregation interval.
	testCounter.Annotation = []byte{2}
	require.NoError(t, e.AddUnion(testTimestamps[2], testCounter, false))
	require.Equal(t, 2, len(e.values))
	a1, err := e.find(xtime.UnixNano(testAlignedStarts[1]))
	require.NoError(t, err)
	v1 := a1.lockedAgg
	require.Equal(t, testCounter.CounterVal, v1.aggregation.Sum())
	require.Equal(t, int64(2), v.aggregation.Count())
	require.Equal(t, int64(0), v.aggregation.SumSq())
	require.Equal(t, []byte{1}, v.aggregation.Annotation())

	// Adding the counter metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnion(testTimestamps[2], testCounter, false))
}

func TestCounterElemAddUnionWithCustomAggregation(t *testing.T) {
	elemData := testCounterElemData
	elemData.AggTypes = testAggregationTypesExpensive
	e, err := NewCounterElem(elemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	// Add a counter metric.
	testCounter.Annotation = []byte{1}
	require.NoError(t, e.AddUnion(testTimestamps[0], testCounter, false))
	require.Equal(t, 1, len(e.values))
	a, err := e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v := a.lockedAgg
	require.Equal(t, testCounter.CounterVal, v.aggregation.Sum())
	require.Equal(t, testCounter.CounterVal, v.aggregation.Max())
	require.Equal(t, testCounter.CounterVal*testCounter.CounterVal, v.aggregation.SumSq())
	require.Equal(t, []byte{1}, v.aggregation.Annotation())

	// Add the counter metric at slightly different time
	// but still within the same aggregation interval.
	testCounter.Annotation = []byte{}
	require.NoError(t, e.AddUnion(testTimestamps[1], testCounter, false))
	require.Equal(t, 1, len(e.values))
	a, err = e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v = a.lockedAgg
	require.Equal(t, 2*testCounter.CounterVal, v.aggregation.Sum())
	require.Equal(t, testCounter.CounterVal, v.aggregation.Max())
	require.Equal(t, []byte{1}, v.aggregation.Annotation())

	// Add the counter metric in the next aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[2], testCounter, false))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	a, err = e.find(xtime.UnixNano(testAlignedStarts[1]))
	require.NoError(t, err)
	v = a.lockedAgg
	require.Equal(t, testCounter.CounterVal, v.aggregation.Sum())
	require.Equal(t, testCounter.CounterVal, v.aggregation.Max())

	// Adding the counter metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnion(testTimestamps[2], testCounter, false))
}

func TestCounterElemAddUnique(t *testing.T) {
	e, err := NewCounterElem(testCounterElemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	// Add a metric.
	source1 := uint32(1234)
	require.NoError(t, e.AddUnique(testTimestamps[0],
		aggregated.ForwardedMetric{Values: []float64{345}},
		metadata.ForwardMetadata{SourceID: source1}))
	require.Equal(t, 1, len(e.values))
	a, err := e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v := a.lockedAgg
	require.Equal(t, int64(345), v.aggregation.Sum())
	require.Equal(t, int64(1), v.aggregation.Count())
	require.Equal(t, int64(0), v.aggregation.SumSq())
	require.Nil(t, v.aggregation.Annotation())
	versions, seen := v.sourcesSeen[source1]
	require.True(t, seen)
	require.True(t, versions.Test(0))

	// Add another metric at slightly different time but still within the
	// same aggregation interval with a different source.
	source2 := uint32(5678)
	require.NoError(t, e.AddUnique(testTimestamps[1],
		aggregated.ForwardedMetric{Values: []float64{500}, Annotation: testAnnot},
		metadata.ForwardMetadata{SourceID: source2}))
	require.Equal(t, 1, len(e.values))
	a, err = e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v = a.lockedAgg
	require.Equal(t, int64(845), v.aggregation.Sum())
	require.Equal(t, int64(2), v.aggregation.Count())
	require.Equal(t, int64(0), v.aggregation.SumSq())
	require.Equal(t, testAnnot, v.aggregation.Annotation())
	versions, seen = v.sourcesSeen[source2]
	require.True(t, seen)
	require.True(t, versions.Test(0))

	// Add the counter metric in the next aggregation interval.
	require.NoError(t, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{278}},
		metadata.ForwardMetadata{SourceID: source1}))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	a1, err := e.find(xtime.UnixNano(testAlignedStarts[1]))
	require.NoError(t, err)
	v1 := a1.lockedAgg
	require.Equal(t, int64(278), v1.aggregation.Sum())
	require.Equal(t, int64(1), v1.aggregation.Count())
	require.Equal(t, int64(0), v1.aggregation.SumSq())
	versions, seen = v.sourcesSeen[source1]
	require.True(t, seen)
	require.True(t, versions.Test(0))
	require.Equal(t, testAnnot, v.aggregation.Annotation())

	// Add the counter metric in the same aggregation interval with the same
	// source results in an error.
	require.Equal(t, errDuplicateForwardingSource, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{278}},
		metadata.ForwardMetadata{SourceID: source1}))
	require.Equal(t, 2, len(e.values))
	a1, err = e.find(xtime.UnixNano(testAlignedStarts[1]))
	require.NoError(t, err)
	v1 = a1.lockedAgg
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	require.Equal(t, int64(278), v1.aggregation.Sum())
	require.Equal(t, int64(1), v1.aggregation.Count())
	require.Equal(t, int64(0), v1.aggregation.SumSq())
	versions, seen = v1.sourcesSeen[source1]
	require.True(t, seen)
	require.True(t, versions.Test(0))

	// Adding the counter metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{100}},
		metadata.ForwardMetadata{SourceID: 1376}))
}

func TestCounterElemAddUniqueWithCustomAggregation(t *testing.T) {
	elemData := testCounterElemData
	elemData.AggTypes = testAggregationTypesExpensive
	e, err := NewCounterElem(elemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	// Add a counter metric.
	source1 := uint32(1234)
	require.NoError(t, e.AddUnique(testTimestamps[0],
		aggregated.ForwardedMetric{Values: []float64{12}},
		metadata.ForwardMetadata{SourceID: source1}))
	require.Equal(t, 1, len(e.values))
	a, err := e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v := a.lockedAgg
	require.Equal(t, int64(12), v.aggregation.Sum())
	require.Equal(t, int64(12), v.aggregation.Max())
	require.Equal(t, int64(144), v.aggregation.SumSq())
	versions, seen := v.sourcesSeen[source1]
	require.True(t, seen)
	require.True(t, versions.Test(0))

	// Add the counter metric at slightly different time
	// but still within the same aggregation interval.
	source2 := uint32(5678)
	require.NoError(t, e.AddUnique(testTimestamps[1],
		aggregated.ForwardedMetric{Values: []float64{14}},
		metadata.ForwardMetadata{SourceID: source2}))
	require.Equal(t, 1, len(e.values))
	a, err = e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v = a.lockedAgg
	require.Equal(t, int64(26), v.aggregation.Sum())
	require.Equal(t, int64(14), v.aggregation.Max())

	// Add the counter metric in the next aggregation interval.
	require.NoError(t, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{20}},
		metadata.ForwardMetadata{SourceID: source1}))
	require.Equal(t, 2, len(e.values))
	a1, err := e.find(xtime.UnixNano(testAlignedStarts[1]))
	require.NoError(t, err)
	v1 := a1.lockedAgg
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	require.Equal(t, int64(20), v1.aggregation.Sum())
	require.Equal(t, int64(20), v1.aggregation.Max())
	require.Equal(t, int64(400), v1.aggregation.SumSq())

	// Add the counter metric in the same aggregation interval with the same
	// source results in an error.
	require.Equal(t, errDuplicateForwardingSource, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{30}},
		metadata.ForwardMetadata{SourceID: source1}))
	require.Equal(t, 2, len(e.values))
	a1, err = e.find(xtime.UnixNano(testAlignedStarts[1]))
	require.NoError(t, err)
	v1 = a1.lockedAgg
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	require.Equal(t, int64(20), v1.aggregation.Sum())
	require.Equal(t, int64(1), v1.aggregation.Count())
	require.Equal(t, int64(400), v1.aggregation.SumSq())
	versions, seen = v1.sourcesSeen[source1]
	require.True(t, seen)
	require.True(t, versions.Test(0))

	// Adding the counter metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{40}},
		metadata.ForwardMetadata{SourceID: 1376}))
}

func TestCounterElemConsumeDefaultAggregationDefaultPipeline(t *testing.T) {
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos
	opts := newTestOptions()
	e := testCounterElem(testAlignedStarts[:len(testAlignedStarts)-1], testCounterVals, maggregation.DefaultTypes, applied.DefaultPipeline, opts)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, expectedLocalMetricsForCounter(testAlignedStarts[1], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, expectedLocalMetricsForCounter(testAlignedStarts[2], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))
}

func TestCounterElemConsumeCustomAggregationDefaultPipeline(t *testing.T) {
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos
	opts := newTestOptions()
	e := testCounterElem(testAlignedStarts[:len(testAlignedStarts)-1], testCounterVals, testAggregationTypes, applied.DefaultPipeline, opts)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, expectedLocalMetricsForCounter(testAlignedStarts[1], testStoragePolicy, testAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, expectedLocalMetricsForCounter(testAlignedStarts[2], testStoragePolicy, testAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))
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
	opts := newTestOptions().SetDiscardNaNAggregatedValues(false)
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

	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
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

	require.False(t, e.Consume(alignedstartAtNanos[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 3, len(e.values))
	require.Len(t, e.flushState, 1)
	fState := e.flushState[xtime.UnixNano(alignedstartAtNanos[0])]
	consumedVal := fState.consumedValues
	require.Len(t, consumedVal, 1)
	require.Equal(t, 123.0, consumedVal[0])

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

	// Test a consume time well past the latest value in the buffer to ensure we always keep around
	// one value to reference in the future as a previous.
	ts := alignedstartAtNanos[3] + int64(time.Second*20)
	require.False(t, e.Consume(ts, isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	expectedOnFlushedRes[0].expiredTimes = append(expectedOnFlushedRes[0].expiredTimes,
		xtime.ToUnixNano(time.Unix(220, 0)), xtime.ToUnixNano(time.Unix(230, 0)))
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 1, len(e.values))
	require.Len(t, e.flushState, 1)
	fState = e.flushState[xtime.UnixNano(alignedstartAtNanos[2])]
	consumedVal = fState.consumedValues
	require.Len(t, consumedVal, 1)
	require.Equal(t, 589.0, consumedVal[0])

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	expectedOnFlushedRes[0].expiredTimes = nil
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 1, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))
}

func TestCounterElemClose(t *testing.T) {
	e := testCounterElem(testAlignedStarts[:len(testAlignedStarts)-1], testCounterVals,
		maggregation.DefaultTypes, applied.DefaultPipeline, newTestOptions())
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
	require.Equal(t, 0, len(e.flushState))
	require.NotNil(t, e.values)
}

func TestCounterFindOrCreateNoSourceSet(t *testing.T) {
	e, err := NewCounterElem(testCounterElemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	inputs := []xtime.UnixNano{10, 10, 20, 10, 15}
	expected := make(map[xtime.UnixNano]bool)
	for _, inputNanos := range inputs {
		expected[inputNanos] = true
		res, err := e.findOrCreate(int64(inputNanos), createAggregationOptions{initSourceSet: false})
		require.NoError(t, err)
		require.Equal(t, e.values[inputNanos].lockedAgg, res)
		require.Nil(t, e.values[inputNanos].lockedAgg.sourcesSeen)
		require.Equal(t, len(expected), len(e.values))
		for k := range e.values {
			_, ok := expected[k]
			require.True(t, ok)
		}
	}
}

// nolint: dupl
func TestCounterFindOrCreateWithSourceSet(t *testing.T) {
	e, err := NewCounterElem(testCounterElemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)
	e.cachedSourceSets = make([]map[uint32]*bitset.BitSet, 0)

	inputs := []xtime.UnixNano{10, 20}
	expected := make(map[xtime.UnixNano]bool)
	for _, inputNanos := range inputs {
		expected[inputNanos] = true
		res, err := e.findOrCreate(int64(inputNanos), createAggregationOptions{initSourceSet: true})
		require.NoError(t, err)
		require.Equal(t, e.values[inputNanos].lockedAgg, res)
		require.NotNil(t, e.values[inputNanos].lockedAgg.sourcesSeen)
		require.Equal(t, len(expected), len(e.values))
		for k := range e.values {
			_, ok := expected[k]
			require.True(t, ok)
		}
	}
	require.Equal(t, 0, len(e.cachedSourceSets))
}

func TestTimerResetSetData(t *testing.T) {
	opts := newTestOptions()
	te, err := NewTimerElem(testTimerElemData, NewElemOptions(opts))
	require.NoError(t, err)
	require.Nil(t, te.quantilesPool)
	require.NotNil(t, te.quantiles)
	require.True(t, te.aggOpts.HasExpensiveAggregations)
	require.Equal(t, opts.AggregationTypesOptions().DefaultTimerAggregationTypes(), te.aggTypes)
	require.True(t, te.useDefaultAggregation)

	// Reset element with a default pipeline.
	elemData := testTimerElemData
	elemData.AggTypes = maggregation.Types{maggregation.Max, maggregation.P999}
	elemData.Pipeline = applied.DefaultPipeline
	err = te.ResetSetData(elemData)
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
	require.Empty(t, te.flushState)

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
	elemData.Pipeline = testPipeline
	err = te.ResetSetData(elemData)
	require.NoError(t, err)
	requirePipelinesMatch(t, expectedParsedPipeline, te.parsedPipeline)
	require.Empty(t, te.flushState)
}

func TestTimerResetSetDataInvalidAggregationType(t *testing.T) {
	opts := newTestOptions()
	te := MustNewTimerElem(testTimerElemData, NewElemOptions(opts))
	elemData := testTimerElemData
	elemData.AggTypes = maggregation.Types{maggregation.Last}
	err := te.ResetSetData(elemData)
	require.Error(t, err)
}

func TestTimerResetSetDataNoRollup(t *testing.T) {
	opts := newTestOptions()
	te := MustNewTimerElem(testTimerElemData, NewElemOptions(opts))

	pipelineNoRollup := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
		},
	})
	elemData := testTimerElemData
	elemData.Pipeline = pipelineNoRollup
	err := te.ResetSetData(elemData)
	require.NoError(t, err)
}

func TestTimerElemAddUnion(t *testing.T) {
	e, err := NewTimerElem(testTimerElemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	// Add a timer metric.
	require.NoError(t, e.AddUnion(testTimestamps[0], testBatchTimer, false))
	require.Equal(t, 1, len(e.values))
	a, err := e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v := a.lockedAgg
	timer := v.aggregation
	require.Equal(t, int64(5), timer.Count())
	require.Equal(t, 18.0, timer.Sum())
	require.Equal(t, 3.5, timer.Quantile(0.5))

	require.Equal(t, 6.5, timer.Quantile(0.95))
	require.Equal(t, 6.5, timer.Quantile(0.99))

	// Add the timer metric at slightly different time
	// but still within the same aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[1], testBatchTimer, false))
	require.Equal(t, 1, len(e.values))
	a, err = e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v = a.lockedAgg
	timer = v.aggregation
	require.Equal(t, int64(10), timer.Count())
	require.Equal(t, 36.0, timer.Sum())
	require.Equal(t, 3.5, timer.Quantile(0.5))
	require.Equal(t, 6.5, timer.Quantile(0.95))
	require.Equal(t, 6.5, timer.Quantile(0.99))

	// Add the timer metric in the next aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[2], testBatchTimer, false))
	require.Equal(t, 2, len(e.values))
	a1, err := e.find(xtime.UnixNano(testAlignedStarts[1]))
	require.NoError(t, err)
	v1 := a1.lockedAgg
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	timer = v1.aggregation
	require.Equal(t, int64(5), timer.Count())
	require.Equal(t, 18.0, timer.Sum())
	require.Equal(t, 3.5, timer.Quantile(0.5))
	require.Equal(t, 6.5, timer.Quantile(0.95))
	require.Equal(t, 6.5, timer.Quantile(0.99))

	// Adding the timer metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnion(testTimestamps[2], testBatchTimer, false))
}

func TestTimerElemAddUnique(t *testing.T) {
	e, err := NewTimerElem(testTimerElemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	// Add a metric.
	require.NoError(t, e.AddUnique(testTimestamps[0],
		aggregated.ForwardedMetric{Values: []float64{11.1}},
		metadata.ForwardMetadata{SourceID: 1}))
	require.NoError(t, e.AddUnique(testTimestamps[0],
		aggregated.ForwardedMetric{Values: []float64{12.2}},
		metadata.ForwardMetadata{SourceID: 2}))
	require.NoError(t, e.AddUnique(testTimestamps[0],
		aggregated.ForwardedMetric{Values: []float64{13.3}},
		metadata.ForwardMetadata{SourceID: 3}))
	require.Equal(t, 1, len(e.values))
	a, err := e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v := a.lockedAgg
	timer := v.aggregation
	require.Equal(t, int64(3), timer.Count())
	require.InEpsilon(t, 36.6, timer.Sum(), 1e-10)
	require.Equal(t, 12.2, timer.Quantile(0.5))

	// Add another metric at slightly different time but still within the
	// same aggregation interval with a different source.
	require.NoError(t, e.AddUnique(testTimestamps[1],
		aggregated.ForwardedMetric{Values: []float64{14.4}},
		metadata.ForwardMetadata{SourceID: 4}))
	require.Equal(t, 1, len(e.values))
	a, err = e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v = a.lockedAgg
	timer = v.aggregation
	require.Equal(t, int64(4), timer.Count())
	require.InEpsilon(t, 51, timer.Sum(), 1e-10)

	// Add the metric in the next aggregation interval.
	require.NoError(t, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{20.0}},
		metadata.ForwardMetadata{SourceID: 1}))
	require.Equal(t, 2, len(e.values))
	a1, err := e.find(xtime.UnixNano(testAlignedStarts[1]))
	require.NoError(t, err)
	v1 := a1.lockedAgg
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	require.Equal(t, 20.0, v1.aggregation.Sum())
	require.Equal(t, int64(1), v1.aggregation.Count())
	require.Equal(t, 20.0, v1.aggregation.Sum())

	// Add the metric in the same aggregation interval with the same
	// source results in an error.
	require.Equal(t, errDuplicateForwardingSource, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{30.0}},
		metadata.ForwardMetadata{SourceID: 1}))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	require.Equal(t, 20.0, v1.aggregation.Sum())
	require.Equal(t, int64(1), v1.aggregation.Count())
	require.InEpsilon(t, 400.0, v1.aggregation.SumSq(), 1e-10)
	versions, seen := v1.sourcesSeen[1]
	require.True(t, seen)
	require.True(t, versions.Test(0))

	// Adding the timer metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{100}},
		metadata.ForwardMetadata{SourceID: 3}))
}

func TestTimerElemConsumeDefaultAggregationDefaultPipeline(t *testing.T) {
	// Set up stream options.
	streamOpts := cm.NewOptions()
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos

	// Verify the pool is big enough to supply all the streams.
	opts := newTestOptions().SetStreamOptions(streamOpts)
	e := testTimerElem(testAlignedStarts[:len(testAlignedStarts)-1], testBatchTimerVals, maggregation.DefaultTypes, applied.DefaultPipeline, opts)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, expectedLocalMetricsForTimer(testAlignedStarts[1], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, expectedLocalMetricsForTimer(testAlignedStarts[2], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))
}

func TestTimerElemConsumeCustomAggregationDefaultPipeline(t *testing.T) {
	// Set up stream options.
	streamOpts := cm.NewOptions()
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos

	// Verify the pool is big enough to supply all the streams.
	opts := newTestOptions().SetStreamOptions(streamOpts)
	e := testTimerElem(testAlignedStarts[:len(testAlignedStarts)-1], testBatchTimerVals, testTimerAggregationTypes, applied.DefaultPipeline, opts)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()

	require.False(t, e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, expectedLocalMetricsForTimer(testAlignedStarts[1], testStoragePolicy, testTimerAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, expectedLocalMetricsForTimer(testAlignedStarts[2], testStoragePolicy, testTimerAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))
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
	opts := newTestOptions().SetDiscardNaNAggregatedValues(false)
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
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
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
	require.False(t, e.Consume(alignedstartAtNanos[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 3, len(e.values))
	require.Len(t, e.flushState, 1)
	fState := e.flushState[xtime.UnixNano(alignedstartAtNanos[0])]
	consumedVal := fState.consumedValues
	require.Len(t, consumedVal, 1)
	require.Equal(t, 123.0, consumedVal[0])

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
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	expectedOnFlushedRes[0].expiredTimes = append(expectedOnFlushedRes[0].expiredTimes,
		xtime.ToUnixNano(time.Unix(220, 0)), xtime.ToUnixNano(time.Unix(230, 0)))
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 1, len(e.values))
	require.Len(t, e.flushState, 1)
	fState = e.flushState[xtime.UnixNano(alignedstartAtNanos[2])]
	consumedVal = fState.consumedValues
	require.Len(t, consumedVal, 1)
	require.Equal(t, 589.0, consumedVal[0])

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	expectedOnFlushedRes[0].expiredTimes = nil
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 1, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))
}

func TestTimerElemClose(t *testing.T) {
	// Set up stream options.
	streamOpts := cm.NewOptions()

	// Verify the pool is big enough to supply all the streams.
	opts := newTestOptions().SetStreamOptions(streamOpts)
	e := testTimerElem(testAlignedStarts[:len(testAlignedStarts)-1], testBatchTimerVals, maggregation.DefaultTypes, applied.DefaultPipeline, opts)

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
	require.Equal(t, 0, len(e.flushState))
	require.NotNil(t, e.values)
}

func TestTimerFindOrCreateNoSourceSet(t *testing.T) {
	e, err := NewTimerElem(testTimerElemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	inputs := []xtime.UnixNano{10, 10, 20, 10, 15}
	expected := make(map[xtime.UnixNano]bool)
	for _, inputNanos := range inputs {
		expected[inputNanos] = true
		res, err := e.findOrCreate(int64(inputNanos), createAggregationOptions{initSourceSet: false})
		require.NoError(t, err)
		require.Equal(t, e.values[inputNanos].lockedAgg, res)
	}
}

func TestTimerFindOrCreateWithSourceSet(t *testing.T) {
	e, err := NewTimerElem(testTimerElemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)
	e.cachedSourceSets = make([]map[uint32]*bitset.BitSet, 0)

	inputs := []xtime.UnixNano{10, 20}
	expected := make(map[xtime.UnixNano]bool)
	for _, inputNanos := range inputs {
		expected[inputNanos] = true
		res, err := e.findOrCreate(int64(inputNanos), createAggregationOptions{initSourceSet: true})
		require.NoError(t, err)
		require.Equal(t, e.values[inputNanos].lockedAgg, res)
		require.NotNil(t, e.values[inputNanos].lockedAgg.sourcesSeen)
		require.Equal(t, len(expected), len(e.values))
		for k := range e.values {
			_, ok := expected[k]
			require.True(t, ok)
		}
	}
	require.Equal(t, 0, len(e.cachedSourceSets))
}

func TestGaugeResetSetData(t *testing.T) {
	opts := newTestOptions()
	ge, err := NewGaugeElem(testGaugeElemData, NewElemOptions(opts))
	require.NoError(t, err)
	require.Equal(t, opts.AggregationTypesOptions().DefaultGaugeAggregationTypes(), ge.aggTypes)
	require.True(t, ge.useDefaultAggregation)
	require.False(t, ge.aggOpts.HasExpensiveAggregations)

	// Reset element with a default pipeline.
	elemData := testGaugeElemData
	elemData.Pipeline = applied.DefaultPipeline
	elemData.AggTypes = testAggregationTypesExpensive
	err = ge.ResetSetData(elemData)
	require.NoError(t, err)
	require.Equal(t, testGaugeID, ge.id)
	require.Equal(t, testStoragePolicy, ge.sp)
	require.Equal(t, testAggregationTypesExpensive, ge.aggTypes)
	require.Equal(t, parsedPipeline{}, ge.parsedPipeline)
	require.False(t, ge.tombstoned)
	require.False(t, ge.closed)
	require.False(t, ge.useDefaultAggregation)
	require.True(t, ge.aggOpts.HasExpensiveAggregations)
	require.Empty(t, ge.flushState)

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
	elemData.Pipeline = testPipeline
	err = ge.ResetSetData(elemData)
	require.NoError(t, err)
	requirePipelinesMatch(t, expectedParsedPipeline, ge.parsedPipeline)
	require.Empty(t, ge.flushState)
}

func TestGaugeElemAddUnion(t *testing.T) {
	e, err := NewGaugeElem(testGaugeElemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	// Add a gauge metric.
	require.NoError(t, e.AddUnion(testTimestamps[0], testGauge, false))
	require.Equal(t, 1, len(e.values))
	a, err := e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v := a.lockedAgg
	require.Equal(t, testGauge.GaugeVal, v.aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, v.aggregation.Sum())
	require.Equal(t, 0.0, v.aggregation.SumSq())

	// Add the gauge metric at slightly different time
	// but still within the same aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[1], testGauge, false))
	require.Equal(t, 1, len(e.values))
	a, err = e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v = a.lockedAgg
	require.Equal(t, testGauge.GaugeVal, v.aggregation.Last())
	require.Equal(t, 2*testGauge.GaugeVal, v.aggregation.Sum())
	require.Equal(t, 0.0, v.aggregation.SumSq())

	// Add the gauge metric in the next aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[2], testGauge, false))
	require.Equal(t, 2, len(e.values))
	a1, err := e.find(xtime.UnixNano(testAlignedStarts[1]))
	require.NoError(t, err)
	v1 := a1.lockedAgg
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	require.Equal(t, testGauge.GaugeVal, v1.aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, v1.aggregation.Sum())
	require.Equal(t, 0.0, v1.aggregation.SumSq())

	// Adding the gauge metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnion(testTimestamps[2], testGauge, false))
}

func TestGaugeElemAddUnionWithCustomAggregation(t *testing.T) {
	elemData := testGaugeElemData
	elemData.AggTypes = testAggregationTypesExpensive
	e, err := NewGaugeElem(elemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	// Add a gauge metric.
	require.NoError(t, e.AddUnion(testTimestamps[0], testGauge, false))
	require.Equal(t, 1, len(e.values))
	a, err := e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v := a.lockedAgg
	require.Equal(t, testGauge.GaugeVal, v.aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, v.aggregation.Sum())
	require.Equal(t, testGauge.GaugeVal, v.aggregation.Mean())
	require.Equal(t, testGauge.GaugeVal, v.aggregation.Sum())
	require.Equal(t, testGauge.GaugeVal*testGauge.GaugeVal, v.aggregation.SumSq())

	// Add the gauge metric at slightly different time
	// but still within the same aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[1], testGauge, false))
	require.Equal(t, 1, len(e.values))
	a, err = e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v = a.lockedAgg
	require.Equal(t, testGauge.GaugeVal, v.aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, v.aggregation.Max())
	require.Equal(t, 2*testGauge.GaugeVal, v.aggregation.Sum())
	require.Equal(t, 2*testGauge.GaugeVal*testGauge.GaugeVal, v.aggregation.SumSq())

	// Add the gauge metric in the next aggregation interval.
	require.NoError(t, e.AddUnion(testTimestamps[2], testGauge, false))
	require.Equal(t, 2, len(e.values))
	a1, err := e.find(xtime.UnixNano(testAlignedStarts[1]))
	require.NoError(t, err)
	v1 := a1.lockedAgg
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	require.Equal(t, testGauge.GaugeVal, v1.aggregation.Last())
	require.Equal(t, testGauge.GaugeVal, v1.aggregation.Max())

	// Adding the gauge metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnion(testTimestamps[2], testGauge, false))
}

func TestGaugeElemAddUnique(t *testing.T) {
	e, err := NewGaugeElem(testGaugeElemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	// Add a metric.
	source1 := uint32(1234)
	require.NoError(t, e.AddUnique(testTimestamps[0],
		aggregated.ForwardedMetric{Values: []float64{12.3, 34.5}},
		metadata.ForwardMetadata{SourceID: source1}))
	require.Equal(t, 1, len(e.values))
	a, err := e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v := a.lockedAgg
	require.Equal(t, 46.8, v.aggregation.Sum())
	require.Equal(t, int64(2), v.aggregation.Count())
	require.Equal(t, 0.0, v.aggregation.SumSq())
	require.Nil(t, v.aggregation.Annotation())
	versions, seen := v.sourcesSeen[source1]
	require.True(t, seen)
	require.True(t, versions.Test(0))

	// Add another metric at slightly different time but still within the
	// same aggregation interval with a different source.
	source2 := uint32(5678)
	require.NoError(t, e.AddUnique(testTimestamps[1],
		aggregated.ForwardedMetric{Values: []float64{50}, Annotation: testAnnot},
		metadata.ForwardMetadata{SourceID: source2}))
	require.Equal(t, 1, len(e.values))
	a, err = e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v = a.lockedAgg
	require.Equal(t, 96.8, v.aggregation.Sum())
	require.Equal(t, int64(3), v.aggregation.Count())
	require.Equal(t, 0.0, v.aggregation.SumSq())
	require.Equal(t, testAnnot, v.aggregation.Annotation())
	versions, seen = v.sourcesSeen[source2]
	require.True(t, seen)
	require.True(t, versions.Test(0))

	// Add the metric in the next aggregation interval.
	require.NoError(t, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{27.8}},
		metadata.ForwardMetadata{SourceID: source1}))
	require.Equal(t, testAnnot, v.aggregation.Annotation())
	require.Equal(t, 2, len(e.values))
	a1, err := e.find(xtime.UnixNano(testAlignedStarts[1]))
	require.NoError(t, err)
	v1 := a1.lockedAgg
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	require.Equal(t, 27.8, v1.aggregation.Sum())
	require.Equal(t, int64(1), v1.aggregation.Count())
	require.Equal(t, 0.0, v1.aggregation.SumSq())
	versions, seen = v1.sourcesSeen[source1]
	require.True(t, seen)
	require.True(t, versions.Test(0))

	// Add the gauge metric in the same aggregation interval with the same
	// source results in an error.
	require.Equal(t, errDuplicateForwardingSource, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{27.8}},
		metadata.ForwardMetadata{SourceID: source1}))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	require.Equal(t, 27.8, v1.aggregation.Sum())
	require.Equal(t, int64(1), v1.aggregation.Count())
	require.Equal(t, 0.0, v1.aggregation.SumSq())
	versions, seen = v1.sourcesSeen[source1]
	require.True(t, seen)
	require.True(t, versions.Test(0))

	// Update the value from the previous aggregation interval
	// ensure out of order versions are handled correctly.
	require.NoError(t, e.AddUnique(testTimestamps[0],
		aggregated.ForwardedMetric{Values: []float64{11.0, 33.0}, PrevValues: []float64{12.0, 34.0}, Version: 2},
		metadata.ForwardMetadata{SourceID: source1}))
	require.NoError(t, e.AddUnique(testTimestamps[0],
		aggregated.ForwardedMetric{Values: []float64{12.0, 34.0}, PrevValues: []float64{12.3, 34.5}, Version: 1},
		metadata.ForwardMetadata{SourceID: source1}))
	// sum changes from 96.8 -> 94.0
	require.Equal(t, 94.0, v.aggregation.Sum())
	require.Equal(t, int64(3), v.aggregation.Count())
	require.Equal(t, 0.0, v.aggregation.SumSq())
	require.Equal(t, testAnnot, v.aggregation.Annotation())
	versions, seen = v.sourcesSeen[source1]
	require.True(t, seen)
	require.True(t, versions.Test(1))
	require.True(t, versions.Test(2))

	// Adding the gauge metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{10.0}},
		metadata.ForwardMetadata{SourceID: 3}))
}

func TestGaugeElemAddUniqueWithCustomAggregation(t *testing.T) {
	elemData := testGaugeElemData
	elemData.AggTypes = testAggregationTypesExpensive
	e, err := NewGaugeElem(elemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	// Add a gauge metric.
	source1 := uint32(1234)
	require.NoError(t, e.AddUnique(testTimestamps[0],
		aggregated.ForwardedMetric{Values: []float64{1.2}},
		metadata.ForwardMetadata{SourceID: source1}))
	require.Equal(t, 1, len(e.values))
	a, err := e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v := a.lockedAgg
	require.Equal(t, 1.2, v.aggregation.Sum())
	require.Equal(t, 1.2, v.aggregation.Max())
	require.Equal(t, 1.44, v.aggregation.SumSq())
	versions, seen := v.sourcesSeen[source1]
	require.True(t, seen)
	require.True(t, versions.Test(0))

	// Add the gauge metric at slightly different time
	// but still within the same aggregation interval.
	source2 := uint32(5678)
	require.NoError(t, e.AddUnique(testTimestamps[1],
		aggregated.ForwardedMetric{Values: []float64{1.4}},
		metadata.ForwardMetadata{SourceID: source2}))
	require.Equal(t, 1, len(e.values))
	a, err = e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v = a.lockedAgg
	require.InEpsilon(t, 2.6, v.aggregation.Sum(), 1e-10)
	require.Equal(t, 1.4, v.aggregation.Max())

	// Add the gauge metric in the next aggregation interval.
	require.NoError(t, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{2.0}},
		metadata.ForwardMetadata{SourceID: source1}))
	require.Equal(t, 2, len(e.values))
	a1, err := e.find(xtime.UnixNano(testAlignedStarts[1]))
	require.NoError(t, err)
	v1 := a1.lockedAgg
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	require.Equal(t, 2.0, v1.aggregation.Sum())
	require.Equal(t, 2.0, v1.aggregation.Max())
	require.Equal(t, 4.0, v1.aggregation.SumSq())

	// Add the gauge metric in the same aggregation interval with the same
	// source results in an error.
	require.Equal(t, errDuplicateForwardingSource, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{3.0}},
		metadata.ForwardMetadata{SourceID: source1}))
	require.Equal(t, 2, len(e.values))
	for i := 0; i < len(e.values); i++ {
		require.Equal(t, xtime.UnixNano(testAlignedStarts[i]), e.values[e.dirty[i]].startAt)
	}
	require.Equal(t, 2.0, v1.aggregation.Sum())
	require.Equal(t, 2.0, v1.aggregation.Max())
	require.Equal(t, 4.0, v1.aggregation.SumSq())
	versions, seen = v1.sourcesSeen[source1]
	require.True(t, seen)
	require.True(t, versions.Test(0))

	// Adding the gauge metric to a closed element results in an error.
	e.closed = true
	require.Equal(t, errElemClosed, e.AddUnique(testTimestamps[2],
		aggregated.ForwardedMetric{Values: []float64{4.0}},
		metadata.ForwardMetadata{SourceID: 3}))
}

func TestGaugeElemConsumeDefaultAggregationDefaultPipeline(t *testing.T) {
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos
	opts := newTestOptions()
	e := testGaugeElem(testAlignedStarts[:len(testAlignedStarts)-1], testGaugeVals, maggregation.DefaultTypes, applied.DefaultPipeline, opts)

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, expectedLocalMetricsForGauge(testAlignedStarts[1], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, expectedLocalMetricsForGauge(testAlignedStarts[2], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))
}

func TestGaugeElemConsumeResendBuffer(t *testing.T) {
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos
	opts := newTestOptions()
	pastBuffer := time.Second * 20
	opts = opts.
		SetInstrumentOptions(instrument.NewOptions()).
		SetBufferForPastTimedMetricFn(func(resolution time.Duration) time.Duration {
			return resolution + pastBuffer
		})
	pipeline := applied.NewPipeline([]applied.OpUnion{{
		Type: pipeline.TransformationOpType,
		Transformation: pipeline.TransformationOp{
			Type: transformation.Increase,
		},
	}})

	elemData := testGaugeData
	elemData.Pipeline = pipeline
	e := testGaugeElemWithData(t, testAlignedStarts[:len(testAlignedStarts)-1], testGaugeVals, elemData, opts,
		true)

	// Consume one value.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t,
		e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
			localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t,
		expectedLocalMetricsForGauge(testAlignedStarts[1], testStoragePolicy, maggregation.DefaultTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	a, err := e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v := a.lockedAgg
	a1, err := e.find(xtime.UnixNano(testAlignedStarts[1]))
	v1 := a1.lockedAgg
	require.NoError(t, err)
	require.False(t, v.dirty)
	require.True(t, e.flushState[xtime.UnixNano(testAlignedStarts[0])].flushed)
	require.True(t, v1.dirty)
	require.False(t, e.flushState[xtime.UnixNano(testAlignedStarts[1])].flushed)

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t,
		e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
			localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t,
		expectedLocalMetricsForGaugeWithVal(testAlignedStarts[2], 0.0, testStoragePolicy,
			maggregation.DefaultTypes),
		*localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))
	require.False(t, v.dirty)
	require.True(t, e.flushState[xtime.UnixNano(testAlignedStarts[0])].flushed)
	require.False(t, v1.dirty)
	require.True(t, e.flushState[xtime.UnixNano(testAlignedStarts[1])].flushed)

	// Update the first value after flushing
	updatedVal := testGaugeVals[0] - 1.0
	mu := unaggregated.MetricUnion{
		GaugeVal: updatedVal,
	}
	require.NoError(t, e.AddUnion(time.Unix(0, testAlignedStarts[0]), mu, true))
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t,
		e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
			localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	// the update cascades, resulting in 2 flushed values.
	exp := expectedLocalMetricsForGaugeWithVal(testAlignedStarts[1], updatedVal, testStoragePolicy,
		maggregation.DefaultTypes)
	exp = append(exp,
		expectedLocalMetricsForGaugeWithVal(testAlignedStarts[2], 1.0, testStoragePolicy,
			maggregation.DefaultTypes)...)
	require.Equal(t, exp, *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))
	require.False(t, v.dirty)
	require.True(t, e.flushState[xtime.UnixNano(testAlignedStarts[0])].flushed)
	require.False(t, v1.dirty)
	require.True(t, e.flushState[xtime.UnixNano(testAlignedStarts[1])].flushed)

	// expire the values past the buffer.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	pastBufferTime := pastBuffer + testStoragePolicy.Resolution().Window
	require.False(t, e.Consume(time.Unix(0, testAlignedStarts[1]).Add(pastBufferTime).UnixNano(),
		isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(time.Unix(0, testAlignedStarts[2]).Add(pastBufferTime).UnixNano(),
		isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))
}

func TestGaugeElemConsumeCustomAggregationDefaultPipeline(t *testing.T) {
	opts := newTestOptions()
	e := testGaugeElem(testAlignedStarts[:len(testAlignedStarts)-1], testGaugeVals, testAggregationTypes, applied.DefaultPipeline, opts)
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos

	// Consume values before an early-enough time.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, expectedLocalMetricsForGauge(testAlignedStarts[1], testStoragePolicy, testAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, expectedLocalMetricsForGauge(testAlignedStarts[2], testStoragePolicy, testAggregationTypes), *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(testAlignedStarts[2], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))
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
	opts := newTestOptions().SetDiscardNaNAggregatedValues(false)
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
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
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
	require.False(t, e.Consume(alignedstartAtNanos[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 3, len(e.values))
	require.Len(t, e.flushState, 1)
	flushedState := e.flushState[xtime.UnixNano(alignedstartAtNanos[0])]
	consumedVal := flushedState.consumedValues
	require.Len(t, consumedVal, 1)
	require.Equal(t, 123.0, consumedVal[0])

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
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	expectedOnFlushedRes[0].expiredTimes = append(expectedOnFlushedRes[0].expiredTimes,
		xtime.ToUnixNano(time.Unix(220, 0)), xtime.ToUnixNano(time.Unix(230, 0)))
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 1, len(e.values))
	require.Len(t, e.flushState, 1)
	flushedState = e.flushState[xtime.UnixNano(alignedstartAtNanos[2])]
	consumedVal = flushedState.consumedValues
	require.Len(t, consumedVal, 1)
	require.Equal(t, 589.0, consumedVal[0])

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.True(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	expectedOnFlushedRes[0].expiredTimes = nil
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 1, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))
}

func TestGaugeElemResendSumReset(t *testing.T) {
	alignedstartAtNanos := []int64{
		time.Unix(210, 0).UnixNano(),
		time.Unix(220, 0).UnixNano(),
	}
	gaugeVals := []float64{123.0, 246.0}
	aggregationTypes := maggregation.Types{maggregation.Sum}
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos
	pastBuffer := time.Second * 30
	opts := newTestOptions().
		SetBufferForPastTimedMetricFn(func(resolution time.Duration) time.Duration {
			return resolution + pastBuffer
		})
	pipe := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Reset},
		},
	})
	data := testGaugeData
	data.Pipeline = pipe
	data.AggTypes = aggregationTypes
	e := testGaugeElemWithData(t, alignedstartAtNanos, gaugeVals, data, opts, true)
	e.numForwardedTimes = 1

	// Consume one value.
	localFn, localRes := testFlushLocalMetricFn()
	forwardFn, forwardRes := testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes := testOnForwardedFlushedFn()
	require.False(t,
		e.Consume(alignedstartAtNanos[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
			localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	exp := expectedLocalMetricsForGaugeWithVal(
		alignedstartAtNanos[1], 123.0, testStoragePolicy, maggregation.DefaultTypes)
	exp = append(exp, expectedLocalMetricsForGaugeWithVal(
		time.Unix(225, 0).UnixNano(), 0.0, testStoragePolicy, maggregation.DefaultTypes)...)
	require.Equal(t, exp, *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))
	a, err := e.find(xtime.UnixNano(testAlignedStarts[0]))
	require.NoError(t, err)
	v := a.lockedAgg
	a1, err := e.find(xtime.UnixNano(testAlignedStarts[1]))
	v1 := a1.lockedAgg
	require.NoError(t, err)
	require.False(t, v.dirty)
	require.True(t, e.flushState[xtime.UnixNano(testAlignedStarts[0])].flushed)
	require.True(t, v1.dirty)
	require.False(t, e.flushState[xtime.UnixNano(testAlignedStarts[1])].flushed)

	// Update the first value after flushing
	require.NoError(t, e.AddUnique(time.Unix(0, testAlignedStarts[0]), aggregated.ForwardedMetric{
		Values:     []float64{122},
		PrevValues: []float64{123},
		Version:    1,
	}, metadata.ForwardMetadata{
		ResendEnabled: true,
		SourceID:      1,
	}))
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t,
		e.Consume(testAlignedStarts[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
			localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	exp = expectedLocalMetricsForGaugeWithVal(
		alignedstartAtNanos[1], 122.0, testStoragePolicy, maggregation.DefaultTypes)
	exp = append(exp, expectedLocalMetricsForGaugeWithVal(
		time.Unix(225, 0).UnixNano(), 0.0, testStoragePolicy, maggregation.DefaultTypes)...)
	require.Equal(t, exp, *localRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 2, len(e.values))
	require.False(t, v.dirty)
	require.True(t, e.flushState[xtime.UnixNano(testAlignedStarts[0])].flushed)
	require.True(t, v1.dirty)
	require.False(t, e.flushState[xtime.UnixNano(testAlignedStarts[1])].flushed)
}

func TestGaugeElemResendBufferForwarding(t *testing.T) {
	alignedstartAtNanos := []int64{
		time.Unix(210, 0).UnixNano(),
		time.Unix(220, 0).UnixNano(),
		time.Unix(230, 0).UnixNano(),
		time.Unix(240, 0).UnixNano(),
	}
	gaugeVals := []float64{123.0, 456.0, 589.0}
	aggregationTypes := maggregation.Types{maggregation.Last}
	isEarlierThanFn := isStandardMetricEarlierThan
	timestampNanosFn := standardMetricTimestampNanos
	bufferPast := time.Second * 30
	opts := newTestOptions().
		SetBufferForPastTimedMetricFn(func(resolution time.Duration) time.Duration {
			return resolution + bufferPast
		})
	sumRollup := applied.OpUnion{
		Type: pipeline.RollupOpType,
		Rollup: applied.RollupOp{
			ID:            []byte("foo.bar"),
			AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
		},
	}
	pipe := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Increase},
		},
		sumRollup,
	})
	data := testGaugeData
	data.Pipeline = pipe
	data.AggTypes = aggregationTypes
	e := testGaugeElemWithData(t, alignedstartAtNanos[:3], gaugeVals, data, opts, true)

	aggKey := aggregationKey{
		aggregationID:     maggregation.MustCompressTypes(maggregation.Sum),
		storagePolicy:     testStoragePolicy,
		numForwardedTimes: 1,
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
	require.False(t,
		e.Consume(0, isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
			localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 3, len(e.values))

	// Consume one value.
	expectedForwardedRes := []testForwardedMetricWithMetadata{
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(220, 0).UnixNano(),
			value:          123.0,
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t,
		e.Consume(alignedstartAtNanos[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
			localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 3, len(e.values))
	require.Len(t, e.flushState, 1)
	fState := e.flushState[xtime.UnixNano(alignedstartAtNanos[0])]
	consumedVal := fState.consumedValues
	require.Len(t, consumedVal, 1)
	require.Equal(t, 123.0, consumedVal[0])

	// Consume all values.
	expectedForwardedRes = []testForwardedMetricWithMetadata{
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(230, 0).UnixNano(),
			value:          333,
		},
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(240, 0).UnixNano(),
			value:          133,
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t,
		e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
			localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 3, len(e.values))
	require.Len(t, e.flushState, 3)
	require.Equal(t, 123.0, e.flushState[xtime.ToUnixNano(time.Unix(210, 0))].consumedValues[0])
	require.Equal(t, 456.0, e.flushState[xtime.ToUnixNano(time.Unix(220, 0))].consumedValues[0])
	require.Equal(t, 589.0, e.flushState[xtime.ToUnixNano(time.Unix(230, 0))].consumedValues[0])

	// Update a previous value
	mu := unaggregated.MetricUnion{
		GaugeVal: 124.0,
	}
	require.NoError(t, e.AddUnion(time.Unix(210, 0), mu, true))
	expectedForwardedRes = []testForwardedMetricWithMetadata{
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(220, 0).UnixNano(),
			value:          124,
		},
		{
			aggregationKey: aggKey,
			timeNanos:      time.Unix(230, 0).UnixNano(),
			value:          332,
		},
	}
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t,
		e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
			localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	verifyForwardedMetrics(t, expectedForwardedRes, *forwardRes)
	verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 3, len(e.values))
	require.Len(t, e.flushState, 3)
	require.Equal(t, 124.0, e.flushState[xtime.ToUnixNano(time.Unix(210, 0))].consumedValues[0])
	require.Equal(t, 456.0, e.flushState[xtime.ToUnixNano(time.Unix(220, 0))].consumedValues[0])
	require.Equal(t, 589.0, e.flushState[xtime.ToUnixNano(time.Unix(230, 0))].consumedValues[0])
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
	opts := newTestOptions().SetDiscardNaNAggregatedValues(false)

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
	require.False(t, e.Consume(0, isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 3, len(e.values))

	// Consume one value.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[1], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, (*localRes)[0].timeNanos, alignedstartAtNanos[1])
	require.Equal(t, (*localRes)[0].value, 123.0)
	require.Equal(t, (*localRes)[1].timeNanos, alignedstartAtNanos[1]+int64(5*time.Second))
	require.Equal(t, (*localRes)[1].value, 0.0)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 3, len(e.values))
	for _, f := range e.flushState {
		require.Equal(t, 0, len(f.consumedValues))
	}

	// Consume all values.
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, (*localRes)[0].timeNanos, alignedstartAtNanos[2])
	require.Equal(t, (*localRes)[0].value, 456.0)
	require.Equal(t, (*localRes)[1].timeNanos, alignedstartAtNanos[2]+int64(5*time.Second))
	require.Equal(t, (*localRes)[1].value, 0.0)
	require.Equal(t, (*localRes)[2].timeNanos, alignedstartAtNanos[3])
	require.Equal(t, (*localRes)[2].value, 589.0)
	require.Equal(t, (*localRes)[3].timeNanos, alignedstartAtNanos[3]+int64(5*time.Second))
	require.Equal(t, (*localRes)[3].value, 0.0)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))
	for _, f := range e.flushState {
		require.Equal(t, 0, len(f.consumedValues))
	}

	// Tombstone the element and discard all values.
	e.tombstoned = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, _ = testOnForwardedFlushedFn()
	require.True(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	// verifyOnForwardedFlushResult(t, expectedOnFlushedRes, *onForwardedFlushedRes)
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 1, len(e.values))

	// Reading and discarding values from a closed element is no op.
	e.closed = true
	localFn, localRes = testFlushLocalMetricFn()
	forwardFn, forwardRes = testFlushForwardedMetricFn()
	onForwardedFlushedFn, onForwardedFlushedRes = testOnForwardedFlushedFn()
	require.False(t, e.Consume(alignedstartAtNanos[3], isEarlierThanFn, timestampNanosFn, standardMetricTargetNanos,
		localFn, forwardFn, onForwardedFlushedFn, 0, consumeType))
	require.Equal(t, 0, len(*localRes))
	require.Equal(t, 0, len(*forwardRes))
	require.Equal(t, 0, len(*onForwardedFlushedRes))
	require.Equal(t, 1, len(e.values))
}

func TestGaugeElemClose(t *testing.T) {
	e := testGaugeElem(testAlignedStarts[:len(testAlignedStarts)-1], testGaugeVals,
		maggregation.DefaultTypes, applied.DefaultPipeline, newTestOptions())
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
	for _, f := range e.flushState {
		require.Equal(t, 0, len(f.consumedValues))
	}
	require.NotNil(t, e.values)
}

func TestGaugeFindOrCreateNoSourceSet(t *testing.T) {
	e, err := NewGaugeElem(testGaugeElemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	inputs := []xtime.UnixNano{10, 10, 20, 10, 15}
	expected := make(map[xtime.UnixNano]bool)
	for _, inputNanos := range inputs {
		expected[inputNanos] = true
		res, err := e.findOrCreate(int64(inputNanos), createAggregationOptions{initSourceSet: false})
		require.NoError(t, err)
		require.Equal(t, e.values[inputNanos].lockedAgg, res)
		require.Equal(t, len(expected), len(e.values))
		for k := range e.values {
			_, ok := expected[k]
			require.True(t, ok)
		}
	}
}

func TestDirtyConsumption(t *testing.T) {
	e, err := NewCounterElem(testCounterElemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)

	resolution := e.sp.Resolution().Window

	// Test 100 startAligned timestamps (and targetNanos is far enough to consume any of them)
	n := int64(100)
	targetNanos := resolution.Nanoseconds() * (n + 1)

	var (
		wg sync.WaitGroup
		// time by consume count
		consumed = make(map[xtime.UnixNano]int)
	)
	stop := atomic.NewBool(false)
	for i := int64(0); i < n; i++ {
		i := i
		wg.Add(1)

		// For each timestamp, repeatedly add an aggregation (will findOrCreate and make dirty)
		// and then consume the presently dirty points. This validates that new times are consumed
		// once added, and existing times are made dirty to be re-consumed.
		go func() {
			for !stop.Load() {
				require.NoError(t, e.AddUnion(time.Unix(0, i*resolution.Nanoseconds()), testCounter, false))

				e.Lock()
				e.dirtyToConsumeWithLock(targetNanos, resolution, isStandardMetricEarlierThan)

				for _, c := range e.toConsume {
					consumed[c.startAt]++
				}
				e.Unlock()
			}
			wg.Done()
		}()
	}

	time.Sleep(time.Second * 2)
	stop.Store(true)
	wg.Wait()

	require.Equal(t, 0, len(e.dirty), "all dirty should have been consumed")
	require.Equal(t, int(n), len(e.values), "values should have all N consumed times")
	require.Equal(t, int(n), len(consumed), "consumed should have all N consumed times")

	for _, count := range consumed {
		// Ensure times were consumed at least (a) upon first creation and (b) upon subsequent dirty
		require.True(t, count > 1)
	}
}

// confirms that when panics are enabled, we do panic, and when disabled,
// the code safely proceeds (and does not encounter some unexpected runtime panic)
func TestPanics(t *testing.T) {
	opts := newTestOptions().SetInstrumentOptions(instrument.NewOptions())
	elemData := testCounterElemData

	tests := []struct {
		name   string
		testFn func()
	}{
		{
			name: "panic if non-existent flush states are expired",
			testFn: func() {
				elem, err := NewCounterElem(elemData, NewElemOptions(opts))
				require.NoError(t, err)
				elem.flushStateToExpire = []xtime.UnixNano{0, 1, 2}
				elem.expireFlushState()
			},
		},
		{
			name: "panic if minStartTime moved beyond remaining values on Close",
			testFn: func() {
				elem, err := NewCounterElem(elemData, NewElemOptions(opts))
				require.NoError(t, err)
				elem.flushStateToExpire = []xtime.UnixNano{0, 1, 2}

				// Panic case where minStartTime > min(elem.values)
				elem.values[10] = timedCounter{
					lockedAgg: &lockedCounterAggregation{
						aggregation: newCounterAggregation(raggregation.NewCounter(elem.aggOpts)),
					},
				}
				elem.minStartTime = 11

				elem.Close()
			},
		},
		{
			name: "panic from dangling flushState on Close",
			testFn: func() {
				elem, err := NewCounterElem(elemData, NewElemOptions(opts))
				require.NoError(t, err)
				elem.flushStateToExpire = []xtime.UnixNano{0, 1, 2}

				// i.e. flushState to cleanup but no associated elem.values
				elem.flushState[10] = flushState{}

				elem.Close()
			},
		},
		{
			name: "panic if invalid flush state",
			testFn: func() {
				elem, err := NewCounterElem(elemData, NewElemOptions(opts))
				require.NoError(t, err)

				counter := timedCounter{
					lockedAgg: &lockedCounterAggregation{
						aggregation: newCounterAggregation(raggregation.NewCounter(elem.aggOpts)),
					},
				}

				elem.toConsume, _ = elem.appendConsumeStateWithLock(counter, elem.toConsume, nil)
				toConsume := elem.toConsume[0]
				toConsume.startAt = xtime.UnixNano(10)

				// Panic case is where dirty + flushed + !resendEnabled since only resendEnabled
				// allows for updating dirty/flushed aggs.
				toConsume.dirty = true
				toConsume.resendEnabled = false
				elem.flushState[toConsume.startAt] = flushState{
					flushed: true,
				}

				localFn, _ := testFlushLocalMetricFn()
				forwardFn, _ := testFlushForwardedMetricFn()
				elem.processValue(toConsume,
					standardMetricTimestampNanos,
					localFn,
					forwardFn,
					0, 0, 0,
					newFlushMetrics(tally.NewTestScope("", nil)))
			},
		},
		{
			name: "panic if no consumedValues",
			testFn: func() {
				elemData := testCounterElemData
				elemData.Pipeline = testPipeline
				elem, err := NewCounterElem(elemData, NewElemOptions(opts))
				require.NoError(t, err)

				counter := timedCounter{
					lockedAgg: &lockedCounterAggregation{
						aggregation: newCounterAggregation(raggregation.NewCounter(elem.aggOpts)),
					},
				}
				elem.toConsume, _ = elem.appendConsumeStateWithLock(counter, elem.toConsume, nil)
				toConsume := elem.toConsume[0]

				// Panic case is where startAt has a flushState entry but prevStartTime does not.
				toConsume.startAt = xtime.UnixNano(10)
				toConsume.prevStartTime = xtime.UnixNano(9)
				elem.flushState[toConsume.startAt] = flushState{
					flushed: true,
				}
				elem.flushState[toConsume.startAt] = flushState{}

				localFn, _ := testFlushLocalMetricFn()
				forwardFn, _ := testFlushForwardedMetricFn()
				elem.processValue(toConsume,
					standardMetricTimestampNanos,
					localFn,
					forwardFn,
					0, 0, 0,
					newFlushMetrics(tally.NewTestScope("", nil)))
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Test panics enabled/disabled.
			instrument.SetShouldPanicEnvironmentVariable(true)
			assert.Panics(t, tt.testFn, "should panic but did not")
			instrument.SetShouldPanicEnvironmentVariable(false)
			tt.testFn()
		})
	}
}

// nolint: dupl
func TestGaugeFindOrCreateWithSourceSet(t *testing.T) {
	e, err := NewGaugeElem(testGaugeElemData, NewElemOptions(newTestOptions()))
	require.NoError(t, err)
	e.cachedSourceSets = make([]map[uint32]*bitset.BitSet, 0)

	inputs := []xtime.UnixNano{10, 20}
	expected := make(map[xtime.UnixNano]bool)
	for _, inputNanos := range inputs {
		expected[inputNanos] = true
		res, err := e.findOrCreate(int64(inputNanos), createAggregationOptions{initSourceSet: true})
		require.NoError(t, err)
		require.Equal(t, e.values[inputNanos].lockedAgg, res)
		require.NotNil(t, e.values[inputNanos].lockedAgg.sourcesSeen)
		require.Equal(t, len(expected), len(e.values))
		for k := range e.values {
			_, ok := expected[k]
			require.True(t, ok)
		}
	}
	require.Equal(t, 0, len(e.cachedSourceSets))
}

func TestExpireValues(t *testing.T) {
	opts := newTestOptions()
	resolutionDuration := opts.DefaultStoragePolicies()[0].Resolution().Window
	resolution := xtime.UnixNano(resolutionDuration)
	bufferPastDuration := opts.BufferForPastTimedMetricFn()(resolutionDuration)

	valsNoGaps := []xtime.UnixNano{
		resolution * 1,
		resolution * 2,
		resolution * 3,
	}
	valsGaps := []xtime.UnixNano{
		resolution * 1,
		resolution * 5,
		resolution * 6,
		resolution * 10,
	}

	for _, test := range []struct {
		name             string
		targetNanos      xtime.UnixNano
		resendEnabled    bool
		values           []xtime.UnixNano
		expectedToExpire []xtime.UnixNano
		expectedValues   []xtime.UnixNano
	}{
		// no gaps - resend disabled
		{
			name:             "no gaps - resend disabled - zero target",
			targetNanos:      resolution * 0,
			resendEnabled:    false,
			values:           valsNoGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsNoGaps,
		},
		{
			name:             "no gaps - resend disabled - target val 0",
			targetNanos:      valsNoGaps[0],
			resendEnabled:    false,
			values:           valsNoGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsNoGaps,
		},
		{
			name:             "no gaps - resend disabled - target val 1",
			targetNanos:      valsNoGaps[1],
			resendEnabled:    false,
			values:           valsNoGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsNoGaps,
		},
		{
			name:             "no gaps - resend disabled - target val 2",
			targetNanos:      valsNoGaps[2],
			resendEnabled:    false,
			values:           valsNoGaps,
			expectedToExpire: valsNoGaps[0:1], expectedValues: valsNoGaps[1:],
		},
		{
			name:             "no gaps - resend disabled - target val 2 + 1 resolution",
			targetNanos:      valsNoGaps[2].Add(resolutionDuration),
			resendEnabled:    false,
			values:           valsNoGaps,
			expectedToExpire: valsNoGaps[0:2], expectedValues: valsNoGaps[2:],
		},
		{
			name:             "no gaps - resend disabled - target val 2 + 2 resolution",
			targetNanos:      valsNoGaps[2].Add(2 * resolutionDuration),
			resendEnabled:    false,
			values:           valsNoGaps,
			expectedToExpire: valsNoGaps[0:2], expectedValues: valsNoGaps[2:],
		},
		// no gaps - resend enabled
		{
			name:             "no gaps - resend enabled - target zero",
			targetNanos:      resolution * 0,
			resendEnabled:    true,
			values:           valsNoGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsNoGaps,
		},
		{
			name:             "no gaps - resend enabled - target val 0",
			targetNanos:      valsNoGaps[0],
			resendEnabled:    true,
			values:           valsNoGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsNoGaps,
		},
		{
			name:          "no gaps - resend enabled - target val 1",
			targetNanos:   valsNoGaps[1],
			resendEnabled: true, values: valsNoGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsNoGaps,
		},
		{
			name:             "no gaps - resend enabled - target val 2",
			targetNanos:      valsNoGaps[2],
			resendEnabled:    true,
			values:           valsNoGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsNoGaps,
		},
		{
			name:             "no gaps - resend enabled - target val 2 + 1 resolution",
			targetNanos:      valsNoGaps[2].Add(resolutionDuration),
			resendEnabled:    true,
			values:           valsNoGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsNoGaps,
		},
		{
			name:             "no gaps - resend enabled - target val 2 + 2 resolution",
			targetNanos:      valsNoGaps[2].Add(2 * resolutionDuration),
			resendEnabled:    true,
			values:           valsNoGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsNoGaps,
		},
		{
			name:             "no gaps - resend enabled - target val 0 + buffer past",
			targetNanos:      valsNoGaps[0].Add(bufferPastDuration),
			resendEnabled:    true,
			values:           valsNoGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsNoGaps,
		},
		{
			name:             "no gaps - resend enabled - target val 1 + buffer past",
			targetNanos:      valsNoGaps[1].Add(bufferPastDuration),
			resendEnabled:    true,
			values:           valsNoGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsNoGaps,
		},
		{
			name:             "no gaps - resend enabled - target val 2 + buffer past",
			targetNanos:      valsNoGaps[2].Add(bufferPastDuration),
			resendEnabled:    true,
			values:           valsNoGaps,
			expectedToExpire: valsNoGaps[0:1], expectedValues: valsNoGaps[1:],
		},
		{
			name:             "no gaps - resend enabled - target val 2 + 1 resolution + buffer past",
			targetNanos:      valsNoGaps[2].Add(resolutionDuration).Add(bufferPastDuration),
			resendEnabled:    true,
			values:           valsNoGaps,
			expectedToExpire: valsNoGaps[0:2], expectedValues: valsNoGaps[2:],
		},
		{
			name:             "no gaps - resend enabled - target val 2 + 2 resolution + buffer past",
			targetNanos:      valsNoGaps[2].Add(2 * resolutionDuration).Add(bufferPastDuration),
			resendEnabled:    true,
			values:           valsNoGaps,
			expectedToExpire: valsNoGaps[0:2], expectedValues: valsNoGaps[2:],
		},
		// gaps - resend disabled
		{
			name:             "gaps - resend disabled - zero target",
			targetNanos:      resolution * 0,
			resendEnabled:    false,
			values:           valsGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsGaps,
		},
		{
			name:             "gaps - resend disabled - target val 0",
			targetNanos:      valsGaps[0],
			resendEnabled:    false,
			values:           valsGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsGaps,
		},
		{
			name:             "gaps - resend disabled - target val 1",
			targetNanos:      valsGaps[1],
			resendEnabled:    false,
			values:           valsGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsGaps,
		},
		{
			name:             "gaps - resend disabled - target val 2",
			targetNanos:      valsGaps[2],
			resendEnabled:    false,
			values:           valsGaps,
			expectedToExpire: valsGaps[0:1], expectedValues: valsGaps[1:],
		},
		{
			name:             "gaps - resend disabled - target between val 2 and val 3 (1 resolution)",
			targetNanos:      valsGaps[2].Add(resolutionDuration),
			resendEnabled:    false,
			values:           valsGaps,
			expectedToExpire: valsGaps[0:2], expectedValues: valsGaps[2:],
		},
		{
			name:             "gaps - resend disabled - target between val 2 and val 3 (2 resolution)",
			targetNanos:      valsGaps[2].Add(2 * resolutionDuration),
			resendEnabled:    false,
			values:           valsGaps,
			expectedToExpire: valsGaps[0:2], expectedValues: valsGaps[2:],
		},
		{
			name:             "gaps - resend disabled - target val 3",
			targetNanos:      valsGaps[3],
			resendEnabled:    false,
			values:           valsGaps,
			expectedToExpire: valsGaps[0:2], expectedValues: valsGaps[2:],
		},
		{
			name:             "gaps - resend disabled - target val 3 + 1 resolution",
			targetNanos:      valsGaps[3].Add(resolutionDuration),
			resendEnabled:    false,
			values:           valsGaps,
			expectedToExpire: valsGaps[0:3], expectedValues: valsGaps[3:],
		},
		{
			name:             "gaps - resend disabled - target val 3 + 2 resolution",
			targetNanos:      valsGaps[3].Add(2 * resolutionDuration),
			resendEnabled:    false,
			values:           valsGaps,
			expectedToExpire: valsGaps[0:3], expectedValues: valsGaps[3:],
		},
		// gaps - resend enabled
		{
			name:          "gaps - resend enabled - target zero",
			targetNanos:   resolution * 0,
			resendEnabled: true,
			values:        valsGaps, expectedToExpire: []xtime.UnixNano{}, expectedValues: valsGaps,
		},
		{
			name:             "gaps - resend enabled - target val 0",
			targetNanos:      valsGaps[0],
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsGaps,
		},
		{
			name:             "gaps - resend enabled - target val 1",
			targetNanos:      valsGaps[1],
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsGaps,
		},
		{
			name:             "gaps - resend enabled - target val 2",
			targetNanos:      valsGaps[2],
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsGaps,
		},
		{
			name:             "gaps - resend enabled - target val 3",
			targetNanos:      valsGaps[3],
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsGaps,
		},
		{
			name:             "gaps - resend enabled - target val 3 + 1 resolution",
			targetNanos:      valsGaps[3].Add(resolutionDuration),
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsGaps,
		},
		{
			name:             "gaps - resend enabled - target val 3 + 2 resolution",
			targetNanos:      valsGaps[3].Add(2 * resolutionDuration),
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsGaps,
		},
		{
			name:             "gaps - resend enabled - target val 0 + buffer past",
			targetNanos:      valsGaps[0].Add(bufferPastDuration),
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsGaps,
		},
		{
			name:             "gaps - resend enabled - target val 1 + buffer past",
			targetNanos:      valsGaps[1].Add(bufferPastDuration),
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: []xtime.UnixNano{}, expectedValues: valsGaps,
		},
		{
			name:             "gaps - resend enabled - target val 2 + buffer past",
			targetNanos:      valsGaps[2].Add(bufferPastDuration),
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: valsGaps[0:1], expectedValues: valsGaps[1:],
		},
		{
			name:             "gaps - resend enabled - target between val 2 and 3 (1 resolution) + buffer past",
			targetNanos:      valsGaps[2].Add(resolutionDuration).Add(bufferPastDuration),
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: valsGaps[0:2], expectedValues: valsGaps[2:],
		},
		{
			name:             "gaps - resend enabled - target between val 2 and 3 (2 resolution) + buffer past",
			targetNanos:      valsGaps[2].Add(2 * resolutionDuration).Add(bufferPastDuration),
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: valsGaps[0:2], expectedValues: valsGaps[2:],
		},
		{
			name:             "gaps - resend enabled - target val 3 + buffer past",
			targetNanos:      valsGaps[3].Add(bufferPastDuration),
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: valsGaps[0:2], expectedValues: valsGaps[2:],
		},
		{
			name:             "gaps - resend enabled - target val 3 + 1 resolution + buffer past",
			targetNanos:      valsGaps[3].Add(resolutionDuration).Add(bufferPastDuration),
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: valsGaps[0:3], expectedValues: valsGaps[3:],
		},
		{
			name:             "gaps - resend enabled - target val 3 + 2 resolution + buffer past",
			targetNanos:      valsGaps[3].Add(2 * resolutionDuration).Add(bufferPastDuration),
			resendEnabled:    true,
			values:           valsGaps,
			expectedToExpire: valsGaps[0:3], expectedValues: valsGaps[3:],
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			e, err := NewCounterElem(testCounterElemData, NewElemOptions(opts))
			require.NoError(t, err)
			require.Equal(t, 0, len(e.flushStateToExpire))

			// Add initial toExpire since expireValues should reset this.
			e.flushStateToExpire = make([]xtime.UnixNano, 10)

			// Add test values.
			for _, v := range test.values {
				a, err := e.findOrCreate(int64(v), createAggregationOptions{})
				a.resendEnabled = test.resendEnabled
				// need to manually seed the flush state since we don't call Consume(), which takes care of setting
				// the flush state for expireValuesWithLock to use.
				e.flushState[v] = flushState{latestResendEnabled: test.resendEnabled}
				require.NoError(t, err)
			}

			// Expire up to target.
			e.expireValuesWithLock(int64(test.targetNanos), isStandardMetricEarlierThan,
				newFlushMetrics(tally.NewTestScope("", nil)))

			// Validate toExpire and remaining values.
			require.Equal(t, len(test.expectedToExpire), len(e.flushStateToExpire))
			for i, toExpire := range test.expectedToExpire {
				require.Equal(t, toExpire, e.flushStateToExpire[i], "missing expire")
			}
			require.Equal(t, len(test.expectedValues), len(e.values))
			for _, value := range test.expectedValues {
				_, ok := e.values[value]
				require.True(t, ok, "missing value")
			}
		})
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
	expiredTimes   []xtime.UnixNano
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
		annotation []byte,
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
		prevValue float64,
		annotation []byte,
		resendEnabled bool,
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
		expiredTimes []xtime.UnixNano,
	) {
		result = append(result, testOnForwardedFlushedData{
			aggregationKey: aggregationKey,
			expiredTimes:   expiredTimes,
		})
	}, &result
}

func testCounterElem(
	alignedstartAtNanos []int64,
	counterVals []int64,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
	opts Options,
) *CounterElem {
	elemData := testCounterElemData
	elemData.AggTypes = aggTypes
	elemData.Pipeline = pipeline
	e := MustNewCounterElem(elemData, NewElemOptions(opts))
	for i, aligned := range alignedstartAtNanos {
		counter := &lockedCounterAggregation{
			aggregation: newCounterAggregation(raggregation.NewCounter(e.aggOpts)),
		}
		counter.dirty = true
		counter.aggregation.Update(time.Unix(0, aligned), counterVals[i], nil)
		startAligned := xtime.UnixNano(aligned)
		e.values[startAligned] = timedCounter{
			startAt:   startAligned,
			lockedAgg: counter,
		}
		e.dirty = append(e.dirty, startAligned)
	}
	e.minStartTime = xtime.UnixNano(alignedstartAtNanos[0])
	e.maxStartTime = xtime.UnixNano(alignedstartAtNanos[len(alignedstartAtNanos)-1])
	return e
}

func testTimerElem(
	alignedstartAtNanos []int64,
	timerBatches [][]float64,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
	opts Options,
) *TimerElem {
	elemData := testTimerElemData
	elemData.Pipeline = pipeline
	elemData.AggTypes = aggTypes
	e := MustNewTimerElem(elemData, NewElemOptions(opts))
	for i, aligned := range alignedstartAtNanos {
		newTimer := raggregation.NewTimer(opts.AggregationTypesOptions().Quantiles(), opts.StreamOptions(), e.aggOpts)
		timer := &lockedTimerAggregation{
			aggregation: newTimerAggregation(newTimer),
		}
		timer.dirty = true
		timer.aggregation.AddBatch(time.Now(), timerBatches[i], nil)
		startAligned := xtime.UnixNano(aligned)
		e.values[startAligned] = timedTimer{
			startAt:   startAligned,
			lockedAgg: timer,
		}
		e.dirty = append(e.dirty, startAligned)
	}
	e.minStartTime = xtime.UnixNano(alignedstartAtNanos[0])
	e.maxStartTime = xtime.UnixNano(alignedstartAtNanos[len(alignedstartAtNanos)-1])
	return e
}

var testGaugeData = ElemData{
	ID:                testGaugeID,
	StoragePolicy:     testStoragePolicy,
	Pipeline:          testPipeline,
	NumForwardedTimes: testNumForwardedTimes,
}

func testGaugeElem(
	alignedstartAtNanos []int64,
	gaugeVals []float64,
	aggTypes maggregation.Types,
	pipeline applied.Pipeline,
	opts Options,
) *GaugeElem {
	data := testGaugeData
	data.Pipeline = pipeline
	data.AggTypes = aggTypes
	return testGaugeElemWithData(&testing.T{}, alignedstartAtNanos, gaugeVals, data, opts, false)
}

func testGaugeElemWithData(
	t *testing.T,
	alignedstartAtNanos []int64,
	gaugeVals []float64,
	data ElemData,
	opts Options,
	resendEnabled bool,
) *GaugeElem {
	e := MustNewGaugeElem(data, NewElemOptions(opts))
	require.NoError(t, e.ResetSetData(data))
	for i, aligned := range alignedstartAtNanos {
		gauge := &lockedGaugeAggregation{
			aggregation:   newGaugeAggregation(raggregation.NewGauge(e.aggOpts)),
			sourcesSeen:   make(map[uint32]*bitset.BitSet),
			resendEnabled: resendEnabled,
		}
		gauge.dirty = true
		// offset the timestamp by 1 so the gauge value can be updated using the aligned timestamp later.
		gauge.aggregation.Update(time.Unix(0, aligned-1), gaugeVals[i], nil)
		startAligned := xtime.UnixNano(aligned)
		e.values[startAligned] = timedGauge{
			startAt:   startAligned,
			lockedAgg: gauge,
		}
		e.dirty = append(e.dirty, startAligned)
	}
	e.minStartTime = xtime.UnixNano(alignedstartAtNanos[0])
	e.maxStartTime = xtime.UnixNano(alignedstartAtNanos[len(alignedstartAtNanos)-1])
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
	return expectedLocalMetricsForGaugeWithVal(timeNanos, testGauge.GaugeVal, sp, aggTypes)
}

func expectedLocalMetricsForGaugeWithVal(
	timeNanos int64,
	val float64,
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
				value:     val,
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
			value:     val,
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
		require.Equal(t, len(expected[i].expiredTimes), len(actual[i].expiredTimes))
		for j := 0; j < len(expected[i].expiredTimes); j++ {
			require.Equal(t, expected[i].expiredTimes[j], actual[i].expiredTimes[j])
		}
	}
}
