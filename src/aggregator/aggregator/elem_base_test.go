// Copyright (c) 2018 Uber Technologies, Inc.
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
	"strings"
	"testing"
	"time"

	raggregation "github.com/m3db/m3/src/aggregator/aggregation"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/transformation"

	"github.com/stretchr/testify/require"
)

func mustNewOp(t require.TestingT, ttype transformation.Type) transformation.Op {
	op, err := ttype.NewOp()
	require.NoError(t, err)
	return op
}

func TestElemBaseID(t *testing.T) {
	e := &elemBase{}
	e.resetSetData(testCounterID, testStoragePolicy, maggregation.DefaultTypes, true, applied.DefaultPipeline, 0, NoPrefixNoSuffix)
	require.Equal(t, testCounterID, e.ID())
}

func requirePipelinesMatch(t require.TestingT, expected, actual parsedPipeline) {
	// Compare transform types
	require.Equal(t, len(expected.Transformations), len(actual.Transformations))
	for i := range expected.Transformations {
		require.Equal(t, expected.Transformations[i].Type(), actual.Transformations[i].Type())
	}

	// Note: transformations are now constructed each time, so not equal, nil out before comparison
	expectedWithoutTransforms := expected
	expectedWithoutTransforms.Transformations = nil
	actualWithoutTransforms := actual
	actualWithoutTransforms.Transformations = nil
	require.Equal(t, expectedWithoutTransforms, actualWithoutTransforms)
}

func TestElemBaseResetSetData(t *testing.T) {
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
	e := &elemBase{}
	e.resetSetData(testCounterID, testStoragePolicy, testAggregationTypesExpensive, false, testPipeline, 3, WithPrefixWithSuffix)
	require.Equal(t, testCounterID, e.id)
	require.Equal(t, testStoragePolicy, e.sp)
	require.Equal(t, testAggregationTypesExpensive, e.aggTypes)
	require.False(t, e.useDefaultAggregation)
	require.True(t, e.aggOpts.HasExpensiveAggregations)

	requirePipelinesMatch(t, expectedParsedPipeline, e.parsedPipeline)

	require.Equal(t, 3, e.numForwardedTimes)
	require.False(t, e.tombstoned)
	require.False(t, e.closed)
	require.Equal(t, WithPrefixWithSuffix, e.idPrefixSuffixType)
}

func TestElemBaseResetSetDataNoRollup(t *testing.T) {
	pipelineNoRollup := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
		},
	})
	e := &elemBase{}
	err := e.resetSetData(testCounterID, testStoragePolicy, testAggregationTypes, false, pipelineNoRollup, 0, WithPrefixWithSuffix)
	require.NoError(t, err)
}

func TestElemBaseForwardedIDWithDefaultPipeline(t *testing.T) {
	e := newElemBase(NewOptions())
	_, ok := e.ForwardedID()
	require.False(t, ok)
}

func TestElemBaseForwardedIDWithCustomPipeline(t *testing.T) {
	e := &elemBase{}
	e.resetSetData(testCounterID, testStoragePolicy, testAggregationTypesExpensive, false, testPipeline, 3, WithPrefixWithSuffix)
	fid, ok := e.ForwardedID()
	require.True(t, ok)
	require.Equal(t, id.RawID("foo.bar"), fid)
}

func TestElemBaseForwardedAggregationKeyWithDefaultPipeline(t *testing.T) {
	e := newElemBase(NewOptions())
	_, ok := e.ForwardedAggregationKey()
	require.False(t, ok)
}

func TestElemBaseForwardedAggregationKeyWithCustomPipeline(t *testing.T) {
	e := &elemBase{}
	e.resetSetData(testCounterID, testStoragePolicy, testAggregationTypesExpensive, false, testPipeline, 3, WithPrefixWithSuffix)
	aggKey, ok := e.ForwardedAggregationKey()
	require.True(t, ok)
	expected := aggregationKey{
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
		numForwardedTimes: 4,
	}
	require.Equal(t, expected, aggKey)
}

func TestElemBaseMarkAsTombStoned(t *testing.T) {
	e := &elemBase{}
	require.False(t, e.tombstoned)

	// Marking a closed element tombstoned has no impact.
	e.closed = true
	e.MarkAsTombstoned()
	require.False(t, e.tombstoned)

	e.closed = false
	e.MarkAsTombstoned()
	require.True(t, e.tombstoned)
}

func TestCounterElemBase(t *testing.T) {
	opts := NewOptions()
	aggTypesOpts := opts.AggregationTypesOptions()
	e := counterElemBase{}
	require.Equal(t, []byte("stats.counts."), e.FullPrefix(opts))
	require.Equal(t, maggregation.Types{maggregation.Sum}, e.DefaultAggregationTypes(aggTypesOpts))
	require.Equal(t, []byte(nil), e.TypeStringFor(aggTypesOpts, maggregation.Sum))
	require.Equal(t, []byte(nil), e.TypeStringFor(aggTypesOpts, maggregation.Min))
	require.True(t, opts.CounterElemPool() == e.ElemPool(opts))
}

func TestCounterElemBaseNewAggregation(t *testing.T) {
	e := counterElemBase{}
	la := e.NewAggregation(nil, raggregation.Options{})
	la.AddUnion(time.Now(), unaggregated.MetricUnion{
		Type:       metric.CounterType,
		CounterVal: 100,
	})
	la.AddUnion(time.Now(), unaggregated.MetricUnion{
		Type:       metric.CounterType,
		CounterVal: 200,
	})
	res := la.ValueOf(maggregation.Sum)
	require.Equal(t, float64(300), res)
}

func TestCounterElemBaseResetSetData(t *testing.T) {
	e := counterElemBase{}
	require.NoError(t, e.ResetSetData(nil, maggregation.Types{maggregation.Sum}, false))
}

func TestCounterElemBaseResetSetDataInvalidTypes(t *testing.T) {
	e := counterElemBase{}
	err := e.ResetSetData(nil, maggregation.Types{maggregation.P10}, false)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "invalid aggregation types P10 for counter"))
}

func TestTimerElemBase(t *testing.T) {
	defaultTimerAggregationTypes := maggregation.Types{
		maggregation.Sum,
		maggregation.SumSq,
		maggregation.Mean,
		maggregation.Min,
		maggregation.Max,
		maggregation.Count,
		maggregation.Stdev,
		maggregation.Median,
		maggregation.P50,
		maggregation.P95,
		maggregation.P99,
	}
	opts := NewOptions()
	aggTypesOpts := opts.AggregationTypesOptions()
	e := timerElemBase{}
	require.Equal(t, []byte("stats.timers."), e.FullPrefix(opts))
	require.Equal(t, defaultTimerAggregationTypes, e.DefaultAggregationTypes(aggTypesOpts))
	require.Equal(t, []byte(".lower"), e.TypeStringFor(aggTypesOpts, maggregation.Min))
	require.True(t, opts.TimerElemPool() == e.ElemPool(opts))
}

func TestTimerElemBaseNewAggregation(t *testing.T) {
	e := timerElemBase{}
	la := e.NewAggregation(NewOptions(), raggregation.Options{})
	la.AddUnion(time.Now(), unaggregated.MetricUnion{
		Type:          metric.TimerType,
		BatchTimerVal: []float64{100.0, 200.0},
	})
	la.AddUnion(time.Now(), unaggregated.MetricUnion{
		Type:          metric.TimerType,
		BatchTimerVal: []float64{300.0, 400.0, 500.0},
	})
	res := la.ValueOf(maggregation.Mean)
	require.Equal(t, float64(300.0), res)
}

func TestTimerElemBaseResetSetDataWithDefaultAggregation(t *testing.T) {
	e := timerElemBase{}
	typesOpts := maggregation.NewTypesOptions()
	aggTypes := typesOpts.DefaultTimerAggregationTypes()
	require.NoError(t, e.ResetSetData(typesOpts, aggTypes, true))
	require.Equal(t, typesOpts.Quantiles(), e.quantiles)
	require.Nil(t, e.quantilesPool)

	e.Close()
	require.Nil(t, e.quantiles)
	require.Nil(t, e.quantilesPool)
}

func TestTimerElemBaseResetSetDataWithCustomAggregation(t *testing.T) {
	e := timerElemBase{}
	typesOpts := maggregation.NewTypesOptions()
	aggTypes := maggregation.Types{maggregation.P99, maggregation.P9999}
	require.NoError(t, e.ResetSetData(typesOpts, aggTypes, false))
	require.Equal(t, []float64{0.99, 0.9999}, e.quantiles)
	require.NotNil(t, e.quantilesPool)

	e.Close()
	require.Nil(t, e.quantiles)
	require.Nil(t, e.quantilesPool)
}

func TestTimerElemBaseResetSetDataInvalidTypes(t *testing.T) {
	e := timerElemBase{}
	err := e.ResetSetData(nil, maggregation.Types{maggregation.Last}, false)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "invalid aggregation types Last for timer"))
}

func TestGaugeElemBase(t *testing.T) {
	opts := NewOptions()
	aggTypesOpts := opts.AggregationTypesOptions()
	e := gaugeElemBase{}
	require.Equal(t, []byte("stats.gauges."), e.FullPrefix(opts))
	require.Equal(t, maggregation.Types{maggregation.Last}, e.DefaultAggregationTypes(aggTypesOpts))
	require.Equal(t, []byte(nil), e.TypeStringFor(aggTypesOpts, maggregation.Last))
	require.Equal(t, []byte(nil), e.TypeStringFor(aggTypesOpts, maggregation.Min))
	require.True(t, opts.GaugeElemPool() == e.ElemPool(opts))
}

func TestGaugeElemBaseNewLockedAggregation(t *testing.T) {
	e := gaugeElemBase{}
	la := e.NewAggregation(nil, raggregation.Options{})
	la.AddUnion(time.Now(), unaggregated.MetricUnion{
		Type:     metric.GaugeType,
		GaugeVal: 100.0,
	})
	la.AddUnion(time.Now(), unaggregated.MetricUnion{
		Type:     metric.GaugeType,
		GaugeVal: 200.0,
	})
	res := la.ValueOf(maggregation.Last)
	require.Equal(t, float64(200.0), res)
}

func TestGaugeElemBaseResetSetData(t *testing.T) {
	e := gaugeElemBase{}
	require.NoError(t, e.ResetSetData(nil, maggregation.Types{maggregation.Sum}, false))
}

func TestGaugeElemBaseResetSetDataInvalidTypes(t *testing.T) {
	e := gaugeElemBase{}
	err := e.ResetSetData(nil, maggregation.Types{maggregation.P99}, false)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "invalid aggregation types P99 for gauge"))
}

func TestParsedPipelineEmptyPipeline(t *testing.T) {
	p := applied.Pipeline{}
	pp, err := newParsedPipeline(p)
	require.NoError(t, err)
	require.Equal(t, parsedPipeline{}, pp)
}

func TestParsePipelineNoTransformation(t *testing.T) {
	p := applied.NewPipeline([]applied.OpUnion{
		{
			Type: pipeline.RollupOpType,
			Rollup: applied.RollupOp{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
			},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: applied.RollupOp{
				ID:            []byte("bar"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Max),
			},
		},
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: applied.RollupOp{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
			},
		},
	})
	expected := parsedPipeline{
		HasDerivativeTransform: false,
		HasRollup:              true,
		Rollup: applied.RollupOp{
			ID:            []byte("foo"),
			AggregationID: maggregation.MustCompressTypes(maggregation.Count),
		},
		Remainder: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("bar"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
			{
				Type:           pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
			},
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("foo"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
				},
			},
		}),
	}
	parsed, err := newParsedPipeline(p)
	require.NoError(t, err)
	requirePipelinesMatch(t, expected, parsed)
}

func TestParsePipelineWithNonDerivativeTransformation(t *testing.T) {
	p := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: applied.RollupOp{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
			},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: applied.RollupOp{
				ID:            []byte("bar"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Max),
			},
		},
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: applied.RollupOp{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
			},
		},
	})
	expected := parsedPipeline{
		HasDerivativeTransform: false,
		Transformations:        []transformation.Op{mustNewOp(t, transformation.Absolute)},
		HasRollup:              true,
		Rollup: applied.RollupOp{
			ID:            []byte("foo"),
			AggregationID: maggregation.MustCompressTypes(maggregation.Count),
		},
		Remainder: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("bar"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
			{
				Type:           pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
			},
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("foo"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
				},
			},
		}),
	}
	parsed, err := newParsedPipeline(p)
	require.NoError(t, err)
	requirePipelinesMatch(t, expected, parsed)
}

func TestParsePipelineWithDerivativeTransformation(t *testing.T) {
	p := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
		},
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: applied.RollupOp{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
			},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: applied.RollupOp{
				ID:            []byte("bar"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Max),
			},
		},
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: applied.RollupOp{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
			},
		},
	})
	expected := parsedPipeline{
		HasDerivativeTransform: true,
		Transformations: []transformation.Op{
			mustNewOp(t, transformation.PerSecond),
			mustNewOp(t, transformation.Absolute),
		},
		HasRollup: true,
		Rollup: applied.RollupOp{
			ID:            []byte("foo"),
			AggregationID: maggregation.MustCompressTypes(maggregation.Count),
		},
		Remainder: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("bar"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
			{
				Type:           pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
			},
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("foo"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
				},
			},
		}),
	}
	parsed, err := newParsedPipeline(p)
	require.NoError(t, err)
	requirePipelinesMatch(t, expected, parsed)
}

func TestParsePipelineInvalidOperationType(t *testing.T) {
	p := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
		},
		{
			Type: pipeline.UnknownOpType,
		},
	})
	_, err := newParsedPipeline(p)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "step 1 has invalid operation type UnknownOpType"))
}

func TestParsePipelineNoRollupOperation(t *testing.T) {
	p := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
		},
	})
	_, err := newParsedPipeline(p)
	require.NoError(t, err)
}

func TestParsePipelineTransformationDerivativeOrderTooHigh(t *testing.T) {
	p := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
		},
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
		},
		{
			Type: pipeline.RollupOpType,
			Rollup: applied.RollupOp{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
			},
		},
	})
	_, err := newParsedPipeline(p)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "transformation derivative order is 2 higher than supported 1"))
}

func TestAddToRestOption(t *testing.T) {
	p := applied.NewPipeline([]applied.OpUnion{
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Add},
		},
		{
			Type:           pipeline.TransformationOpType,
			Transformation: pipeline.TransformationOp{Type: transformation.Increase},
		},
	})
	e := newElemBase(NewOptions().SetAddToReset(false))
	e.resetSetData(testCounterID, testStoragePolicy, testAggregationTypesExpensive, false, p, 3, WithPrefixWithSuffix)
	require.Equal(t, transformation.Add, e.parsedPipeline.Transformations[0].Type())
	require.Equal(t, transformation.Increase, e.parsedPipeline.Transformations[1].Type())

	e = newElemBase(NewOptions().SetAddToReset(true))
	e.resetSetData(testCounterID, testStoragePolicy, testAggregationTypesExpensive, false, p, 3, WithPrefixWithSuffix)
	require.Equal(t, transformation.Reset, e.parsedPipeline.Transformations[0].Type())
	require.Equal(t, transformation.Increase, e.parsedPipeline.Transformations[1].Type())
}
