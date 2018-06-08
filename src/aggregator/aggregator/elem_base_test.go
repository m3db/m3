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

	raggregation "github.com/m3db/m3aggregator/aggregation"
	maggregation "github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/op"
	"github.com/m3db/m3metrics/op/applied"
	"github.com/m3db/m3metrics/transformation"

	"github.com/stretchr/testify/require"
)

func TestElemBaseID(t *testing.T) {
	e := &elemBase{}
	e.resetSetData(testCounterID, testStoragePolicy, maggregation.DefaultTypes, true, applied.DefaultPipeline, 0)
	require.Equal(t, testCounterID, e.ID())
}

func TestElemBaseResetSetData(t *testing.T) {
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
	e := &elemBase{}
	e.resetSetData(testCounterID, testStoragePolicy, testAggregationTypesExpensive, false, testPipeline, 3)
	require.Equal(t, testCounterID, e.id)
	require.Equal(t, testStoragePolicy, e.sp)
	require.Equal(t, testAggregationTypesExpensive, e.aggTypes)
	require.False(t, e.useDefaultAggregation)
	require.True(t, e.aggOpts.HasExpensiveAggregations)
	require.Equal(t, expectedParsedPipeline, e.parsedPipeline)
	require.Equal(t, 3, e.numForwardedTimes)
	require.False(t, e.tombstoned)
	require.False(t, e.closed)
}

func TestElemBaseResetSetDataInvalidPipeline(t *testing.T) {
	invalidPipeline := applied.NewPipeline([]applied.Union{
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.Absolute},
		},
	})
	e := &elemBase{}
	err := e.resetSetData(testCounterID, testStoragePolicy, testAggregationTypes, false, invalidPipeline, 0)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "has no rollup operations"))
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
	require.Equal(t, []byte("lower"), e.TypeStringFor(aggTypesOpts, maggregation.Min))
	require.True(t, opts.CounterElemPool() == e.ElemPool(opts))
}

func TestCounterElemBaseNewAggregation(t *testing.T) {
	e := counterElemBase{}
	la := e.NewAggregation(nil, raggregation.Options{})
	la.AddUnion(unaggregated.MetricUnion{
		Type:       metric.CounterType,
		CounterVal: 100,
	})
	la.AddUnion(unaggregated.MetricUnion{
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
	err := e.ResetSetData(nil, maggregation.Types{maggregation.Last}, false)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "invalid aggregation types Last for counter"))
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
	require.Equal(t, []byte("lower"), e.TypeStringFor(aggTypesOpts, maggregation.Min))
	require.True(t, opts.TimerElemPool() == e.ElemPool(opts))
}

func TestTimerElemBaseNewAggregation(t *testing.T) {
	e := timerElemBase{}
	la := e.NewAggregation(NewOptions(), raggregation.Options{})
	la.AddUnion(unaggregated.MetricUnion{
		Type:          metric.TimerType,
		BatchTimerVal: []float64{100.0, 200.0},
	})
	la.AddUnion(unaggregated.MetricUnion{
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
	require.Equal(t, typesOpts.TimerQuantiles(), e.quantiles)
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
	require.Equal(t, []byte("lower"), e.TypeStringFor(aggTypesOpts, maggregation.Min))
	require.True(t, opts.GaugeElemPool() == e.ElemPool(opts))
}

func TestGaugeElemBaseNewLockedAggregation(t *testing.T) {
	e := gaugeElemBase{}
	la := e.NewAggregation(nil, raggregation.Options{})
	la.AddUnion(unaggregated.MetricUnion{
		Type:     metric.GaugeType,
		GaugeVal: 100.0,
	})
	la.AddUnion(unaggregated.MetricUnion{
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
	p := applied.NewPipeline([]applied.Union{
		{
			Type: op.RollupType,
			Rollup: applied.Rollup{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
			},
		},
		{
			Type: op.RollupType,
			Rollup: applied.Rollup{
				ID:            []byte("bar"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Max),
			},
		},
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.Absolute},
		},
		{
			Type: op.RollupType,
			Rollup: applied.Rollup{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
			},
		},
	})
	expected := parsedPipeline{
		HasDerivativeTransform: false,
		Transformations:        applied.NewPipeline([]applied.Union{}),
		HasRollup:              true,
		Rollup: applied.Rollup{
			ID:            []byte("foo"),
			AggregationID: maggregation.MustCompressTypes(maggregation.Count),
		},
		Remainder: applied.NewPipeline([]applied.Union{
			{
				Type: op.RollupType,
				Rollup: applied.Rollup{
					ID:            []byte("bar"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
			{
				Type:           op.TransformationType,
				Transformation: op.Transformation{Type: transformation.Absolute},
			},
			{
				Type: op.RollupType,
				Rollup: applied.Rollup{
					ID:            []byte("foo"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
				},
			},
		}),
	}
	parsed, err := newParsedPipeline(p)
	require.NoError(t, err)
	require.Equal(t, expected, parsed)
}

func TestParsePipelineWithNonDerivativeTransformation(t *testing.T) {
	p := applied.NewPipeline([]applied.Union{
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.Absolute},
		},
		{
			Type: op.RollupType,
			Rollup: applied.Rollup{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
			},
		},
		{
			Type: op.RollupType,
			Rollup: applied.Rollup{
				ID:            []byte("bar"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Max),
			},
		},
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.Absolute},
		},
		{
			Type: op.RollupType,
			Rollup: applied.Rollup{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
			},
		},
	})
	expected := parsedPipeline{
		HasDerivativeTransform: false,
		Transformations: applied.NewPipeline([]applied.Union{
			{
				Type:           op.TransformationType,
				Transformation: op.Transformation{Type: transformation.Absolute},
			},
		}),
		HasRollup: true,
		Rollup: applied.Rollup{
			ID:            []byte("foo"),
			AggregationID: maggregation.MustCompressTypes(maggregation.Count),
		},
		Remainder: applied.NewPipeline([]applied.Union{
			{
				Type: op.RollupType,
				Rollup: applied.Rollup{
					ID:            []byte("bar"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
			{
				Type:           op.TransformationType,
				Transformation: op.Transformation{Type: transformation.Absolute},
			},
			{
				Type: op.RollupType,
				Rollup: applied.Rollup{
					ID:            []byte("foo"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
				},
			},
		}),
	}
	parsed, err := newParsedPipeline(p)
	require.NoError(t, err)
	require.Equal(t, expected, parsed)
}

func TestParsePipelineWithDerivativeTransformation(t *testing.T) {
	p := applied.NewPipeline([]applied.Union{
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.PerSecond},
		},
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.Absolute},
		},
		{
			Type: op.RollupType,
			Rollup: applied.Rollup{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
			},
		},
		{
			Type: op.RollupType,
			Rollup: applied.Rollup{
				ID:            []byte("bar"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Max),
			},
		},
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.Absolute},
		},
		{
			Type: op.RollupType,
			Rollup: applied.Rollup{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
			},
		},
	})
	expected := parsedPipeline{
		HasDerivativeTransform: true,
		Transformations: applied.NewPipeline([]applied.Union{
			{
				Type:           op.TransformationType,
				Transformation: op.Transformation{Type: transformation.PerSecond},
			},
			{
				Type:           op.TransformationType,
				Transformation: op.Transformation{Type: transformation.Absolute},
			},
		}),
		HasRollup: true,
		Rollup: applied.Rollup{
			ID:            []byte("foo"),
			AggregationID: maggregation.MustCompressTypes(maggregation.Count),
		},
		Remainder: applied.NewPipeline([]applied.Union{
			{
				Type: op.RollupType,
				Rollup: applied.Rollup{
					ID:            []byte("bar"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Max),
				},
			},
			{
				Type:           op.TransformationType,
				Transformation: op.Transformation{Type: transformation.Absolute},
			},
			{
				Type: op.RollupType,
				Rollup: applied.Rollup{
					ID:            []byte("foo"),
					AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
				},
			},
		}),
	}
	parsed, err := newParsedPipeline(p)
	require.NoError(t, err)
	require.Equal(t, expected, parsed)
}

func TestParsePipelineInvalidOperationType(t *testing.T) {
	p := applied.NewPipeline([]applied.Union{
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.Absolute},
		},
		{
			Type: op.UnknownType,
		},
	})
	_, err := newParsedPipeline(p)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "step 1 has invalid operation type UnknownType"))
}

func TestParsePipelineNoRollupOperation(t *testing.T) {
	p := applied.NewPipeline([]applied.Union{
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.Absolute},
		},
	})
	_, err := newParsedPipeline(p)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "has no rollup operations"))
}

func TestParsePipelineTransformationDerivativeOrderTooHigh(t *testing.T) {
	p := applied.NewPipeline([]applied.Union{
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.PerSecond},
		},
		{
			Type:           op.TransformationType,
			Transformation: op.Transformation{Type: transformation.PerSecond},
		},
		{
			Type: op.RollupType,
			Rollup: applied.Rollup{
				ID:            []byte("foo"),
				AggregationID: maggregation.MustCompressTypes(maggregation.Count),
			},
		},
	})
	_, err := newParsedPipeline(p)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "transformation derivative order is 2 higher than supported 1"))
}
