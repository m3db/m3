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

package protobuf

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"

	"github.com/stretchr/testify/require"
)

var (
	testCounterBeforeResetProto = metricpb.Counter{
		Id:    []byte("testCounter"),
		Value: 1234,
	}
	testCounterAfterResetProto = metricpb.Counter{
		Id:    []byte{},
		Value: 0,
	}
	testBatchTimerBeforeResetProto = metricpb.BatchTimer{
		Id:     []byte("testBatchTimer"),
		Values: []float64{13.45, 98.23},
	}
	testBatchTimerAfterResetProto = metricpb.BatchTimer{
		Id:     []byte{},
		Values: []float64{},
	}
	testGaugeBeforeResetProto = metricpb.Gauge{
		Id:    []byte("testGauge"),
		Value: 3.48,
	}
	testGaugeAfterResetProto = metricpb.Gauge{
		Id:    []byte{},
		Value: 0.0,
	}
	testTimedMetricBeforeResetProto = metricpb.TimedMetric{
		Type:      metricpb.MetricType_COUNTER,
		Id:        []byte("testTimedMetric"),
		TimeNanos: 1234,
		Value:     1.23,
	}
	testForwardedMetricBeforeResetProto = metricpb.ForwardedMetric{
		Type:      metricpb.MetricType_COUNTER,
		Id:        []byte("testForwardedMetric"),
		TimeNanos: 1234,
		Values:    []float64{1.23, -4.56},
	}
	testForwardedMetricAfterResetProto = metricpb.ForwardedMetric{
		Type:      metricpb.MetricType_UNKNOWN,
		Id:        []byte{},
		TimeNanos: 0,
		Values:    []float64{},
	}
	testMetadatasBeforeResetProto = metricpb.StagedMetadatas{
		Metadatas: []metricpb.StagedMetadata{
			{
				CutoverNanos: 1234,
			},
			{
				CutoverNanos: 5678,
			},
		},
	}
	testMetadatasAfterResetProto = metricpb.StagedMetadatas{
		Metadatas: []metricpb.StagedMetadata{},
	}
	testForwardMetadataBeforeResetProto = metricpb.ForwardMetadata{
		Pipeline: pipelinepb.AppliedPipeline{
			Ops: []pipelinepb.AppliedPipelineOp{
				{
					Type: pipelinepb.AppliedPipelineOp_ROLLUP,
					Rollup: &pipelinepb.AppliedRollupOp{
						Id: []byte("foo"),
						AggregationId: aggregationpb.AggregationID{
							Id: 12,
						},
					},
				},
			},
		},
		SourceId:          342,
		NumForwardedTimes: 23,
	}
	testForwardMetadataAfterResetProto = metricpb.ForwardMetadata{
		Pipeline: pipelinepb.AppliedPipeline{
			Ops: []pipelinepb.AppliedPipelineOp{},
		},
		SourceId:          0,
		NumForwardedTimes: 0,
	}
)

func TestResetMetricWithMetadatasProtoNilProto(t *testing.T) {
	require.NotPanics(t, func() { resetMetricWithMetadatasProto(nil) })
}

func TestResetAggregatedMetricProto(t *testing.T) {
	input := &metricpb.AggregatedMetric{
		Metric: metricpb.TimedMetricWithStoragePolicy{
			TimedMetric: testTimedMetricBeforeResetProto,
			StoragePolicy: policypb.StoragePolicy{
				Resolution: &policypb.Resolution{
					WindowSize: 10 * time.Second.Nanoseconds(),
					Precision:  time.Second.Nanoseconds(),
				},
				Retention: &policypb.Retention{
					Period: (6 * time.Hour).Nanoseconds(),
				},
			},
		},
		EncodeNanos: 1234,
	}
	resetAggregatedMetricProto(input)
	require.Equal(t, metricpb.AggregatedMetric{
		Metric: metricpb.TimedMetricWithStoragePolicy{
			TimedMetric:   metricpb.TimedMetric{Id: []byte{}},
			StoragePolicy: policypb.StoragePolicy{},
		},
		EncodeNanos: 0,
	}, *input)
	require.True(t, cap(input.Metric.TimedMetric.Id) > 0)
}

func TestResetMetricWithMetadatasProtoOnlyCounter(t *testing.T) {
	input := &metricpb.MetricWithMetadatas{
		Type: metricpb.MetricWithMetadatas_COUNTER_WITH_METADATAS,
		CounterWithMetadatas: &metricpb.CounterWithMetadatas{
			Counter:   testCounterBeforeResetProto,
			Metadatas: testMetadatasBeforeResetProto,
		},
	}
	expected := &metricpb.MetricWithMetadatas{
		Type: metricpb.MetricWithMetadatas_UNKNOWN,
		CounterWithMetadatas: &metricpb.CounterWithMetadatas{
			Counter:   testCounterAfterResetProto,
			Metadatas: testMetadatasAfterResetProto,
		},
	}
	resetMetricWithMetadatasProto(input)
	require.Equal(t, expected, input)
	require.True(t, cap(input.CounterWithMetadatas.Counter.Id) > 0)
	require.True(t, cap(input.CounterWithMetadatas.Metadatas.Metadatas) > 0)
}

func TestResetMetricWithMetadatasProtoOnlyBatchTimer(t *testing.T) {
	input := &metricpb.MetricWithMetadatas{
		Type: metricpb.MetricWithMetadatas_BATCH_TIMER_WITH_METADATAS,
		BatchTimerWithMetadatas: &metricpb.BatchTimerWithMetadatas{
			BatchTimer: testBatchTimerBeforeResetProto,
			Metadatas:  testMetadatasBeforeResetProto,
		},
	}
	expected := &metricpb.MetricWithMetadatas{
		Type: metricpb.MetricWithMetadatas_UNKNOWN,
		BatchTimerWithMetadatas: &metricpb.BatchTimerWithMetadatas{
			BatchTimer: testBatchTimerAfterResetProto,
			Metadatas:  testMetadatasAfterResetProto,
		},
	}
	resetMetricWithMetadatasProto(input)
	require.Equal(t, expected, input)
	require.True(t, cap(input.BatchTimerWithMetadatas.BatchTimer.Id) > 0)
	require.True(t, cap(input.BatchTimerWithMetadatas.Metadatas.Metadatas) > 0)
}

func TestResetMetricWithMetadatasProtoOnlyGauge(t *testing.T) {
	input := &metricpb.MetricWithMetadatas{
		Type: metricpb.MetricWithMetadatas_GAUGE_WITH_METADATAS,
		GaugeWithMetadatas: &metricpb.GaugeWithMetadatas{
			Gauge:     testGaugeBeforeResetProto,
			Metadatas: testMetadatasBeforeResetProto,
		},
	}
	expected := &metricpb.MetricWithMetadatas{
		Type: metricpb.MetricWithMetadatas_UNKNOWN,
		GaugeWithMetadatas: &metricpb.GaugeWithMetadatas{
			Gauge:     testGaugeAfterResetProto,
			Metadatas: testMetadatasAfterResetProto,
		},
	}
	resetMetricWithMetadatasProto(input)
	require.Equal(t, expected, input)
	require.True(t, cap(input.GaugeWithMetadatas.Gauge.Id) > 0)
	require.True(t, cap(input.GaugeWithMetadatas.Metadatas.Metadatas) > 0)
}

func TestResetMetricWithMetadatasProtoOnlyForwardedMetric(t *testing.T) {
	input := &metricpb.MetricWithMetadatas{
		Type: metricpb.MetricWithMetadatas_FORWARDED_METRIC_WITH_METADATA,
		ForwardedMetricWithMetadata: &metricpb.ForwardedMetricWithMetadata{
			Metric:   testForwardedMetricBeforeResetProto,
			Metadata: testForwardMetadataBeforeResetProto,
		},
	}
	expected := &metricpb.MetricWithMetadatas{
		Type: metricpb.MetricWithMetadatas_UNKNOWN,
		ForwardedMetricWithMetadata: &metricpb.ForwardedMetricWithMetadata{
			Metric:   testForwardedMetricAfterResetProto,
			Metadata: testForwardMetadataAfterResetProto,
		},
	}
	resetMetricWithMetadatasProto(input)
	require.Equal(t, expected, input)
	require.True(t, cap(input.ForwardedMetricWithMetadata.Metric.Id) > 0)
	require.True(t, cap(input.ForwardedMetricWithMetadata.Metric.Values) > 0)
	require.True(t, cap(input.ForwardedMetricWithMetadata.Metadata.Pipeline.Ops) > 0)
}

func TestResetMetricWithMetadatasProtoAll(t *testing.T) {
	input := &metricpb.MetricWithMetadatas{
		Type: metricpb.MetricWithMetadatas_GAUGE_WITH_METADATAS,
		CounterWithMetadatas: &metricpb.CounterWithMetadatas{
			Counter:   testCounterBeforeResetProto,
			Metadatas: testMetadatasBeforeResetProto,
		},
		BatchTimerWithMetadatas: &metricpb.BatchTimerWithMetadatas{
			BatchTimer: testBatchTimerBeforeResetProto,
			Metadatas:  testMetadatasBeforeResetProto,
		},
		GaugeWithMetadatas: &metricpb.GaugeWithMetadatas{
			Gauge:     testGaugeBeforeResetProto,
			Metadatas: testMetadatasBeforeResetProto,
		},
		ForwardedMetricWithMetadata: &metricpb.ForwardedMetricWithMetadata{
			Metric:   testForwardedMetricBeforeResetProto,
			Metadata: testForwardMetadataBeforeResetProto,
		},
	}
	expected := &metricpb.MetricWithMetadatas{
		Type: metricpb.MetricWithMetadatas_UNKNOWN,
		CounterWithMetadatas: &metricpb.CounterWithMetadatas{
			Counter:   testCounterAfterResetProto,
			Metadatas: testMetadatasAfterResetProto,
		},
		BatchTimerWithMetadatas: &metricpb.BatchTimerWithMetadatas{
			BatchTimer: testBatchTimerAfterResetProto,
			Metadatas:  testMetadatasAfterResetProto,
		},
		GaugeWithMetadatas: &metricpb.GaugeWithMetadatas{
			Gauge:     testGaugeAfterResetProto,
			Metadatas: testMetadatasAfterResetProto,
		},
		ForwardedMetricWithMetadata: &metricpb.ForwardedMetricWithMetadata{
			Metric:   testForwardedMetricAfterResetProto,
			Metadata: testForwardMetadataAfterResetProto,
		},
	}
	resetMetricWithMetadatasProto(input)
	require.Equal(t, expected, input)
	require.True(t, cap(input.CounterWithMetadatas.Counter.Id) > 0)
	require.True(t, cap(input.CounterWithMetadatas.Metadatas.Metadatas) > 0)
	require.True(t, cap(input.BatchTimerWithMetadatas.BatchTimer.Id) > 0)
	require.True(t, cap(input.BatchTimerWithMetadatas.Metadatas.Metadatas) > 0)
	require.True(t, cap(input.GaugeWithMetadatas.Gauge.Id) > 0)
	require.True(t, cap(input.GaugeWithMetadatas.Metadatas.Metadatas) > 0)
	require.True(t, cap(input.ForwardedMetricWithMetadata.Metric.Id) > 0)
	require.True(t, cap(input.ForwardedMetricWithMetadata.Metric.Values) > 0)
	require.True(t, cap(input.ForwardedMetricWithMetadata.Metadata.Pipeline.Ops) > 0)
}
