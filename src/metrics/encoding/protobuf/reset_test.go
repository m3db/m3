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

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
)

var (
	testCounterBeforeResetProto = metricpb.Counter{
		Id:              []byte("testCounter"),
		Value:           1234,
		ClientTimeNanos: 10,
	}
	testCounterAfterResetProto = metricpb.Counter{
		Id:    []byte{},
		Value: 0,
	}
	testBatchTimerBeforeResetProto = metricpb.BatchTimer{
		Id:              []byte("testBatchTimer"),
		Values:          []float64{13.45, 98.23},
		ClientTimeNanos: 10,
	}
	testBatchTimerAfterResetProto = metricpb.BatchTimer{
		Id:     []byte{},
		Values: []float64{},
	}
	testGaugeBeforeResetProto = metricpb.Gauge{
		Id:              []byte("testGauge"),
		Value:           3.48,
		ClientTimeNanos: 10,
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
		Version:   1,
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
				Metadata: metricpb.Metadata{
					Pipelines: []metricpb.PipelineMetadata{
						{
							ResendEnabled: true,
						},
					},
				},
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
		ResendEnabled: true,
		Pipeline: pipelinepb.AppliedPipeline{
			Ops: []pipelinepb.AppliedPipelineOp{
				{
					Type: pipelinepb.AppliedPipelineOp_ROLLUP,
					Rollup: pipelinepb.AppliedRollupOp{
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

func TestReuseMetricWithMetadatasProtoNilProto(t *testing.T) {
	require.NotPanics(t, func() { ReuseMetricWithMetadatasProto(nil) })
}

func TestReuseAggregatedMetricProto(t *testing.T) {
	input := &metricpb.AggregatedMetric{
		Metric: metricpb.TimedMetricWithStoragePolicy{
			TimedMetric: testTimedMetricBeforeResetProto,
			StoragePolicy: policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: 10 * time.Second.Nanoseconds(),
					Precision:  time.Second.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: (6 * time.Hour).Nanoseconds(),
				},
			},
		},
		EncodeNanos: 1234,
	}
	ReuseAggregatedMetricProto(input)
	require.Equal(t, metricpb.AggregatedMetric{
		Metric: metricpb.TimedMetricWithStoragePolicy{
			TimedMetric:   metricpb.TimedMetric{Id: []byte{}},
			StoragePolicy: policypb.StoragePolicy{},
		},
		EncodeNanos: 0,
	}, *input)
	require.True(t, cap(input.Metric.TimedMetric.Id) > 0)
}

func TestReuseMetricWithMetadatasProtoOnlyCounter(t *testing.T) {
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
	ReuseMetricWithMetadatasProto(input)
	require.Equal(t, expected, input)
	require.True(t, cap(input.CounterWithMetadatas.Counter.Id) > 0)
	require.True(t, cap(input.CounterWithMetadatas.Metadatas.Metadatas) > 0)
}

func TestReuseMetricForEncoding(t *testing.T) {
	input := metricpb.MetricWithMetadatas{
		Type: metricpb.MetricWithMetadatas_COUNTER_WITH_METADATAS,
		CounterWithMetadatas: &metricpb.CounterWithMetadatas{
			Counter: metricpb.Counter{
				Id: []byte("testCounter"),
			},
			Metadatas: metricpb.StagedMetadatas{
				Metadatas: []metricpb.StagedMetadata{
					{
						Metadata: metricpb.Metadata{
							Pipelines: []metricpb.PipelineMetadata{
								{
									ResendEnabled: true,
									StoragePolicies: []policypb.StoragePolicy{
										{
											Resolution: policypb.Resolution{
												WindowSize: 1000,
											},
											Retention: policypb.Retention{
												Period: 1000,
											},
										},
									},
									Pipeline: pipelinepb.AppliedPipeline{
										Ops: []pipelinepb.AppliedPipelineOp{
											{
												Rollup: pipelinepb.AppliedRollupOp{
													Id: []byte("foobar"),
												},
											},
										},
									},
								},
							},
						},
					},
					{
						CutoverNanos: 5678,
					},
				},
			},
		},
	}
	b, err := input.Marshal()
	require.NoError(t, err)

	var pb metricpb.MetricWithMetadatas
	require.NoError(t, pb.Unmarshal(b))
	require.Equal(t, input, pb)

	// verify all slices are empty with a non-zero capacity.
	ReuseMetricWithMetadatasProto(&pb)
	metas := pb.CounterWithMetadatas.Metadatas.Metadatas
	require.True(t, cap(metas) > 0)
	require.Empty(t, metas)
	metas = metas[0:1]
	pipes := metas[0].Metadata.Pipelines
	require.True(t, cap(pipes) > 0)
	require.Empty(t, pipes)
	pipes = pipes[0:1]
	sps := pipes[0].StoragePolicies
	require.Empty(t, sps)
	require.True(t, cap(sps) > 0)
	ops := pipes[0].Pipeline.Ops
	require.Empty(t, ops)
	require.True(t, cap(ops) > 0)
	ops = ops[0:1]
	id := ops[0].Rollup.Id
	require.Empty(t, id)
	require.True(t, cap(id) > 0)

	// verify the proto can be properly reused for unmarshaling
	require.NoError(t, pb.Unmarshal(b))
	require.Equal(t, input, pb)
}

func TestReuseMetricWithMetadatasProtoOnlyBatchTimer(t *testing.T) {
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
	ReuseMetricWithMetadatasProto(input)
	require.Equal(t, expected, input)
	require.True(t, cap(input.BatchTimerWithMetadatas.BatchTimer.Id) > 0)
	require.True(t, cap(input.BatchTimerWithMetadatas.Metadatas.Metadatas) > 0)
}

func TestReuseMetricWithMetadatasProtoOnlyGauge(t *testing.T) {
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
	ReuseMetricWithMetadatasProto(input)
	require.Equal(t, expected, input)
	require.True(t, cap(input.GaugeWithMetadatas.Gauge.Id) > 0)
	require.True(t, cap(input.GaugeWithMetadatas.Metadatas.Metadatas) > 0)
}

func TestReuseMetricWithMetadatasProtoOnlyForwardedMetric(t *testing.T) {
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
	ReuseMetricWithMetadatasProto(input)
	require.Equal(t, expected, input)
	require.True(t, cap(input.ForwardedMetricWithMetadata.Metric.Id) > 0)
	require.True(t, cap(input.ForwardedMetricWithMetadata.Metric.Values) > 0)
	require.True(t, cap(input.ForwardedMetricWithMetadata.Metadata.Pipeline.Ops) > 0)
}

func TestReuseMetricWithMetadatasProtoAll(t *testing.T) {
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
	ReuseMetricWithMetadatasProto(input)
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
