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
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/generated/proto/transformationpb"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

var (
	testCounter1 = unaggregated.Counter{
		ID:    []byte("testCounter1"),
		Value: 123,
	}
	testCounter2 = unaggregated.Counter{
		ID:    []byte("testCounter2"),
		Value: 456,
	}
	testBatchTimer1 = unaggregated.BatchTimer{
		ID:     []byte("testBatchTimer1"),
		Values: []float64{3.67, -9.38},
	}
	testBatchTimer2 = unaggregated.BatchTimer{
		ID:     []byte("testBatchTimer2"),
		Values: []float64{4.57, 189234.01},
	}
	testGauge1 = unaggregated.Gauge{
		ID:    []byte("testGauge1"),
		Value: 845.23,
	}
	testGauge2 = unaggregated.Gauge{
		ID:    []byte("testGauge2"),
		Value: 234231.345,
	}
	testForwardedMetric1 = aggregated.ForwardedMetric{
		Type:      metric.CounterType,
		ID:        []byte("testForwardedMetric1"),
		TimeNanos: 8259,
		Values:    []float64{1, 3234, -12},
	}
	testForwardedMetric2 = aggregated.ForwardedMetric{
		Type:      metric.TimerType,
		ID:        []byte("testForwardedMetric2"),
		TimeNanos: 145668,
		Values:    []float64{563.875, -23.87},
	}
	testTimedMetric1 = aggregated.Metric{
		Type:      metric.CounterType,
		ID:        []byte("testTimedMetric1"),
		TimeNanos: 8259,
		Value:     3234,
	}
	testTimedMetric2 = aggregated.Metric{
		Type:      metric.GaugeType,
		ID:        []byte("testTimedMetric2"),
		TimeNanos: 82590,
		Value:     0,
	}
	testPassthroughMetric1 = aggregated.Metric{
		Type:      metric.CounterType,
		ID:        []byte("testPassthroughMetric1"),
		TimeNanos: 11111,
		Value:     1,
	}
	testPassthroughMetric2 = aggregated.Metric{
		Type:      metric.GaugeType,
		ID:        []byte("testPassthroughMetric2"),
		TimeNanos: 22222,
		Value:     2,
	}
	testStagedMetadatas1 = metadata.StagedMetadatas{
		{
			CutoverNanos: 1234,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
						},
					},
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
						},
						Pipeline: applied.NewPipeline([]applied.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: applied.RollupOp{
									ID:            []byte("baz"),
									AggregationID: aggregation.MustCompressTypes(aggregation.Mean),
								},
							},
						}),
					},
				},
			},
		},
	}
	testStagedMetadatas2 = metadata.StagedMetadatas{
		{
			CutoverNanos: 4567,
			Tombstoned:   false,
		},
		{
			CutoverNanos: 7890,
			Tombstoned:   true,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.MustCompressTypes(aggregation.Count),
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
						},
					},
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 6*time.Hour),
							policy.NewStoragePolicy(time.Hour, xtime.Hour, 30*24*time.Hour),
						},
						Pipeline: applied.NewPipeline([]applied.OpUnion{
							{
								Type: pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{
									Type: transformation.Absolute,
								},
							},
							{
								Type: pipeline.RollupOpType,
								Rollup: applied.RollupOp{
									ID:            []byte("foo"),
									AggregationID: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum),
								},
							},
						}),
					},
				},
			},
		},
		{
			CutoverNanos: 32768,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						Pipeline: applied.NewPipeline([]applied.OpUnion{
							{
								Type: pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{
									Type: transformation.PerSecond,
								},
							},
							{
								Type: pipeline.RollupOpType,
								Rollup: applied.RollupOp{
									ID:            []byte("bar"),
									AggregationID: aggregation.MustCompressTypes(aggregation.P99),
								},
							},
						}),
					},
				},
			},
		},
	}
	testForwardMetadata1 = metadata.ForwardMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicy: policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
		Pipeline: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("foo"),
					AggregationID: aggregation.MustCompressTypes(aggregation.Count),
				},
			},
		}),
		SourceID:          1234,
		NumForwardedTimes: 3,
	}
	testForwardMetadata2 = metadata.ForwardMetadata{
		AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
		StoragePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
		Pipeline: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{
					Type: transformation.Absolute,
				},
			},
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("bar"),
					AggregationID: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum),
				},
			},
		}),
		SourceID:          897,
		NumForwardedTimes: 2,
	}
	testTimedMetadata1 = metadata.TimedMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicy: policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
	}
	testTimedMetadata2 = metadata.TimedMetadata{
		AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
		StoragePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
	}
	testPassthroughMetadata1 = policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour)
	testPassthroughMetadata2 = policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour)
	testCounter1Proto        = metricpb.Counter{
		Id:    []byte("testCounter1"),
		Value: 123,
	}
	testCounter2Proto = metricpb.Counter{
		Id:    []byte("testCounter2"),
		Value: 456,
	}
	testBatchTimer1Proto = metricpb.BatchTimer{
		Id:     []byte("testBatchTimer1"),
		Values: []float64{3.67, -9.38},
	}
	testBatchTimer2Proto = metricpb.BatchTimer{
		Id:     []byte("testBatchTimer2"),
		Values: []float64{4.57, 189234.01},
	}
	testGauge1Proto = metricpb.Gauge{
		Id:    []byte("testGauge1"),
		Value: 845.23,
	}
	testGauge2Proto = metricpb.Gauge{
		Id:    []byte("testGauge2"),
		Value: 234231.345,
	}
	testForwardedMetric1Proto = metricpb.ForwardedMetric{
		Type:      metricpb.MetricType_COUNTER,
		Id:        []byte("testForwardedMetric1"),
		TimeNanos: 8259,
		Values:    []float64{1, 3234, -12},
	}
	testForwardedMetric2Proto = metricpb.ForwardedMetric{
		Type:      metricpb.MetricType_TIMER,
		Id:        []byte("testForwardedMetric2"),
		TimeNanos: 145668,
		Values:    []float64{563.875, -23.87},
	}
	testTimedMetric1Proto = metricpb.TimedMetric{
		Type:      metricpb.MetricType_COUNTER,
		Id:        []byte("testTimedMetric1"),
		TimeNanos: 8259,
		Value:     3234,
	}
	testTimedMetric2Proto = metricpb.TimedMetric{
		Type:      metricpb.MetricType_GAUGE,
		Id:        []byte("testTimedMetric2"),
		TimeNanos: 82590,
		Value:     0,
	}
	testPassthroughMetric1Proto = metricpb.TimedMetric{
		Type:      metricpb.MetricType_COUNTER,
		Id:        []byte("testPassthroughMetric1"),
		TimeNanos: 11111,
		Value:     1,
	}
	testPassthroughMetric2Proto = metricpb.TimedMetric{
		Type:      metricpb.MetricType_GAUGE,
		Id:        []byte("testPassthroughMetric2"),
		TimeNanos: 22222,
		Value:     2,
	}
	testStagedMetadatas1Proto = metricpb.StagedMetadatas{
		Metadatas: []metricpb.StagedMetadata{
			{
				CutoverNanos: 1234,
				Tombstoned:   false,
				Metadata: metricpb.Metadata{
					Pipelines: []metricpb.PipelineMetadata{
						{
							AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.Sum)[0]},
							StoragePolicies: []policypb.StoragePolicy{
								{
									Resolution: policypb.Resolution{
										WindowSize: time.Second.Nanoseconds(),
										Precision:  time.Second.Nanoseconds(),
									},
									Retention: policypb.Retention{
										Period: time.Hour.Nanoseconds(),
									},
								},
							},
						},
						{
							AggregationId: aggregationpb.AggregationID{Id: 0},
							StoragePolicies: []policypb.StoragePolicy{
								{
									Resolution: policypb.Resolution{
										WindowSize: 10 * time.Second.Nanoseconds(),
										Precision:  time.Second.Nanoseconds(),
									},
									Retention: policypb.Retention{
										Period: time.Hour.Nanoseconds(),
									},
								},
							},
							Pipeline: pipelinepb.AppliedPipeline{
								Ops: []pipelinepb.AppliedPipelineOp{
									{
										Type: pipelinepb.AppliedPipelineOp_ROLLUP,
										Rollup: pipelinepb.AppliedRollupOp{
											Id:            []byte("baz"),
											AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.Mean)[0]},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	testStagedMetadatas2Proto = metricpb.StagedMetadatas{
		Metadatas: []metricpb.StagedMetadata{
			{
				CutoverNanos: 4567,
				Tombstoned:   false,
			},
			{
				CutoverNanos: 7890,
				Tombstoned:   true,
				Metadata: metricpb.Metadata{
					Pipelines: []metricpb.PipelineMetadata{
						{
							AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.Count)[0]},
							StoragePolicies: []policypb.StoragePolicy{
								{
									Resolution: policypb.Resolution{
										WindowSize: time.Second.Nanoseconds(),
										Precision:  time.Second.Nanoseconds(),
									},
									Retention: policypb.Retention{
										Period: time.Hour.Nanoseconds(),
									},
								},
							},
						},
						{
							AggregationId: aggregationpb.AggregationID{Id: 0},
							StoragePolicies: []policypb.StoragePolicy{
								{
									Resolution: policypb.Resolution{
										WindowSize: time.Minute.Nanoseconds(),
										Precision:  time.Minute.Nanoseconds(),
									},
									Retention: policypb.Retention{
										Period: (6 * time.Hour).Nanoseconds(),
									},
								},
								{
									Resolution: policypb.Resolution{
										WindowSize: time.Hour.Nanoseconds(),
										Precision:  time.Hour.Nanoseconds(),
									},
									Retention: policypb.Retention{
										Period: (30 * 24 * time.Hour).Nanoseconds(),
									},
								},
							},
							Pipeline: pipelinepb.AppliedPipeline{
								Ops: []pipelinepb.AppliedPipelineOp{
									{
										Type: pipelinepb.AppliedPipelineOp_TRANSFORMATION,
										Transformation: pipelinepb.TransformationOp{
											Type: transformationpb.TransformationType_ABSOLUTE,
										},
									},
									{
										Type: pipelinepb.AppliedPipelineOp_ROLLUP,
										Rollup: pipelinepb.AppliedRollupOp{
											Id:            []byte("foo"),
											AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum)[0]},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				CutoverNanos: 32768,
				Tombstoned:   false,
				Metadata: metricpb.Metadata{
					Pipelines: []metricpb.PipelineMetadata{
						{
							AggregationId: aggregationpb.AggregationID{Id: 0},
							Pipeline: pipelinepb.AppliedPipeline{
								Ops: []pipelinepb.AppliedPipelineOp{
									{
										Type: pipelinepb.AppliedPipelineOp_TRANSFORMATION,
										Transformation: pipelinepb.TransformationOp{
											Type: transformationpb.TransformationType_PERSECOND,
										},
									},
									{
										Type: pipelinepb.AppliedPipelineOp_ROLLUP,
										Rollup: pipelinepb.AppliedRollupOp{
											Id:            []byte("bar"),
											AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.P99)[0]},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	testForwardMetadata1Proto = metricpb.ForwardMetadata{
		AggregationId: aggregationpb.AggregationID{Id: 0},
		StoragePolicy: policypb.StoragePolicy{
			Resolution: policypb.Resolution{
				WindowSize: time.Minute.Nanoseconds(),
				Precision:  time.Minute.Nanoseconds(),
			},
			Retention: policypb.Retention{
				Period: (12 * time.Hour).Nanoseconds(),
			},
		},
		Pipeline: pipelinepb.AppliedPipeline{
			Ops: []pipelinepb.AppliedPipelineOp{
				{
					Type: pipelinepb.AppliedPipelineOp_ROLLUP,
					Rollup: pipelinepb.AppliedRollupOp{
						Id:            []byte("foo"),
						AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.Count)[0]},
					},
				},
			},
		},
		SourceId:          1234,
		NumForwardedTimes: 3,
	}
	testForwardMetadata2Proto = metricpb.ForwardMetadata{
		AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.Sum)[0]},
		StoragePolicy: policypb.StoragePolicy{
			Resolution: policypb.Resolution{
				WindowSize: 10 * time.Second.Nanoseconds(),
				Precision:  time.Second.Nanoseconds(),
			},
			Retention: policypb.Retention{
				Period: (6 * time.Hour).Nanoseconds(),
			},
		},
		Pipeline: pipelinepb.AppliedPipeline{
			Ops: []pipelinepb.AppliedPipelineOp{
				{
					Type: pipelinepb.AppliedPipelineOp_TRANSFORMATION,
					Transformation: pipelinepb.TransformationOp{
						Type: transformationpb.TransformationType_ABSOLUTE,
					},
				},
				{
					Type: pipelinepb.AppliedPipelineOp_ROLLUP,
					Rollup: pipelinepb.AppliedRollupOp{
						Id:            []byte("bar"),
						AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum)[0]},
					},
				},
			},
		},
		SourceId:          897,
		NumForwardedTimes: 2,
	}
	testTimedMetadata1Proto = metricpb.TimedMetadata{
		AggregationId: aggregationpb.AggregationID{Id: 0},
		StoragePolicy: policypb.StoragePolicy{
			Resolution: policypb.Resolution{
				WindowSize: time.Minute.Nanoseconds(),
				Precision:  time.Minute.Nanoseconds(),
			},
			Retention: policypb.Retention{
				Period: (12 * time.Hour).Nanoseconds(),
			},
		},
	}
	testTimedMetadata2Proto = metricpb.TimedMetadata{
		AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.Sum)[0]},
		StoragePolicy: policypb.StoragePolicy{
			Resolution: policypb.Resolution{
				WindowSize: 10 * time.Second.Nanoseconds(),
				Precision:  time.Second.Nanoseconds(),
			},
			Retention: policypb.Retention{
				Period: (6 * time.Hour).Nanoseconds(),
			},
		},
	}
	testPassthroughMetadata1Proto = policypb.StoragePolicy{
		Resolution: policypb.Resolution{
			WindowSize: time.Minute.Nanoseconds(),
			Precision:  time.Minute.Nanoseconds(),
		},
		Retention: policypb.Retention{
			Period: (12 * time.Hour).Nanoseconds(),
		},
	}
	testPassthroughMetadata2Proto = policypb.StoragePolicy{
		Resolution: policypb.Resolution{
			WindowSize: 10 * time.Second.Nanoseconds(),
			Precision:  time.Second.Nanoseconds(),
		},
		Retention: policypb.Retention{
			Period: (6 * time.Hour).Nanoseconds(),
		},
	}
	testCmpOpts = []cmp.Option{
		cmpopts.EquateEmpty(),
		cmp.AllowUnexported(policy.StoragePolicy{}),
	}
)

func TestUnaggregatedEncoderEncodeCounterWithMetadatas(t *testing.T) {
	inputs := []unaggregated.CounterWithMetadatas{
		{
			Counter:         testCounter1,
			StagedMetadatas: testStagedMetadatas1,
		},
		{
			Counter:         testCounter2,
			StagedMetadatas: testStagedMetadatas1,
		},
		{
			Counter:         testCounter1,
			StagedMetadatas: testStagedMetadatas2,
		},
		{
			Counter:         testCounter2,
			StagedMetadatas: testStagedMetadatas2,
		},
	}
	expected := []metricpb.CounterWithMetadatas{
		{
			Counter:   testCounter1Proto,
			Metadatas: testStagedMetadatas1Proto,
		},
		{
			Counter:   testCounter2Proto,
			Metadatas: testStagedMetadatas1Proto,
		},
		{
			Counter:   testCounter1Proto,
			Metadatas: testStagedMetadatas2Proto,
		},
		{
			Counter:   testCounter2Proto,
			Metadatas: testStagedMetadatas2Proto,
		},
	}

	var (
		sizeRes int
		pbRes   metricpb.MetricWithMetadatas
	)
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	enc.(*unaggregatedEncoder).encodeMessageSizeFn = func(size int) { sizeRes = size }
	enc.(*unaggregatedEncoder).encodeMessageFn = func(pb metricpb.MetricWithMetadatas) error { pbRes = pb; return nil }
	for i, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:                 encoding.CounterWithMetadatasType,
			CounterWithMetadatas: input,
		}))
		expectedProto := metricpb.MetricWithMetadatas{
			Type:                 metricpb.MetricWithMetadatas_COUNTER_WITH_METADATAS,
			CounterWithMetadatas: &expected[i],
		}
		expectedMsgSize := expectedProto.Size()
		require.Equal(t, expectedMsgSize, sizeRes)
		require.Equal(t, expectedProto, pbRes)
	}
}

func TestUnaggregatedEncoderEncodeBatchTimerWithMetadatas(t *testing.T) {
	inputs := []unaggregated.BatchTimerWithMetadatas{
		{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testStagedMetadatas1,
		},
		{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testStagedMetadatas1,
		},
		{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testStagedMetadatas2,
		},
		{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testStagedMetadatas2,
		},
	}
	expected := []metricpb.BatchTimerWithMetadatas{
		{
			BatchTimer: testBatchTimer1Proto,
			Metadatas:  testStagedMetadatas1Proto,
		},
		{
			BatchTimer: testBatchTimer2Proto,
			Metadatas:  testStagedMetadatas1Proto,
		},
		{
			BatchTimer: testBatchTimer1Proto,
			Metadatas:  testStagedMetadatas2Proto,
		},
		{
			BatchTimer: testBatchTimer2Proto,
			Metadatas:  testStagedMetadatas2Proto,
		},
	}

	var (
		sizeRes int
		pbRes   metricpb.MetricWithMetadatas
	)
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	enc.(*unaggregatedEncoder).encodeMessageSizeFn = func(size int) { sizeRes = size }
	enc.(*unaggregatedEncoder).encodeMessageFn = func(pb metricpb.MetricWithMetadatas) error { pbRes = pb; return nil }
	for i, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:                    encoding.BatchTimerWithMetadatasType,
			BatchTimerWithMetadatas: input,
		}))
		expectedProto := metricpb.MetricWithMetadatas{
			Type:                    metricpb.MetricWithMetadatas_BATCH_TIMER_WITH_METADATAS,
			BatchTimerWithMetadatas: &expected[i],
		}
		expectedMsgSize := expectedProto.Size()
		require.Equal(t, expectedMsgSize, sizeRes)
		require.Equal(t, expectedProto, pbRes)
	}
}

func TestUnaggregatedEncoderEncodeGaugeWithMetadatas(t *testing.T) {
	inputs := []unaggregated.GaugeWithMetadatas{
		{
			Gauge:           testGauge1,
			StagedMetadatas: testStagedMetadatas1,
		},
		{
			Gauge:           testGauge2,
			StagedMetadatas: testStagedMetadatas1,
		},
		{
			Gauge:           testGauge1,
			StagedMetadatas: testStagedMetadatas2,
		},
		{
			Gauge:           testGauge2,
			StagedMetadatas: testStagedMetadatas2,
		},
	}
	expected := []metricpb.GaugeWithMetadatas{
		{
			Gauge:     testGauge1Proto,
			Metadatas: testStagedMetadatas1Proto,
		},
		{
			Gauge:     testGauge2Proto,
			Metadatas: testStagedMetadatas1Proto,
		},
		{
			Gauge:     testGauge1Proto,
			Metadatas: testStagedMetadatas2Proto,
		},
		{
			Gauge:     testGauge2Proto,
			Metadatas: testStagedMetadatas2Proto,
		},
	}

	var (
		sizeRes int
		pbRes   metricpb.MetricWithMetadatas
	)
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	enc.(*unaggregatedEncoder).encodeMessageSizeFn = func(size int) { sizeRes = size }
	enc.(*unaggregatedEncoder).encodeMessageFn = func(pb metricpb.MetricWithMetadatas) error { pbRes = pb; return nil }
	for i, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:               encoding.GaugeWithMetadatasType,
			GaugeWithMetadatas: input,
		}))
		expectedProto := metricpb.MetricWithMetadatas{
			Type:               metricpb.MetricWithMetadatas_GAUGE_WITH_METADATAS,
			GaugeWithMetadatas: &expected[i],
		}
		expectedMsgSize := expectedProto.Size()
		require.Equal(t, expectedMsgSize, sizeRes)
		require.Equal(t, expectedProto, pbRes)
	}
}

func TestUnaggregatedEncoderEncodeForwardedMetricWithMetadata(t *testing.T) {
	inputs := []aggregated.ForwardedMetricWithMetadata{
		{
			ForwardedMetric: testForwardedMetric1,
			ForwardMetadata: testForwardMetadata1,
		},
		{
			ForwardedMetric: testForwardedMetric1,
			ForwardMetadata: testForwardMetadata2,
		},
		{
			ForwardedMetric: testForwardedMetric2,
			ForwardMetadata: testForwardMetadata1,
		},
		{
			ForwardedMetric: testForwardedMetric2,
			ForwardMetadata: testForwardMetadata2,
		},
	}
	expected := []metricpb.ForwardedMetricWithMetadata{
		{
			Metric:   testForwardedMetric1Proto,
			Metadata: testForwardMetadata1Proto,
		},
		{
			Metric:   testForwardedMetric1Proto,
			Metadata: testForwardMetadata2Proto,
		},
		{
			Metric:   testForwardedMetric2Proto,
			Metadata: testForwardMetadata1Proto,
		},
		{
			Metric:   testForwardedMetric2Proto,
			Metadata: testForwardMetadata2Proto,
		},
	}

	var (
		sizeRes int
		pbRes   metricpb.MetricWithMetadatas
	)
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	enc.(*unaggregatedEncoder).encodeMessageSizeFn = func(size int) { sizeRes = size }
	enc.(*unaggregatedEncoder).encodeMessageFn = func(pb metricpb.MetricWithMetadatas) error { pbRes = pb; return nil }
	for i, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:                        encoding.ForwardedMetricWithMetadataType,
			ForwardedMetricWithMetadata: input,
		}))
		expectedProto := metricpb.MetricWithMetadatas{
			Type:                        metricpb.MetricWithMetadatas_FORWARDED_METRIC_WITH_METADATA,
			ForwardedMetricWithMetadata: &expected[i],
		}
		expectedMsgSize := expectedProto.Size()
		require.Equal(t, expectedMsgSize, sizeRes)
		require.Equal(t, expectedProto, pbRes)
	}
}

func TestUnaggregatedEncoderEncodeTimedMetricWithMetadata(t *testing.T) {
	inputs := []aggregated.TimedMetricWithMetadata{
		{
			Metric:        testTimedMetric1,
			TimedMetadata: testTimedMetadata1,
		},
		{
			Metric:        testTimedMetric1,
			TimedMetadata: testTimedMetadata2,
		},
		{
			Metric:        testTimedMetric2,
			TimedMetadata: testTimedMetadata1,
		},
		{
			Metric:        testTimedMetric2,
			TimedMetadata: testTimedMetadata2,
		},
	}
	expected := []metricpb.TimedMetricWithMetadata{
		{
			Metric:   testTimedMetric1Proto,
			Metadata: testTimedMetadata1Proto,
		},
		{
			Metric:   testTimedMetric1Proto,
			Metadata: testTimedMetadata2Proto,
		},
		{
			Metric:   testTimedMetric2Proto,
			Metadata: testTimedMetadata1Proto,
		},
		{
			Metric:   testTimedMetric2Proto,
			Metadata: testTimedMetadata2Proto,
		},
	}

	var (
		sizeRes int
		pbRes   metricpb.MetricWithMetadatas
	)
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	enc.(*unaggregatedEncoder).encodeMessageSizeFn = func(size int) { sizeRes = size }
	enc.(*unaggregatedEncoder).encodeMessageFn = func(pb metricpb.MetricWithMetadatas) error { pbRes = pb; return nil }
	for i, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:                    encoding.TimedMetricWithMetadataType,
			TimedMetricWithMetadata: input,
		}))
		expectedProto := metricpb.MetricWithMetadatas{
			Type:                    metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATA,
			TimedMetricWithMetadata: &expected[i],
		}
		expectedMsgSize := expectedProto.Size()
		require.Equal(t, expectedMsgSize, sizeRes)
		require.Equal(t, expectedProto, pbRes)
	}
}

func TestUnaggregatedEncoderEncodePassthroughMetricWithMetadata(t *testing.T) {
	inputs := []aggregated.PassthroughMetricWithMetadata{
		{
			Metric:        testPassthroughMetric1,
			StoragePolicy: testPassthroughMetadata1,
		},
		{
			Metric:        testPassthroughMetric1,
			StoragePolicy: testPassthroughMetadata2,
		},
		{
			Metric:        testPassthroughMetric2,
			StoragePolicy: testPassthroughMetadata1,
		},
		{
			Metric:        testPassthroughMetric2,
			StoragePolicy: testPassthroughMetadata2,
		},
	}
	expected := []metricpb.TimedMetricWithStoragePolicy{
		{
			TimedMetric:   testPassthroughMetric1Proto,
			StoragePolicy: testPassthroughMetadata1Proto,
		},
		{
			TimedMetric:   testPassthroughMetric1Proto,
			StoragePolicy: testPassthroughMetadata2Proto,
		},
		{
			TimedMetric:   testPassthroughMetric2Proto,
			StoragePolicy: testPassthroughMetadata1Proto,
		},
		{
			TimedMetric:   testPassthroughMetric2Proto,
			StoragePolicy: testPassthroughMetadata2Proto,
		},
	}

	var (
		sizeRes int
		pbRes   metricpb.MetricWithMetadatas
	)
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	enc.(*unaggregatedEncoder).encodeMessageSizeFn = func(size int) { sizeRes = size }
	enc.(*unaggregatedEncoder).encodeMessageFn = func(pb metricpb.MetricWithMetadatas) error { pbRes = pb; return nil }
	for i, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:                          encoding.PassthroughMetricWithMetadataType,
			PassthroughMetricWithMetadata: input,
		}))
		expectedProto := metricpb.MetricWithMetadatas{
			Type:                         metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_STORAGE_POLICY,
			TimedMetricWithStoragePolicy: &expected[i],
		}
		expectedMsgSize := expectedProto.Size()
		require.Equal(t, expectedMsgSize, sizeRes)
		require.Equal(t, expectedProto, pbRes)
	}
}

func TestUnaggregatedEncoderStress(t *testing.T) {
	inputs := []interface{}{
		unaggregated.CounterWithMetadatas{
			Counter:         testCounter1,
			StagedMetadatas: testStagedMetadatas1,
		},
		unaggregated.BatchTimerWithMetadatas{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testStagedMetadatas1,
		},
		unaggregated.GaugeWithMetadatas{
			Gauge:           testGauge1,
			StagedMetadatas: testStagedMetadatas1,
		},
		aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: testForwardedMetric1,
			ForwardMetadata: testForwardMetadata1,
		},
		aggregated.TimedMetricWithMetadata{
			Metric:        testTimedMetric1,
			TimedMetadata: testTimedMetadata1,
		},
		aggregated.PassthroughMetricWithMetadata{
			Metric:        testPassthroughMetric1,
			StoragePolicy: testPassthroughMetadata1,
		},
		unaggregated.CounterWithMetadatas{
			Counter:         testCounter2,
			StagedMetadatas: testStagedMetadatas1,
		},
		unaggregated.BatchTimerWithMetadatas{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testStagedMetadatas1,
		},
		unaggregated.GaugeWithMetadatas{
			Gauge:           testGauge2,
			StagedMetadatas: testStagedMetadatas1,
		},
		aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: testForwardedMetric2,
			ForwardMetadata: testForwardMetadata1,
		},
		unaggregated.CounterWithMetadatas{
			Counter:         testCounter1,
			StagedMetadatas: testStagedMetadatas2,
		},
		unaggregated.BatchTimerWithMetadatas{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testStagedMetadatas2,
		},
		unaggregated.GaugeWithMetadatas{
			Gauge:           testGauge1,
			StagedMetadatas: testStagedMetadatas2,
		},
		aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: testForwardedMetric1,
			ForwardMetadata: testForwardMetadata2,
		},
		unaggregated.CounterWithMetadatas{
			Counter:         testCounter2,
			StagedMetadatas: testStagedMetadatas2,
		},
		unaggregated.BatchTimerWithMetadatas{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testStagedMetadatas2,
		},
		unaggregated.GaugeWithMetadatas{
			Gauge:           testGauge2,
			StagedMetadatas: testStagedMetadatas2,
		},
		aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: testForwardedMetric2,
			ForwardMetadata: testForwardMetadata2,
		},
		aggregated.TimedMetricWithMetadata{
			Metric:        testTimedMetric2,
			TimedMetadata: testTimedMetadata2,
		},
		aggregated.PassthroughMetricWithMetadata{
			Metric:        testPassthroughMetric2,
			StoragePolicy: testPassthroughMetadata2,
		},
	}

	expected := []interface{}{
		metricpb.CounterWithMetadatas{
			Counter:   testCounter1Proto,
			Metadatas: testStagedMetadatas1Proto,
		},
		metricpb.BatchTimerWithMetadatas{
			BatchTimer: testBatchTimer1Proto,
			Metadatas:  testStagedMetadatas1Proto,
		},
		metricpb.GaugeWithMetadatas{
			Gauge:     testGauge1Proto,
			Metadatas: testStagedMetadatas1Proto,
		},
		metricpb.ForwardedMetricWithMetadata{
			Metric:   testForwardedMetric1Proto,
			Metadata: testForwardMetadata1Proto,
		},
		metricpb.TimedMetricWithMetadata{
			Metric:   testTimedMetric1Proto,
			Metadata: testTimedMetadata1Proto,
		},
		metricpb.TimedMetricWithStoragePolicy{
			TimedMetric:   testPassthroughMetric1Proto,
			StoragePolicy: testPassthroughMetadata1Proto,
		},
		metricpb.CounterWithMetadatas{
			Counter:   testCounter2Proto,
			Metadatas: testStagedMetadatas1Proto,
		},
		metricpb.BatchTimerWithMetadatas{
			BatchTimer: testBatchTimer2Proto,
			Metadatas:  testStagedMetadatas1Proto,
		},
		metricpb.GaugeWithMetadatas{
			Gauge:     testGauge2Proto,
			Metadatas: testStagedMetadatas1Proto,
		},
		metricpb.ForwardedMetricWithMetadata{
			Metric:   testForwardedMetric2Proto,
			Metadata: testForwardMetadata1Proto,
		},
		metricpb.CounterWithMetadatas{
			Counter:   testCounter1Proto,
			Metadatas: testStagedMetadatas2Proto,
		},
		metricpb.BatchTimerWithMetadatas{
			BatchTimer: testBatchTimer1Proto,
			Metadatas:  testStagedMetadatas2Proto,
		},
		metricpb.GaugeWithMetadatas{
			Gauge:     testGauge1Proto,
			Metadatas: testStagedMetadatas2Proto,
		},
		metricpb.ForwardedMetricWithMetadata{
			Metric:   testForwardedMetric1Proto,
			Metadata: testForwardMetadata2Proto,
		},
		metricpb.CounterWithMetadatas{
			Counter:   testCounter2Proto,
			Metadatas: testStagedMetadatas2Proto,
		},
		metricpb.BatchTimerWithMetadatas{
			BatchTimer: testBatchTimer2Proto,
			Metadatas:  testStagedMetadatas2Proto,
		},
		metricpb.GaugeWithMetadatas{
			Gauge:     testGauge2Proto,
			Metadatas: testStagedMetadatas2Proto,
		},
		metricpb.ForwardedMetricWithMetadata{
			Metric:   testForwardedMetric2Proto,
			Metadata: testForwardMetadata2Proto,
		},
		metricpb.TimedMetricWithMetadata{
			Metric:   testTimedMetric2Proto,
			Metadata: testTimedMetadata2Proto,
		},
		metricpb.TimedMetricWithStoragePolicy{
			TimedMetric:   testPassthroughMetric2Proto,
			StoragePolicy: testPassthroughMetadata2Proto,
		},
	}

	var (
		sizeRes int
		pbRes   metricpb.MetricWithMetadatas
		numIter = 1000
	)
	opts := NewUnaggregatedOptions().SetInitBufferSize(2)
	enc := NewUnaggregatedEncoder(opts)
	enc.(*unaggregatedEncoder).encodeMessageSizeFn = func(size int) { sizeRes = size }
	enc.(*unaggregatedEncoder).encodeMessageFn = func(pb metricpb.MetricWithMetadatas) error { pbRes = pb; return nil }
	for iter := 0; iter < numIter; iter++ {
		for i, input := range inputs {
			var (
				msg           encoding.UnaggregatedMessageUnion
				expectedProto metricpb.MetricWithMetadatas
			)
			switch input := input.(type) {
			case unaggregated.CounterWithMetadatas:
				msg = encoding.UnaggregatedMessageUnion{
					Type:                 encoding.CounterWithMetadatasType,
					CounterWithMetadatas: input,
				}
				res := expected[i].(metricpb.CounterWithMetadatas)
				expectedProto = metricpb.MetricWithMetadatas{
					Type:                 metricpb.MetricWithMetadatas_COUNTER_WITH_METADATAS,
					CounterWithMetadatas: &res,
				}
			case unaggregated.BatchTimerWithMetadatas:
				msg = encoding.UnaggregatedMessageUnion{
					Type:                    encoding.BatchTimerWithMetadatasType,
					BatchTimerWithMetadatas: input,
				}
				res := expected[i].(metricpb.BatchTimerWithMetadatas)
				expectedProto = metricpb.MetricWithMetadatas{
					Type:                    metricpb.MetricWithMetadatas_BATCH_TIMER_WITH_METADATAS,
					BatchTimerWithMetadatas: &res,
				}
			case unaggregated.GaugeWithMetadatas:
				msg = encoding.UnaggregatedMessageUnion{
					Type:               encoding.GaugeWithMetadatasType,
					GaugeWithMetadatas: input,
				}
				res := expected[i].(metricpb.GaugeWithMetadatas)
				expectedProto = metricpb.MetricWithMetadatas{
					Type:               metricpb.MetricWithMetadatas_GAUGE_WITH_METADATAS,
					GaugeWithMetadatas: &res,
				}
			case aggregated.ForwardedMetricWithMetadata:
				msg = encoding.UnaggregatedMessageUnion{
					Type:                        encoding.ForwardedMetricWithMetadataType,
					ForwardedMetricWithMetadata: input,
				}
				res := expected[i].(metricpb.ForwardedMetricWithMetadata)
				expectedProto = metricpb.MetricWithMetadatas{
					Type:                        metricpb.MetricWithMetadatas_FORWARDED_METRIC_WITH_METADATA,
					ForwardedMetricWithMetadata: &res,
				}
			case aggregated.TimedMetricWithMetadata:
				msg = encoding.UnaggregatedMessageUnion{
					Type:                    encoding.TimedMetricWithMetadataType,
					TimedMetricWithMetadata: input,
				}
				res := expected[i].(metricpb.TimedMetricWithMetadata)
				expectedProto = metricpb.MetricWithMetadatas{
					Type:                    metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATA,
					TimedMetricWithMetadata: &res,
				}
			case aggregated.PassthroughMetricWithMetadata:
				msg = encoding.UnaggregatedMessageUnion{
					Type:                          encoding.PassthroughMetricWithMetadataType,
					PassthroughMetricWithMetadata: input,
				}
				res := expected[i].(metricpb.TimedMetricWithStoragePolicy)
				expectedProto = metricpb.MetricWithMetadatas{
					Type:                         metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_STORAGE_POLICY,
					TimedMetricWithStoragePolicy: &res,
				}
			default:
				require.Fail(t, "unrecognized type %T", input)
			}
			require.NoError(t, enc.EncodeMessage(msg))
			expectedMsgSize := expectedProto.Size()
			require.Equal(t, expectedMsgSize, sizeRes)
			require.True(t, cmp.Equal(expectedProto, pbRes, testCmpOpts...))
		}
	}
}

func TestUnaggregatedEncoderEncodeMessageInvalidMessageType(t *testing.T) {
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	msg := encoding.UnaggregatedMessageUnion{Type: encoding.UnknownMessageType}
	err := enc.EncodeMessage(msg)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "unknown message type"))
}

func TestUnaggregatedEncoderEncodeMessageTooLarge(t *testing.T) {
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.CounterWithMetadatasType,
		CounterWithMetadatas: unaggregated.CounterWithMetadatas{
			Counter:         testCounter1,
			StagedMetadatas: testStagedMetadatas1,
		},
	}
	opts := NewUnaggregatedOptions().SetMaxMessageSize(1)
	enc := NewUnaggregatedEncoder(opts)
	err := enc.EncodeMessage(msg)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "larger than maximum supported size"))
}

func TestUnaggregatedEncoderTruncate(t *testing.T) {
	opts := NewUnaggregatedOptions().SetInitBufferSize(2)
	enc := NewUnaggregatedEncoder(opts)
	encoder := enc.(*unaggregatedEncoder)
	buf := []byte{1, 2, 3, 4}
	enc.Reset(buf)
	require.Equal(t, 4, enc.Len())

	for i := 4; i >= 0; i-- {
		require.NoError(t, enc.Truncate(i))
		require.Equal(t, i, enc.Len())
		require.Equal(t, buf[:i], encoder.buf[:encoder.used])
	}
}

func TestUnaggregatedEncoderTruncateError(t *testing.T) {
	opts := NewUnaggregatedOptions().SetInitBufferSize(2)
	enc := NewUnaggregatedEncoder(opts)
	buf := []byte{1, 2, 3, 4}
	enc.Reset(buf)
	require.Equal(t, 4, enc.Len())

	invalidTargets := []int{-3, 5}
	for _, target := range invalidTargets {
		err := enc.Truncate(target)
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "truncation out of range"))
	}
}

func TestUnaggregatedEncoderEncodeMessageRelinquishReset(t *testing.T) {
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.CounterWithMetadatasType,
		CounterWithMetadatas: unaggregated.CounterWithMetadatas{
			Counter:         testCounter1,
			StagedMetadatas: testStagedMetadatas1,
		},
	}
	opts := NewUnaggregatedOptions().SetInitBufferSize(2)
	enc := NewUnaggregatedEncoder(opts)
	encoder := enc.(*unaggregatedEncoder)
	require.NoError(t, enc.EncodeMessage(msg))
	require.True(t, enc.Len() > 0)
	require.NotNil(t, encoder.buf)

	initData := []byte{1, 2, 3, 4}
	enc.Reset(initData)
	require.Equal(t, initData, encoder.buf)
	require.Equal(t, 4, enc.Len())

	// Verify the initial data has been copied.
	initData[0] = 123
	require.Equal(t, byte(1), encoder.buf[0])
}

func TestUnaggregatedEncoderRelinquish(t *testing.T) {
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.CounterWithMetadatasType,
		CounterWithMetadatas: unaggregated.CounterWithMetadatas{
			Counter:         testCounter1,
			StagedMetadatas: testStagedMetadatas1,
		},
	}
	opts := NewUnaggregatedOptions().SetInitBufferSize(2)
	enc := NewUnaggregatedEncoder(opts)
	encoder := enc.(*unaggregatedEncoder)
	require.NoError(t, enc.EncodeMessage(msg))
	require.True(t, enc.Len() > 0)
	require.NotNil(t, encoder.buf)

	var (
		size    = enc.Len()
		buf     = encoder.buf
		dataBuf = enc.Relinquish()
	)
	require.True(t, enc.Len() == 0)
	require.Nil(t, encoder.buf)
	require.Equal(t, buf[:size], dataBuf.Bytes())
}
