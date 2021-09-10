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

package unaggregated

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/generated/proto/transformationpb"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	testCounter = Counter{
		ID:    []byte("testCounter"),
		Value: 1234,
	}
	testCounterUnion = MetricUnion{
		Type:       metric.CounterType,
		ID:         []byte("testCounter"),
		CounterVal: 1234,
	}
	testBatchTimer = BatchTimer{
		ID:     []byte("testBatchTimer"),
		Values: []float64{4.78, -2384, 0.0, 3145, 9999},
	}
	testBatchTimerUnion = MetricUnion{
		Type:          metric.TimerType,
		ID:            []byte("testBatchTimer"),
		BatchTimerVal: []float64{4.78, -2384, 0.0, 3145, 9999},
	}
	testGauge = Gauge{
		ID:    []byte("testGauge"),
		Value: 45.28,
	}
	testGaugeUnion = MetricUnion{
		Type:     metric.GaugeType,
		ID:       []byte("testGauge"),
		GaugeVal: 45.28,
	}
	testMetadatas = metadata.StagedMetadatas{
		{
			CutoverNanos: 1234,
			Tombstoned:   false,
		},
		{
			CutoverNanos: 4567,
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
	testBadMetadatas = metadata.StagedMetadatas{
		{
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Minute, xtime.Unit(100), 6*time.Hour),
						},
					},
				},
			},
		},
	}
	testCounterWithMetadatas = CounterWithMetadatas{
		Counter:         testCounter,
		StagedMetadatas: testMetadatas,
	}
	testBatchTimerWithMetadatas = BatchTimerWithMetadatas{
		BatchTimer:      testBatchTimer,
		StagedMetadatas: testMetadatas,
	}
	testGaugeWithMetadatas = GaugeWithMetadatas{
		Gauge:           testGauge,
		StagedMetadatas: testMetadatas,
	}
	testCounterProto = metricpb.Counter{
		Id:    []byte("testCounter"),
		Value: 1234,
	}
	testBatchTimerProto = metricpb.BatchTimer{
		Id:     []byte("testBatchTimer"),
		Values: []float64{4.78, -2384, 0.0, 3145, 9999},
	}
	testGaugeProto = metricpb.Gauge{
		Id:    []byte("testGauge"),
		Value: 45.28,
	}
	testMetadatasProto = metricpb.StagedMetadatas{
		Metadatas: []metricpb.StagedMetadata{
			{
				CutoverNanos: 1234,
				Tombstoned:   false,
			},
			{
				CutoverNanos: 4567,
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
	testBadMetadatasProto = metricpb.StagedMetadatas{
		Metadatas: []metricpb.StagedMetadata{
			{
				Metadata: metricpb.Metadata{
					Pipelines: []metricpb.PipelineMetadata{
						{
							StoragePolicies: []policypb.StoragePolicy{
								{},
							},
						},
					},
				},
			},
		},
	}
	testCounterWithMetadatasProto = metricpb.CounterWithMetadatas{
		Counter:   testCounterProto,
		Metadatas: testMetadatasProto,
	}
	testBatchTimerWithMetadatasProto = metricpb.BatchTimerWithMetadatas{
		BatchTimer: testBatchTimerProto,
		Metadatas:  testMetadatasProto,
	}
	testGaugeWithMetadatasProto = metricpb.GaugeWithMetadatas{
		Gauge:     testGaugeProto,
		Metadatas: testMetadatasProto,
	}
)

func TestCounterToUnion(t *testing.T) {
	require.Equal(t, testCounterUnion, testCounter.ToUnion())
}

func TestCounterToProto(t *testing.T) {
	var pb metricpb.Counter
	testCounter.ToProto(&pb)
	require.Equal(t, testCounterProto, pb)
}

func TestCounterFromProto(t *testing.T) {
	var c Counter
	c.FromProto(testCounterProto)
	require.Equal(t, testCounter, c)
}

func TestCounterRoundTrip(t *testing.T) {
	var (
		pb metricpb.Counter
		c  Counter
	)
	testCounter.ToProto(&pb)
	c.FromProto(pb)
	require.Equal(t, testCounter, c)
}

func TestBatchTimerToUnion(t *testing.T) {
	require.Equal(t, testBatchTimerUnion, testBatchTimer.ToUnion())
}

func TestBatchTimerToProto(t *testing.T) {
	var pb metricpb.BatchTimer
	testBatchTimer.ToProto(&pb)
	require.Equal(t, testBatchTimerProto, pb)
}

func TestBatchTimerFromProto(t *testing.T) {
	var c BatchTimer
	c.FromProto(testBatchTimerProto)
	require.Equal(t, testBatchTimer, c)
}

func TestBatchTimerRoundTrip(t *testing.T) {
	var (
		pb metricpb.BatchTimer
		c  BatchTimer
	)
	testBatchTimer.ToProto(&pb)
	c.FromProto(pb)
	require.Equal(t, testBatchTimer, c)
}

func TestGaugeToUnion(t *testing.T) {
	require.Equal(t, testGaugeUnion, testGauge.ToUnion())
}

func TestGaugeToProto(t *testing.T) {
	var pb metricpb.Gauge
	testGauge.ToProto(&pb)
	require.Equal(t, testGaugeProto, pb)
}

func TestGaugeFromProto(t *testing.T) {
	var c Gauge
	c.FromProto(testGaugeProto)
	require.Equal(t, testGauge, c)
}

func TestGaugeRoundTrip(t *testing.T) {
	var (
		pb metricpb.Gauge
		c  Gauge
	)
	testGauge.ToProto(&pb)
	c.FromProto(pb)
	require.Equal(t, testGauge, c)
}

func TestCounterWithMetadatasToProto(t *testing.T) {
	var pb metricpb.CounterWithMetadatas
	require.NoError(t, testCounterWithMetadatas.ToProto(&pb))
	require.Equal(t, testCounterWithMetadatasProto, pb)
}

func TestCounterWithMetadatasToProtoBadMetadatas(t *testing.T) {
	var pb metricpb.CounterWithMetadatas
	badCounterWithMetadatas := CounterWithMetadatas{
		Counter:         testCounter,
		StagedMetadatas: testBadMetadatas,
	}
	require.Error(t, badCounterWithMetadatas.ToProto(&pb))
}

func TestCounterWithMetadatasFromProto(t *testing.T) {
	var c CounterWithMetadatas
	require.NoError(t, c.FromProto(&testCounterWithMetadatasProto))
	require.Equal(t, testCounterWithMetadatas, c)
}

func TestCounterWithMetadatasFromProtoNilProto(t *testing.T) {
	var c CounterWithMetadatas
	require.Equal(t, errNilCounterWithMetadatasProto, c.FromProto(nil))
}

func TestCounterWithMetadatasFromProtoBadProto(t *testing.T) {
	var c CounterWithMetadatas
	badCounterWithMetadatasProto := metricpb.CounterWithMetadatas{
		Counter:   testCounterProto,
		Metadatas: testBadMetadatasProto,
	}
	require.Error(t, c.FromProto(&badCounterWithMetadatasProto))
}

func TestCounterWithMetadatasRoundTrip(t *testing.T) {
	var (
		pb metricpb.CounterWithMetadatas
		c  CounterWithMetadatas
	)
	require.NoError(t, testCounterWithMetadatas.ToProto(&pb))
	require.NoError(t, c.FromProto(&pb))
	require.Equal(t, testCounterWithMetadatas, c)
}

func TestBatchTimerWithMetadatasToProto(t *testing.T) {
	var pb metricpb.BatchTimerWithMetadatas
	require.NoError(t, testBatchTimerWithMetadatas.ToProto(&pb))
	require.Equal(t, testBatchTimerWithMetadatasProto, pb)
}

func TestBatchTimerWithMetadatasToProtoBadMetadatas(t *testing.T) {
	var pb metricpb.BatchTimerWithMetadatas
	badBatchTimerWithMetadatas := BatchTimerWithMetadatas{
		BatchTimer:      testBatchTimer,
		StagedMetadatas: testBadMetadatas,
	}
	require.Error(t, badBatchTimerWithMetadatas.ToProto(&pb))
}

func TestBatchTimerWithMetadatasFromProto(t *testing.T) {
	var b BatchTimerWithMetadatas
	require.NoError(t, b.FromProto(&testBatchTimerWithMetadatasProto))
	require.Equal(t, testBatchTimerWithMetadatas, b)
}

func TestBatchTimerWithMetadatasFromProtoNilProto(t *testing.T) {
	var b BatchTimerWithMetadatas
	require.Equal(t, errNilBatchTimerWithMetadatasProto, b.FromProto(nil))
}

func TestBatchTimerWithMetadatasFromProtoBadProto(t *testing.T) {
	var b BatchTimerWithMetadatas
	badBatchTimerWithMetadatasProto := metricpb.BatchTimerWithMetadatas{
		BatchTimer: testBatchTimerProto,
		Metadatas:  testBadMetadatasProto,
	}
	require.Error(t, b.FromProto(&badBatchTimerWithMetadatasProto))
}

func TestBatchTimerWithMetadatasRoundTrip(t *testing.T) {
	var (
		pb metricpb.BatchTimerWithMetadatas
		b  BatchTimerWithMetadatas
	)
	require.NoError(t, testBatchTimerWithMetadatas.ToProto(&pb))
	require.NoError(t, b.FromProto(&pb))
	require.Equal(t, testBatchTimerWithMetadatas, b)
}

func TestGaugeWithMetadatasToProto(t *testing.T) {
	var pb metricpb.GaugeWithMetadatas
	require.NoError(t, testGaugeWithMetadatas.ToProto(&pb))
	require.Equal(t, testGaugeWithMetadatasProto, pb)
}

func TestGaugeWithMetadatasToProtoBadMetadatas(t *testing.T) {
	var pb metricpb.GaugeWithMetadatas
	badGaugeWithMetadatas := GaugeWithMetadatas{
		Gauge:           testGauge,
		StagedMetadatas: testBadMetadatas,
	}
	require.Error(t, badGaugeWithMetadatas.ToProto(&pb))
}

func TestGaugeWithMetadatasFromProto(t *testing.T) {
	var g GaugeWithMetadatas
	require.NoError(t, g.FromProto(&testGaugeWithMetadatasProto))
	require.Equal(t, testGaugeWithMetadatas, g)
}

func TestGaugeWithMetadatasFromProtoNilProto(t *testing.T) {
	var g GaugeWithMetadatas
	require.Equal(t, errNilGaugeWithMetadatasProto, g.FromProto(nil))
}

func TestGaugeWithMetadatasFromProtoBadProto(t *testing.T) {
	var g GaugeWithMetadatas
	badGaugeWithMetadatasProto := metricpb.GaugeWithMetadatas{
		Gauge:     testGaugeProto,
		Metadatas: testBadMetadatasProto,
	}
	require.Error(t, g.FromProto(&badGaugeWithMetadatasProto))
}

func TestGaugeWithMetadatasRoundTrip(t *testing.T) {
	var (
		pb metricpb.GaugeWithMetadatas
		g  GaugeWithMetadatas
	)
	require.NoError(t, testGaugeWithMetadatas.ToProto(&pb))
	require.NoError(t, g.FromProto(&pb))
	require.Equal(t, testGaugeWithMetadatas, g)
}
