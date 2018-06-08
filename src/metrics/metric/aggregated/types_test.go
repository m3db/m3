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

package aggregated

import (
	"testing"
	"time"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3metrics/generated/proto/metricpb"
	"github.com/m3db/m3metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3metrics/generated/proto/policypb"
	"github.com/m3db/m3metrics/generated/proto/transformationpb"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/pipeline"
	"github.com/m3db/m3metrics/pipeline/applied"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/transformation"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testMetric1 = Metric{
		Type:      metric.CounterType,
		ID:        []byte("testMetric1"),
		TimeNanos: 12345,
		Value:     33.87,
	}
	testMetric2 = Metric{
		Type:      metric.TimerType,
		ID:        []byte("testMetric2"),
		TimeNanos: 67890,
		Value:     21.99,
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
		SourceID:          []byte("testForward1"),
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
		SourceID:          []byte("testForward2"),
		NumForwardedTimes: 2,
	}
	testBadMetric = Metric{
		Type: metric.UnknownType,
	}
	testBadForwardMetadata = metadata.ForwardMetadata{
		StoragePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Unit(101), 6*time.Hour),
	}
	testMetric1Proto = metricpb.TimedMetric{
		Type:      metricpb.MetricType_COUNTER,
		Id:        []byte("testMetric1"),
		TimeNanos: 12345,
		Value:     33.87,
	}
	testMetric2Proto = metricpb.TimedMetric{
		Type:      metricpb.MetricType_TIMER,
		Id:        []byte("testMetric2"),
		TimeNanos: 67890,
		Value:     21.99,
	}
	testForwardMetadata1Proto = metricpb.ForwardMetadata{
		AggregationId: aggregationpb.AggregationID{Id: 0},
		StoragePolicy: policypb.StoragePolicy{
			Resolution: &policypb.Resolution{
				WindowSize: time.Minute.Nanoseconds(),
				Precision:  time.Minute.Nanoseconds(),
			},
			Retention: &policypb.Retention{
				Period: (12 * time.Hour).Nanoseconds(),
			},
		},
		Pipeline: pipelinepb.AppliedPipeline{
			Ops: []pipelinepb.AppliedPipelineOp{
				{
					Type: pipelinepb.AppliedPipelineOp_ROLLUP,
					Rollup: &pipelinepb.AppliedRollupOp{
						Id:            []byte("foo"),
						AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.Count)[0]},
					},
				},
			},
		},
		SourceId:          []byte("testForward1"),
		NumForwardedTimes: 3,
	}
	testForwardMetadata2Proto = metricpb.ForwardMetadata{
		AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.Sum)[0]},
		StoragePolicy: policypb.StoragePolicy{
			Resolution: &policypb.Resolution{
				WindowSize: 10 * time.Second.Nanoseconds(),
				Precision:  time.Second.Nanoseconds(),
			},
			Retention: &policypb.Retention{
				Period: (6 * time.Hour).Nanoseconds(),
			},
		},
		Pipeline: pipelinepb.AppliedPipeline{
			Ops: []pipelinepb.AppliedPipelineOp{
				{
					Type: pipelinepb.AppliedPipelineOp_TRANSFORMATION,
					Transformation: &pipelinepb.TransformationOp{
						Type: transformationpb.TransformationType_ABSOLUTE,
					},
				},
				{
					Type: pipelinepb.AppliedPipelineOp_ROLLUP,
					Rollup: &pipelinepb.AppliedRollupOp{
						Id:            []byte("bar"),
						AggregationId: aggregationpb.AggregationID{Id: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum)[0]},
					},
				},
			},
		},
		SourceId:          []byte("testForward2"),
		NumForwardedTimes: 2,
	}
	testBadMetricProto = metricpb.TimedMetric{
		Type: metricpb.MetricType_UNKNOWN,
	}
)

func TestMetricToProto(t *testing.T) {
	var pb metricpb.TimedMetric
	inputs := []Metric{testMetric1, testMetric2}
	expected := []metricpb.TimedMetric{testMetric1Proto, testMetric2Proto}
	for i := 0; i < len(inputs); i++ {
		require.NoError(t, inputs[i].ToProto(&pb))
		require.Equal(t, expected[i], pb)
	}
}

func TestMetricToProtoBadMetric(t *testing.T) {
	var pb metricpb.TimedMetric
	require.Error(t, testBadMetric.ToProto(&pb))
}

func TestMetricFromProto(t *testing.T) {
	var m Metric
	inputs := []metricpb.TimedMetric{testMetric1Proto, testMetric2Proto}
	expected := []Metric{testMetric1, testMetric2}
	for i := 0; i < len(inputs); i++ {
		require.NoError(t, m.FromProto(inputs[i]))
		require.Equal(t, expected[i], m)
	}
}

func TestMetricFromProtoBadMetricProto(t *testing.T) {
	var m Metric
	require.Error(t, m.FromProto(testBadMetricProto))
}

func TestCounterRoundTrip(t *testing.T) {
	var (
		pb  metricpb.TimedMetric
		res Metric
	)
	inputs := []Metric{testMetric1, testMetric2}
	for i := 0; i < len(inputs); i++ {
		require.NoError(t, inputs[i].ToProto(&pb))
		require.NoError(t, res.FromProto(pb))
		require.Equal(t, inputs[i], res)
	}
}

func TestMetricWithForwardMetadataToProto(t *testing.T) {
	inputs := []struct {
		metric   Metric
		metadata metadata.ForwardMetadata
		expected metricpb.TimedMetricWithForwardMetadata
	}{
		{
			metric:   testMetric1,
			metadata: testForwardMetadata1,
			expected: metricpb.TimedMetricWithForwardMetadata{
				Metric:   testMetric1Proto,
				Metadata: testForwardMetadata1Proto,
			},
		},
		{
			metric:   testMetric1,
			metadata: testForwardMetadata2,
			expected: metricpb.TimedMetricWithForwardMetadata{
				Metric:   testMetric1Proto,
				Metadata: testForwardMetadata2Proto,
			},
		},
		{
			metric:   testMetric2,
			metadata: testForwardMetadata1,
			expected: metricpb.TimedMetricWithForwardMetadata{
				Metric:   testMetric2Proto,
				Metadata: testForwardMetadata1Proto,
			},
		},
		{
			metric:   testMetric2,
			metadata: testForwardMetadata2,
			expected: metricpb.TimedMetricWithForwardMetadata{
				Metric:   testMetric2Proto,
				Metadata: testForwardMetadata2Proto,
			},
		},
	}

	var pb metricpb.TimedMetricWithForwardMetadata
	for _, input := range inputs {
		tm := MetricWithForwardMetadata{
			Metric:          input.metric,
			ForwardMetadata: input.metadata,
		}
		require.NoError(t, tm.ToProto(&pb))
	}
}

func TestMetricWithForwardMetadataToProtoBadMetric(t *testing.T) {
	var pb metricpb.TimedMetricWithForwardMetadata
	tm := MetricWithForwardMetadata{
		Metric:          testBadMetric,
		ForwardMetadata: testForwardMetadata1,
	}
	require.Error(t, tm.ToProto(&pb))
}

func TestMetricWithForwardMetadataToProtoBadMetadata(t *testing.T) {
	var pb metricpb.TimedMetricWithForwardMetadata
	tm := MetricWithForwardMetadata{
		Metric:          testMetric1,
		ForwardMetadata: testBadForwardMetadata,
	}
	require.Error(t, tm.ToProto(&pb))
}

func TestMetricWithForwardMetadataFromProto(t *testing.T) {
	inputs := []struct {
		data             metricpb.TimedMetricWithForwardMetadata
		expectedMetric   Metric
		expectedMetadata metadata.ForwardMetadata
	}{
		{
			data: metricpb.TimedMetricWithForwardMetadata{
				Metric:   testMetric1Proto,
				Metadata: testForwardMetadata1Proto,
			},
			expectedMetric:   testMetric1,
			expectedMetadata: testForwardMetadata1,
		},
		{
			data: metricpb.TimedMetricWithForwardMetadata{
				Metric:   testMetric1Proto,
				Metadata: testForwardMetadata2Proto,
			},
			expectedMetric:   testMetric1,
			expectedMetadata: testForwardMetadata2,
		},
		{
			data: metricpb.TimedMetricWithForwardMetadata{
				Metric:   testMetric2Proto,
				Metadata: testForwardMetadata1Proto,
			},
			expectedMetric:   testMetric2,
			expectedMetadata: testForwardMetadata1,
		},
		{
			data: metricpb.TimedMetricWithForwardMetadata{
				Metric:   testMetric2Proto,
				Metadata: testForwardMetadata2Proto,
			},
			expectedMetric:   testMetric2,
			expectedMetadata: testForwardMetadata2,
		},
	}

	var res MetricWithForwardMetadata
	for _, input := range inputs {
		require.NoError(t, res.FromProto(&input.data))
		expected := MetricWithForwardMetadata{
			Metric:          input.expectedMetric,
			ForwardMetadata: input.expectedMetadata,
		}
		require.Equal(t, expected, res)
	}
}

func TestMetricWithForwardMetadataFromProtoNilProto(t *testing.T) {
	var res MetricWithForwardMetadata
	require.Equal(t, errNilTimedMetricWithForwardMetadataProto, res.FromProto(nil))
}

func TestMetricWithForwardMetadataFromProtoBadMetricProto(t *testing.T) {
	var res MetricWithForwardMetadata
	pb := metricpb.TimedMetricWithForwardMetadata{
		Metric:   testBadMetricProto,
		Metadata: testForwardMetadata1Proto,
	}
	require.Error(t, res.FromProto(&pb))
}

func TestMetricWithForwardMetadataRoundtrip(t *testing.T) {
	inputs := []struct {
		metric   Metric
		metadata metadata.ForwardMetadata
	}{
		{
			metric:   testMetric1,
			metadata: testForwardMetadata1,
		},
		{
			metric:   testMetric1,
			metadata: testForwardMetadata2,
		},
		{
			metric:   testMetric2,
			metadata: testForwardMetadata1,
		},
		{
			metric:   testMetric2,
			metadata: testForwardMetadata2,
		},
	}

	var (
		res MetricWithForwardMetadata
		pb  metricpb.TimedMetricWithForwardMetadata
	)
	for _, input := range inputs {
		data := MetricWithForwardMetadata{
			Metric:          input.metric,
			ForwardMetadata: input.metadata,
		}
		require.NoError(t, data.ToProto(&pb))
		require.NoError(t, res.FromProto(&pb))
		require.Equal(t, data, res)
	}
}
