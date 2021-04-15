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

package rules

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/generated/proto/transformationpb"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestNewRollupTargetV1ProtoNilProto(t *testing.T) {
	_, err := newRollupTargetFromV1Proto(nil)
	require.Equal(t, errNilRollupTargetV1Proto, err)
}

func TestNewRollupTargetV1ProtoInvalidProto(t *testing.T) {
	proto := &rulepb.RollupTarget{
		Policies: []*policypb.Policy{
			&policypb.Policy{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: policypb.Resolution{
						WindowSize: 10 * time.Second.Nanoseconds(),
						Precision:  time.Second.Nanoseconds(),
					},
					Retention: policypb.Retention{
						Period: 24 * time.Hour.Nanoseconds(),
					},
				},
				AggregationTypes: []aggregationpb.AggregationType{10, 1234567},
			},
		},
	}
	_, err := newRollupTargetFromV1Proto(proto)
	require.Error(t, err)
}

func TestNewRollupTargetV1ProtoWithDefaultAggregationID(t *testing.T) {
	proto := &rulepb.RollupTarget{
		Name: "testV1Proto",
		Tags: []string{"testTag2", "testTag1"},
		Policies: []*policypb.Policy{
			{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: policypb.Resolution{
						WindowSize: 10 * time.Second.Nanoseconds(),
						Precision:  time.Second.Nanoseconds(),
					},
					Retention: policypb.Retention{
						Period: 24 * time.Hour.Nanoseconds(),
					},
				},
			},
			{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: policypb.Resolution{
						WindowSize: time.Minute.Nanoseconds(),
						Precision:  time.Minute.Nanoseconds(),
					},
					Retention: policypb.Retention{
						Period: 720 * time.Hour.Nanoseconds(),
					},
				},
			},
			{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: policypb.Resolution{
						WindowSize: time.Hour.Nanoseconds(),
						Precision:  time.Hour.Nanoseconds(),
					},
					Retention: policypb.Retention{
						Period: 365 * 24 * time.Hour.Nanoseconds(),
					},
				},
			},
		},
	}
	res, err := newRollupTargetFromV1Proto(proto)
	require.NoError(t, err)

	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testV1Proto",
		[]string{"testTag1", "testTag2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	expected := rollupTarget{
		Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
			{
				Type:   pipeline.RollupOpType,
				Rollup: rr1,
			},
		}),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
			policy.NewStoragePolicy(time.Hour, xtime.Hour, 365*24*time.Hour),
		},
	}
	require.Equal(t, expected, res)
}

func TestNewRollupTargetV1ProtoWithCustomAggregationID(t *testing.T) {
	proto := &rulepb.RollupTarget{
		Name: "testV1Proto",
		Tags: []string{"testTag2", "testTag1"},
		Policies: []*policypb.Policy{
			{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: policypb.Resolution{
						WindowSize: 10 * time.Second.Nanoseconds(),
						Precision:  time.Second.Nanoseconds(),
					},
					Retention: policypb.Retention{
						Period: 24 * time.Hour.Nanoseconds(),
					},
				},
				AggregationTypes: []aggregationpb.AggregationType{1, 2},
			},
			{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: policypb.Resolution{
						WindowSize: time.Minute.Nanoseconds(),
						Precision:  time.Minute.Nanoseconds(),
					},
					Retention: policypb.Retention{
						Period: 720 * time.Hour.Nanoseconds(),
					},
				},
				AggregationTypes: []aggregationpb.AggregationType{1, 2},
			},
			{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: policypb.Resolution{
						WindowSize: time.Hour.Nanoseconds(),
						Precision:  time.Hour.Nanoseconds(),
					},
					Retention: policypb.Retention{
						Period: 365 * 24 * time.Hour.Nanoseconds(),
					},
				},
				AggregationTypes: []aggregationpb.AggregationType{1, 2},
			},
		},
	}
	res, err := newRollupTargetFromV1Proto(proto)
	require.NoError(t, err)

	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testV1Proto",
		[]string{"testTag1", "testTag2"},
		aggregation.MustCompressTypes(aggregation.Last, aggregation.Min),
	)
	require.NoError(t, err)

	expected := rollupTarget{
		Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
			{
				Type:   pipeline.RollupOpType,
				Rollup: rr1,
			},
		}),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
			policy.NewStoragePolicy(time.Hour, xtime.Hour, 365*24*time.Hour),
		},
	}
	require.Equal(t, expected, res)
}

func TestNewRollupTargetV2ProtoNilProto(t *testing.T) {
	_, err := newRollupTargetFromV2Proto(nil)
	require.Equal(t, err, errNilRollupTargetV2Proto)
}

func TestNewRollupTargetV2ProtoInvalidPipelineProto(t *testing.T) {
	proto := &rulepb.RollupTargetV2{
		Pipeline: &pipelinepb.Pipeline{
			Ops: []pipelinepb.PipelineOp{
				{
					Type: pipelinepb.PipelineOp_TRANSFORMATION,
					Transformation: &pipelinepb.TransformationOp{
						Type: transformationpb.TransformationType_UNKNOWN,
					},
				},
			},
		},
	}
	_, err := newRollupTargetFromV2Proto(proto)
	require.Error(t, err)
}

func TestNewRollupTargetV2ProtoInvalidStoragePoliciesProto(t *testing.T) {
	proto := &rulepb.RollupTargetV2{
		Pipeline: &pipelinepb.Pipeline{
			Ops: []pipelinepb.PipelineOp{
				{
					Type: pipelinepb.PipelineOp_TRANSFORMATION,
					Transformation: &pipelinepb.TransformationOp{
						Type: transformationpb.TransformationType_ABSOLUTE,
					},
				},
			},
		},
		StoragePolicies: []*policypb.StoragePolicy{
			&policypb.StoragePolicy{
				Resolution: policypb.Resolution{Precision: 1234},
				Retention:  policypb.Retention{Period: 5678},
			},
		},
	}
	_, err := newRollupTargetFromV2Proto(proto)
	require.Error(t, err)
}

func TestNewRollupTargetV2Proto(t *testing.T) {
	proto := &rulepb.RollupTargetV2{
		Pipeline: &pipelinepb.Pipeline{
			Ops: []pipelinepb.PipelineOp{
				{
					Type: pipelinepb.PipelineOp_AGGREGATION,
					Aggregation: &pipelinepb.AggregationOp{
						Type: aggregationpb.AggregationType_SUM,
					},
				},
				{
					Type: pipelinepb.PipelineOp_TRANSFORMATION,
					Transformation: &pipelinepb.TransformationOp{
						Type: transformationpb.TransformationType_ABSOLUTE,
					},
				},
				{
					Type: pipelinepb.PipelineOp_ROLLUP,
					Rollup: &pipelinepb.RollupOp{
						NewName: "testRollupOp",
						Tags:    []string{"testTag2", "testTag1"},
						AggregationTypes: []aggregationpb.AggregationType{
							aggregationpb.AggregationType_MIN,
							aggregationpb.AggregationType_MAX,
						},
					},
				},
			},
		},
		StoragePolicies: []*policypb.StoragePolicy{
			{
				Resolution: policypb.Resolution{
					WindowSize: 10 * time.Second.Nanoseconds(),
					Precision:  time.Second.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 24 * time.Hour.Nanoseconds(),
				},
			},
			{
				Resolution: policypb.Resolution{
					WindowSize: time.Minute.Nanoseconds(),
					Precision:  time.Minute.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 720 * time.Hour.Nanoseconds(),
				},
			},
			{
				Resolution: policypb.Resolution{
					WindowSize: time.Hour.Nanoseconds(),
					Precision:  time.Hour.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 365 * 24 * time.Hour.Nanoseconds(),
				},
			},
		},
	}
	res, err := newRollupTargetFromV2Proto(proto)
	require.NoError(t, err)

	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testRollupOp",
		[]string{"testTag1", "testTag2"},
		aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
	)
	require.NoError(t, err)

	expected := rollupTarget{
		Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
			{
				Type: pipeline.AggregationOpType,
				Aggregation: pipeline.AggregationOp{
					Type: aggregation.Sum,
				},
			},
			{
				Type: pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{
					Type: transformation.Absolute,
				},
			},
			{
				Type:   pipeline.RollupOpType,
				Rollup: rr1,
			},
		}),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
			policy.NewStoragePolicy(time.Hour, xtime.Hour, 365*24*time.Hour),
		},
	}
	require.Equal(t, expected, res)
}

func TestRollupTargetClone(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testRollupOp",
		[]string{"testTag1", "testTag2"},
		aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
	)
	require.NoError(t, err)

	source := rollupTarget{
		Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
			{
				Type: pipeline.AggregationOpType,
				Aggregation: pipeline.AggregationOp{
					Type: aggregation.Sum,
				},
			},
			{
				Type: pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{
					Type: transformation.Absolute,
				},
			},
			{
				Type:   pipeline.RollupOpType,
				Rollup: rr1,
			},
		}),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
			policy.NewStoragePolicy(time.Hour, xtime.Hour, 365*24*time.Hour),
		},
	}
	cloned := source.clone()
	require.Equal(t, source, cloned)

	// Assert that mutating the clone doesn't mutate the source.
	cloned2 := source.clone()
	cloned.StoragePolicies[0] = policy.NewStoragePolicy(time.Second, xtime.Second, 24*time.Hour)
	require.Equal(t, cloned2, source)
}

func TestRollupTargetProtoInvalidPipeline(t *testing.T) {
	target := rollupTarget{
		Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
			{
				Type: pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{
					Type: transformation.UnknownType,
				},
			},
		}),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
			policy.NewStoragePolicy(time.Hour, xtime.Hour, 365*24*time.Hour),
		},
	}
	_, err := target.proto()
	require.Error(t, err)
}

func TestRollupTargetProtoInvalidStoragePolicies(t *testing.T) {
	target := rollupTarget{
		Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
			{
				Type: pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{
					Type: transformation.Absolute,
				},
			},
		}),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Unit(100), 24*time.Hour),
		},
	}
	_, err := target.proto()
	require.Error(t, err)
}

func TestRollupTargetProto(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testRollupOp",
		[]string{"testTag1", "testTag2"},
		aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
	)
	require.NoError(t, err)

	target := rollupTarget{
		Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
			{
				Type: pipeline.AggregationOpType,
				Aggregation: pipeline.AggregationOp{
					Type: aggregation.Sum,
				},
			},
			{
				Type: pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{
					Type: transformation.Absolute,
				},
			},
			{
				Type:   pipeline.RollupOpType,
				Rollup: rr1,
			},
		}),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
			policy.NewStoragePolicy(time.Hour, xtime.Hour, 365*24*time.Hour),
		},
	}
	res, err := target.proto()
	require.NoError(t, err)

	expected := &rulepb.RollupTargetV2{
		Pipeline: &pipelinepb.Pipeline{
			Ops: []pipelinepb.PipelineOp{
				{
					Type: pipelinepb.PipelineOp_AGGREGATION,
					Aggregation: &pipelinepb.AggregationOp{
						Type: aggregationpb.AggregationType_SUM,
					},
				},
				{
					Type: pipelinepb.PipelineOp_TRANSFORMATION,
					Transformation: &pipelinepb.TransformationOp{
						Type: transformationpb.TransformationType_ABSOLUTE,
					},
				},
				{
					Type: pipelinepb.PipelineOp_ROLLUP,
					Rollup: &pipelinepb.RollupOp{
						NewName: "testRollupOp",
						Tags:    []string{"testTag1", "testTag2"},
						AggregationTypes: []aggregationpb.AggregationType{
							aggregationpb.AggregationType_MIN,
							aggregationpb.AggregationType_MAX,
						},
					},
				},
			},
		},
		StoragePolicies: []*policypb.StoragePolicy{
			{
				Resolution: policypb.Resolution{
					WindowSize: 10 * time.Second.Nanoseconds(),
					Precision:  time.Second.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 24 * time.Hour.Nanoseconds(),
				},
			},
			{
				Resolution: policypb.Resolution{
					WindowSize: time.Minute.Nanoseconds(),
					Precision:  time.Minute.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 720 * time.Hour.Nanoseconds(),
				},
			},
			{
				Resolution: policypb.Resolution{
					WindowSize: time.Hour.Nanoseconds(),
					Precision:  time.Hour.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 365 * 24 * time.Hour.Nanoseconds(),
				},
			},
		},
	}
	require.Equal(t, expected, res)
}
