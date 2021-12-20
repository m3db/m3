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

package metadata

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/generated/proto/transformationpb"
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
	testSmallForwardMetadata = ForwardMetadata{
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
	testLargeForwardMetadata = ForwardMetadata{
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
	testSmallPipelineMetadata = PipelineMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicies: []policy.StoragePolicy{
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 6*time.Hour),
		},
		Pipeline: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{
					Type: transformation.PerSecond,
				},
			},
		}),
	}
	testLargePipelineMetadata = PipelineMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicies: []policy.StoragePolicy{
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
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
	}
	testBadForwardMetadata = ForwardMetadata{
		StoragePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Unit(101), 6*time.Hour),
	}
	testBadPipelineMetadata = PipelineMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicies: []policy.StoragePolicy{
			policy.NewStoragePolicy(time.Minute, xtime.Unit(100), 6*time.Hour),
		},
	}
	testSmallStagedMetadatas = StagedMetadatas{
		{
			CutoverNanos: 4567,
			Tombstoned:   true,
			Metadata: Metadata{
				Pipelines: []PipelineMetadata{
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
	testLargeStagedMetadatas = StagedMetadatas{
		{
			CutoverNanos: 1234,
			Tombstoned:   false,
		},
		{
			CutoverNanos: 4567,
			Tombstoned:   true,
			Metadata: Metadata{
				Pipelines: []PipelineMetadata{
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
			Metadata: Metadata{
				Pipelines: []PipelineMetadata{
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
	testBadStagedMetadatas = StagedMetadatas{
		{
			Metadata: Metadata{
				Pipelines: []PipelineMetadata{
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
	testSmallForwardMetadataProto = metricpb.ForwardMetadata{
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
	testLargeForwardMetadataProto = metricpb.ForwardMetadata{
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
	testBadForwardMetadataProto    = metricpb.ForwardMetadata{}
	testSmallPipelineMetadataProto = metricpb.PipelineMetadata{
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
		},
		Pipeline: pipelinepb.AppliedPipeline{
			Ops: []pipelinepb.AppliedPipelineOp{
				{
					Type: pipelinepb.AppliedPipelineOp_TRANSFORMATION,
					Transformation: pipelinepb.TransformationOp{
						Type: transformationpb.TransformationType_PERSECOND,
					},
				},
			},
		},
	}
	testLargePipelineMetadataProto = metricpb.PipelineMetadata{
		AggregationId: aggregationpb.AggregationID{Id: 0},
		StoragePolicies: []policypb.StoragePolicy{
			{
				Resolution: policypb.Resolution{
					WindowSize: time.Minute.Nanoseconds(),
					Precision:  time.Minute.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: (12 * time.Hour).Nanoseconds(),
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
	}
	testBadPipelineMetadataProto = metricpb.PipelineMetadata{
		StoragePolicies: []policypb.StoragePolicy{
			{},
		},
	}
	testSmallStagedMetadatasProto = metricpb.StagedMetadatas{
		Metadatas: []metricpb.StagedMetadata{
			{
				CutoverNanos: 4567,
				Tombstoned:   true,
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
	testSmallStagedMetadatasWithLargeStoragePoliciesProto = metricpb.StagedMetadatas{
		Metadatas: []metricpb.StagedMetadata{
			{
				CutoverNanos: 4567,
				Tombstoned:   true,
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
										Period: 10 * time.Second.Nanoseconds(),
									},
								},
								{
									Resolution: policypb.Resolution{
										WindowSize: 10 * time.Second.Nanoseconds(),
										Precision:  time.Second.Nanoseconds(),
									},
									Retention: policypb.Retention{
										Period: time.Hour.Nanoseconds(),
									},
								},
								{
									Resolution: policypb.Resolution{
										WindowSize: 10 * time.Minute.Nanoseconds(),
										Precision:  time.Second.Nanoseconds(),
									},
									Retention: policypb.Retention{
										Period: time.Minute.Nanoseconds(),
									},
								},
								{
									Resolution: policypb.Resolution{
										WindowSize: 10 * time.Minute.Nanoseconds(),
										Precision:  time.Second.Nanoseconds(),
									},
									Retention: policypb.Retention{
										Period: time.Second.Nanoseconds(),
									},
								},
								{
									Resolution: policypb.Resolution{
										WindowSize: 10 * time.Hour.Nanoseconds(),
										Precision:  time.Second.Nanoseconds(),
									},
									Retention: policypb.Retention{
										Period: time.Second.Nanoseconds(),
									},
								},
							},
						},
					},
				},
			},
		},
	}
	testLargeStagedMetadatasProto = metricpb.StagedMetadatas{
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
	testBadStagedMetadatasProto = metricpb.StagedMetadatas{
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
	testDropMustDropPolicyPipelineMetadata = PipelineMetadata{
		AggregationID:   aggregation.DefaultID,
		StoragePolicies: []policy.StoragePolicy{},
		Pipeline:        applied.DefaultPipeline,
		DropPolicy:      policy.DropMust,
	}
	testDropExceptIfMatchOtherDropPolicyPipelineMetadata = PipelineMetadata{
		AggregationID:   aggregation.DefaultID,
		StoragePolicies: []policy.StoragePolicy{},
		Pipeline:        applied.DefaultPipeline,
		DropPolicy:      policy.DropMust,
	}
)

func TestStagedMetadatasIsDefault(t *testing.T) {
	inputs := []struct {
		metadatas StagedMetadatas
		expected  bool
	}{
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipelines: []PipelineMetadata{
							{},
						},
					},
				},
			},
			expected: true,
		},
		{
			metadatas: DefaultStagedMetadatas,
			expected:  true,
		},
		{
			metadatas: StagedMetadatas{},
			expected:  false,
		},
		{
			metadatas: StagedMetadatas{
				{
					CutoverNanos: 1234,
					Metadata: Metadata{
						Pipelines: []PipelineMetadata{
							{},
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Tombstoned: true,
					Metadata: Metadata{
						Pipelines: []PipelineMetadata{
							{},
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipelines: []PipelineMetadata{
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipelines: []PipelineMetadata{
							{
								StoragePolicies: []policy.StoragePolicy{
									policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipelines: []PipelineMetadata{
							{
								Pipeline: applied.NewPipeline([]applied.OpUnion{
									{
										Type:           pipeline.TransformationOpType,
										Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
									},
								}),
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipelines: []PipelineMetadata{
							{
								Pipeline: applied.NewPipeline([]applied.OpUnion{
									{
										Type:   pipeline.RollupOpType,
										Rollup: applied.RollupOp{ID: []byte("foo")},
									},
								}),
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipelines: []PipelineMetadata{
							{
								Pipeline: applied.NewPipeline([]applied.OpUnion{
									{
										Type:   pipeline.RollupOpType,
										Rollup: applied.RollupOp{AggregationID: aggregation.MustCompressTypes(aggregation.Sum)},
									},
								}),
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			metadatas: StagedMetadatas{
				{
					Metadata: Metadata{
						Pipelines: []PipelineMetadata{
							{},
						},
					},
				},
				{
					Metadata: Metadata{
						Pipelines: []PipelineMetadata{
							{},
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, input := range inputs {
		input := input
		t.Run(fmt.Sprintf("%v", input.metadatas), func(t *testing.T) {
			require.Equal(t, input.expected, input.metadatas.IsDefault())
		})
	}
}

func TestForwardMetadataToProto(t *testing.T) {
	inputs := []struct {
		sequence []ForwardMetadata
		expected []metricpb.ForwardMetadata
	}{
		{
			sequence: []ForwardMetadata{
				testSmallForwardMetadata,
				testLargeForwardMetadata,
			},
			expected: []metricpb.ForwardMetadata{
				testSmallForwardMetadataProto,
				testLargeForwardMetadataProto,
			},
		},
		{
			sequence: []ForwardMetadata{
				testLargeForwardMetadata,
				testSmallForwardMetadata,
			},
			expected: []metricpb.ForwardMetadata{
				testLargeForwardMetadataProto,
				testSmallForwardMetadataProto,
			},
		},
	}

	for _, input := range inputs {
		var pb metricpb.ForwardMetadata
		for i, meta := range input.sequence {
			require.NoError(t, meta.ToProto(&pb))
			require.Equal(t, input.expected[i], pb)
		}
	}
}

func TestForwardMetadataFromProto(t *testing.T) {
	inputs := []struct {
		sequence []metricpb.ForwardMetadata
		expected []ForwardMetadata
	}{
		{
			sequence: []metricpb.ForwardMetadata{
				testSmallForwardMetadataProto,
				testLargeForwardMetadataProto,
			},
			expected: []ForwardMetadata{
				testSmallForwardMetadata,
				testLargeForwardMetadata,
			},
		},
		{
			sequence: []metricpb.ForwardMetadata{
				testLargeForwardMetadataProto,
				testSmallForwardMetadataProto,
			},
			expected: []ForwardMetadata{
				testLargeForwardMetadata,
				testSmallForwardMetadata,
			},
		},
	}

	for _, input := range inputs {
		var res ForwardMetadata
		for i, pb := range input.sequence {
			require.NoError(t, res.FromProto(pb))
			require.Equal(t, input.expected[i], res)
		}
	}
}

func TestForwardMetadataRoundtrip(t *testing.T) {
	inputs := [][]ForwardMetadata{
		{
			testSmallForwardMetadata,
			testLargeForwardMetadata,
		},
		{
			testLargeForwardMetadata,
			testSmallForwardMetadata,
		},
	}

	for _, input := range inputs {
		var (
			pb  metricpb.ForwardMetadata
			res ForwardMetadata
		)
		for _, metadata := range input {
			require.NoError(t, metadata.ToProto(&pb))
			require.NoError(t, res.FromProto(pb))
			require.Equal(t, metadata, res)
		}
	}
}

func TestForwardMetadataToProtoBadMetadata(t *testing.T) {
	var pb metricpb.ForwardMetadata
	require.Error(t, testBadForwardMetadata.ToProto(&pb))
}

func TestForwardMetadataFromProtoBadMetadataProto(t *testing.T) {
	var res ForwardMetadata
	require.Error(t, res.FromProto(testBadForwardMetadataProto))
}

func TestPipelineMetadataClone(t *testing.T) {
	cloned1 := testLargePipelineMetadata.Clone()
	cloned2 := testLargePipelineMetadata.Clone()
	require.True(t, cloned1.Equal(testLargePipelineMetadata))
	require.True(t, cloned2.Equal(testLargePipelineMetadata))

	// Assert that modifying the clone does not mutate the original pipeline metadata.
	cloned1.StoragePolicies[0] = policy.MustParseStoragePolicy("1h:1h")
	require.False(t, cloned1.Equal(testLargePipelineMetadata))
	require.True(t, cloned2.Equal(testLargePipelineMetadata))
}

func TestPipelineMetadataToProto(t *testing.T) {
	inputs := []struct {
		sequence []PipelineMetadata
		expected []metricpb.PipelineMetadata
	}{
		{
			sequence: []PipelineMetadata{
				testSmallPipelineMetadata,
				testLargePipelineMetadata,
			},
			expected: []metricpb.PipelineMetadata{
				testSmallPipelineMetadataProto,
				testLargePipelineMetadataProto,
			},
		},
		{
			sequence: []PipelineMetadata{
				testLargePipelineMetadata,
				testSmallPipelineMetadata,
			},
			expected: []metricpb.PipelineMetadata{
				testLargePipelineMetadataProto,
				testSmallPipelineMetadataProto,
			},
		},
	}

	for _, input := range inputs {
		var pb metricpb.PipelineMetadata
		for i, meta := range input.sequence {
			require.NoError(t, meta.ToProto(&pb))
			require.Equal(t, input.expected[i], pb)
		}
	}
}

func TestPipelineMetadataFromProto(t *testing.T) {
	inputs := []struct {
		sequence []metricpb.PipelineMetadata
		expected []PipelineMetadata
	}{
		{
			sequence: []metricpb.PipelineMetadata{
				testSmallPipelineMetadataProto,
				testLargePipelineMetadataProto,
			},
			expected: []PipelineMetadata{
				testSmallPipelineMetadata,
				testLargePipelineMetadata,
			},
		},
		{
			sequence: []metricpb.PipelineMetadata{
				testLargePipelineMetadataProto,
				testSmallPipelineMetadataProto,
			},
			expected: []PipelineMetadata{
				testLargePipelineMetadata,
				testSmallPipelineMetadata,
			},
		},
	}

	for _, input := range inputs {
		var res PipelineMetadata
		for i, pb := range input.sequence {
			require.NoError(t, res.FromProto(pb))
			require.Equal(t, input.expected[i], res)
		}
	}
}

func TestPipelineMetadataRoundTrip(t *testing.T) {
	inputs := [][]PipelineMetadata{
		{
			testSmallPipelineMetadata,
			testLargePipelineMetadata,
		},
		{
			testLargePipelineMetadata,
			testSmallPipelineMetadata,
		},
	}

	for _, input := range inputs {
		var (
			pb  metricpb.PipelineMetadata
			res PipelineMetadata
		)
		for _, metadata := range input {
			require.NoError(t, metadata.ToProto(&pb))
			require.NoError(t, res.FromProto(pb))
			require.Equal(t, metadata, res)
		}
	}
}

func TestPipelineMetadataToProtoBadMetadata(t *testing.T) {
	var pb metricpb.PipelineMetadata
	require.Error(t, testBadPipelineMetadata.ToProto(&pb))
}

func TestPipelineMetadataFromProtoBadMetadataProto(t *testing.T) {
	var res PipelineMetadata
	require.Error(t, res.FromProto(testBadPipelineMetadataProto))
}

func TestPipelineMetadatasClone(t *testing.T) {
	input := PipelineMetadatas{
		testSmallPipelineMetadata,
		testLargePipelineMetadata,
	}
	cloned1 := input.Clone()
	cloned2 := input.Clone()
	require.True(t, cloned1.Equal(input))
	require.True(t, cloned2.Equal(input))

	// Assert that modifying the clone does not mutate the original pipeline metadata.
	cloned1[0].StoragePolicies[0] = policy.MustParseStoragePolicy("1h:1h")
	require.False(t, cloned1.Equal(input))
	require.True(t, cloned2.Equal(input))
}

func TestStagedMetadatasToProto(t *testing.T) {
	inputs := []struct {
		sequence []StagedMetadatas
		expected []metricpb.StagedMetadatas
	}{
		{
			sequence: []StagedMetadatas{
				testSmallStagedMetadatas,
				testLargeStagedMetadatas,
			},
			expected: []metricpb.StagedMetadatas{
				testSmallStagedMetadatasProto,
				testLargeStagedMetadatasProto,
			},
		},
		{
			sequence: []StagedMetadatas{
				testLargeStagedMetadatas,
				testSmallStagedMetadatas,
			},
			expected: []metricpb.StagedMetadatas{
				testLargeStagedMetadatasProto,
				testSmallStagedMetadatasProto,
			},
		},
	}

	for _, input := range inputs {
		var pb metricpb.StagedMetadatas
		for i, meta := range input.sequence {
			require.NoError(t, meta.ToProto(&pb))
			require.Equal(t, input.expected[i], pb)
		}
	}
}

func TestStagedMetadatasFromProto(t *testing.T) {
	inputs := []struct {
		sequence []metricpb.StagedMetadatas
		expected []StagedMetadatas
	}{
		{
			sequence: []metricpb.StagedMetadatas{
				testSmallStagedMetadatasProto,
				testLargeStagedMetadatasProto,
			},
			expected: []StagedMetadatas{
				testSmallStagedMetadatas,
				testLargeStagedMetadatas,
			},
		},
		{
			sequence: []metricpb.StagedMetadatas{
				testLargeStagedMetadatasProto,
				testSmallStagedMetadatasProto,
			},
			expected: []StagedMetadatas{
				testLargeStagedMetadatas,
				testSmallStagedMetadatas,
			},
		},
	}

	for _, input := range inputs {
		var resOpt, resReference StagedMetadatas
		for i, pb := range input.sequence {
			require.NoError(t, resReference.fromProto(pb))
			require.NoError(t, resOpt.FromProto(pb))
			require.Equal(t, input.expected[i], resOpt)
			require.Equal(t, input.expected[i], resReference)
			require.Equal(t, resOpt, resReference)
		}
	}
}

func TestStagedMetadatasRoundTrip(t *testing.T) {
	inputs := [][]StagedMetadatas{
		{
			testSmallStagedMetadatas,
			testLargeStagedMetadatas,
		},
		{
			testLargeStagedMetadatas,
			testSmallStagedMetadatas,
		},
	}

	for _, input := range inputs {
		var (
			pb  metricpb.StagedMetadatas
			res StagedMetadatas
		)
		for _, metadata := range input {
			require.NoError(t, metadata.ToProto(&pb))
			require.NoError(t, res.FromProto(pb))
			require.Equal(t, metadata, res)
		}
	}
}

func TestStagedMetadatasToProtoBadMetadatas(t *testing.T) {
	var pb metricpb.StagedMetadatas
	require.Error(t, testBadStagedMetadatas.ToProto(&pb))
}

func TestStagedMetadatasFromProtoBadMetadatasProto(t *testing.T) {
	var res StagedMetadatas
	require.Error(t, res.FromProto(testBadStagedMetadatasProto))
}

func TestVersionedStagedMetadatasMarshalJSON(t *testing.T) {
	vs := VersionedStagedMetadatas{
		Version: 12,
		StagedMetadatas: StagedMetadatas{
			{
				CutoverNanos: 4567,
				Tombstoned:   true,
				Metadata: Metadata{
					Pipelines: []PipelineMetadata{
						{
							ResendEnabled: true,
							AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
							StoragePolicies: []policy.StoragePolicy{
								policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
								policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
							},
						},
						{
							AggregationID: aggregation.DefaultID,
							StoragePolicies: []policy.StoragePolicy{
								policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
							},
						},
					},
				},
			},
		},
	}
	res, err := json.Marshal(vs)
	require.NoError(t, err)

	expected := `{` +
		`"stagedMetadatas":` +
		`[{"metadata":{"pipelines":[` +
		`{"storagePolicies":["1s:1h","1m:12h"],"aggregation":["Sum"],"resendEnabled":true},` +
		`{"storagePolicies":["10s:1h"],"aggregation":null}]},` +
		`"cutoverNanos":4567,` +
		`"tombstoned":true}],` +
		`"version":12` +
		`}`
	require.Equal(t, expected, string(res))
}

func TestVersionedStagedMetadatasMarshalJSONRoundtrip(t *testing.T) {
	vs := VersionedStagedMetadatas{
		Version: 12,
		StagedMetadatas: StagedMetadatas{
			{
				CutoverNanos: 4567,
				Tombstoned:   true,
				Metadata: Metadata{
					Pipelines: []PipelineMetadata{
						{
							AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
							StoragePolicies: []policy.StoragePolicy{
								policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
								policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
							},
							DropPolicy: policy.DropNone,
						},
						{
							AggregationID: aggregation.DefaultID,
							StoragePolicies: []policy.StoragePolicy{
								policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
							},
							DropPolicy: policy.DropNone,
						},
					},
				},
			},
		},
	}
	b, err := json.Marshal(vs)
	require.NoError(t, err)
	var res VersionedStagedMetadatas
	require.NoError(t, json.Unmarshal(b, &res))
	require.Equal(t, vs, res)
}

func TestDropMustDropPolicyPipelineMetadata(t *testing.T) {
	require.True(t, testDropMustDropPolicyPipelineMetadata.IsDropPolicyApplied())
}

func TestDropExceptIfMatchOtherDropPolicyPipelineMetadata(t *testing.T) {
	require.True(t, testDropExceptIfMatchOtherDropPolicyPipelineMetadata.IsDropPolicyApplied())
}

func TestApplyOrRemoveDropPoliciesDropMust(t *testing.T) {
	input := PipelineMetadatas{
		{
			AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
			StoragePolicies: []policy.StoragePolicy{
				policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
			},
			DropPolicy: policy.DropNone,
		},
		{
			AggregationID:   aggregation.DefaultID,
			StoragePolicies: nil,
			DropPolicy:      policy.DropMust,
		},
		{
			AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
			StoragePolicies: []policy.StoragePolicy{
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
				policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 24*time.Hour),
			},
			DropPolicy: policy.DropNone,
		},
	}
	output, result := input.ApplyOrRemoveDropPolicies()
	require.Equal(t, AppliedEffectiveDropPolicyResult, result)
	require.True(t, output.Equal(DropPipelineMetadatas))
}

func TestApplyOrRemoveDropPoliciesDropIfOnlyMatchEffective(t *testing.T) {
	input := PipelineMetadatas{
		{
			AggregationID:   aggregation.DefaultID,
			StoragePolicies: nil,
			DropPolicy:      policy.DropIfOnlyMatch,
		},
	}
	output, result := input.ApplyOrRemoveDropPolicies()
	require.Equal(t, AppliedEffectiveDropPolicyResult, result)
	require.True(t, output.Equal(DropPipelineMetadatas))

	// Ensure that modifying output does not affect DropPipelineMetadatas,
	// to prevent regressions where global variables are returned.
	output[0].AggregationID = aggregation.MustCompressTypes(aggregation.Count)
	require.False(t, output.Equal(DropPipelineMetadatas))
}

func TestApplyOrRemoveDropPoliciesDropIfOnlyMatchMiddleIneffective(t *testing.T) {
	validRules := PipelineMetadatas{
		{
			AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
			StoragePolicies: []policy.StoragePolicy{
				policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
			},
			DropPolicy: policy.DropNone,
		},
		{
			AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
			StoragePolicies: []policy.StoragePolicy{
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
				policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 24*time.Hour),
			},
			DropPolicy: policy.DropNone,
		},
	}

	// Run test for every single insertion point
	for i := 0; i < len(validRules)+1; i++ {
		t.Run(fmt.Sprintf("test insert drop if only rule at %d", i),
			func(t *testing.T) {
				var (
					copy  = append(PipelineMetadatas(nil), validRules...)
					input PipelineMetadatas
				)
				for j := 0; j < len(validRules)+1; j++ {
					if j == i {
						// Insert the drop if only match rule at this position
						input = append(input, PipelineMetadata{
							AggregationID:   aggregation.DefaultID,
							StoragePolicies: nil,
							DropPolicy:      policy.DropIfOnlyMatch,
						})
					} else {
						input = append(input, copy[0])
						copy = copy[1:]
					}
				}

				output, result := input.ApplyOrRemoveDropPolicies()
				require.Equal(t, RemovedIneffectiveDropPoliciesResult, result)
				require.True(t, output.Equal(validRules))
			})
	}
}

func TestApplyOrRemoveDropPoliciesDropIfOnlyMatchNone(t *testing.T) {
	input := PipelineMetadatas{
		{
			AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
			StoragePolicies: []policy.StoragePolicy{
				policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
			},
			DropPolicy: policy.DropNone,
		},
		{
			AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
			StoragePolicies: []policy.StoragePolicy{
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
				policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 24*time.Hour),
			},
			DropPolicy: policy.DropNone,
		},
	}
	output, result := input.ApplyOrRemoveDropPolicies()
	require.Equal(t, NoDropPolicyPresentResult, result)
	require.True(t, output.Equal(input))
}

func TestStagedMetadatasDropReturnsIsDropPolicyAppliedTrue(t *testing.T) {
	require.True(t, StagedMetadatas{
		StagedMetadata{Metadata: DropMetadata, CutoverNanos: 123},
	}.IsDropPolicyApplied())
}

func TestStagedMetadataClone(t *testing.T) {
	mdClone := testLargeStagedMetadatas[0].Clone()
	require.True(t, cmp.Equal(testLargeStagedMetadatas[0], mdClone, cmpopts.EquateEmpty()))

	mdsClone := testLargeStagedMetadatas.Clone()
	require.True(t, cmp.Equal(testLargeStagedMetadatas, mdsClone, cmpopts.EquateEmpty()))
}

func TestStagedMetadataReset(t *testing.T) {
	md := testLargeStagedMetadatas[0].Clone()
	md.Reset()

	require.Equal(t, StagedMetadata{
		Metadata: Metadata{
			Pipelines: []PipelineMetadata{},
		},
	}, md)
}
