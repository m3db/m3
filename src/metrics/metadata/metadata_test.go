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
	"testing"
	"time"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3metrics/generated/proto/metricpb"
	"github.com/m3db/m3metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3metrics/generated/proto/policypb"
	"github.com/m3db/m3metrics/generated/proto/transformationpb"
	"github.com/m3db/m3metrics/op"
	"github.com/m3db/m3metrics/op/applied"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/transformation"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testSmallPipelineMetadata = PipelineMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicies: []policy.StoragePolicy{
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 6*time.Hour),
		},
		Pipeline: applied.NewPipeline([]applied.Union{
			{
				Type: op.TransformationType,
				Transformation: op.Transformation{
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
		Pipeline: applied.NewPipeline([]applied.Union{
			{
				Type: op.TransformationType,
				Transformation: op.Transformation{
					Type: transformation.Absolute,
				},
			},
			{
				Type: op.RollupType,
				Rollup: applied.Rollup{
					ID:            []byte("foo"),
					AggregationID: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum),
				},
			},
		}),
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
						Pipeline: applied.NewPipeline([]applied.Union{
							{
								Type: op.RollupType,
								Rollup: applied.Rollup{
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
						Pipeline: applied.NewPipeline([]applied.Union{
							{
								Type: op.TransformationType,
								Transformation: op.Transformation{
									Type: transformation.Absolute,
								},
							},
							{
								Type: op.RollupType,
								Rollup: applied.Rollup{
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
						Pipeline: applied.NewPipeline([]applied.Union{
							{
								Type: op.TransformationType,
								Transformation: op.Transformation{
									Type: transformation.PerSecond,
								},
							},
							{
								Type: op.RollupType,
								Rollup: applied.Rollup{
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
	testSmallPipelineMetadataProto = metricpb.PipelineMetadata{
		AggregationId: aggregationpb.AggregationID{Id: 0},
		StoragePolicies: []policypb.StoragePolicy{
			{
				Resolution: &policypb.Resolution{
					WindowSize: time.Minute.Nanoseconds(),
					Precision:  time.Minute.Nanoseconds(),
				},
				Retention: &policypb.Retention{
					Period: (6 * time.Hour).Nanoseconds(),
				},
			},
		},
		Pipeline: pipelinepb.AppliedPipeline{
			Ops: []pipelinepb.AppliedPipelineOp{
				{
					Type: pipelinepb.AppliedPipelineOp_TRANSFORMATION,
					Transformation: &pipelinepb.TransformationOp{
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
				Resolution: &policypb.Resolution{
					WindowSize: time.Minute.Nanoseconds(),
					Precision:  time.Minute.Nanoseconds(),
				},
				Retention: &policypb.Retention{
					Period: (12 * time.Hour).Nanoseconds(),
				},
			},
			{
				Resolution: &policypb.Resolution{
					WindowSize: time.Hour.Nanoseconds(),
					Precision:  time.Hour.Nanoseconds(),
				},
				Retention: &policypb.Retention{
					Period: (30 * 24 * time.Hour).Nanoseconds(),
				},
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
									Resolution: &policypb.Resolution{
										WindowSize: time.Second.Nanoseconds(),
										Precision:  time.Second.Nanoseconds(),
									},
									Retention: &policypb.Retention{
										Period: time.Hour.Nanoseconds(),
									},
								},
							},
						},
						{
							AggregationId: aggregationpb.AggregationID{Id: 0},
							StoragePolicies: []policypb.StoragePolicy{
								{
									Resolution: &policypb.Resolution{
										WindowSize: 10 * time.Second.Nanoseconds(),
										Precision:  time.Second.Nanoseconds(),
									},
									Retention: &policypb.Retention{
										Period: time.Hour.Nanoseconds(),
									},
								},
							},
							Pipeline: pipelinepb.AppliedPipeline{
								Ops: []pipelinepb.AppliedPipelineOp{
									{
										Type: pipelinepb.AppliedPipelineOp_ROLLUP,
										Rollup: &pipelinepb.AppliedRollupOp{
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
									Resolution: &policypb.Resolution{
										WindowSize: time.Second.Nanoseconds(),
										Precision:  time.Second.Nanoseconds(),
									},
									Retention: &policypb.Retention{
										Period: time.Hour.Nanoseconds(),
									},
								},
							},
						},
						{
							AggregationId: aggregationpb.AggregationID{Id: 0},
							StoragePolicies: []policypb.StoragePolicy{
								{
									Resolution: &policypb.Resolution{
										WindowSize: time.Minute.Nanoseconds(),
										Precision:  time.Minute.Nanoseconds(),
									},
									Retention: &policypb.Retention{
										Period: (6 * time.Hour).Nanoseconds(),
									},
								},
								{
									Resolution: &policypb.Resolution{
										WindowSize: time.Hour.Nanoseconds(),
										Precision:  time.Hour.Nanoseconds(),
									},
									Retention: &policypb.Retention{
										Period: (30 * 24 * time.Hour).Nanoseconds(),
									},
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
										Transformation: &pipelinepb.TransformationOp{
											Type: transformationpb.TransformationType_PERSECOND,
										},
									},
									{
										Type: pipelinepb.AppliedPipelineOp_ROLLUP,
										Rollup: &pipelinepb.AppliedRollupOp{
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
								Pipeline: applied.NewPipeline([]applied.Union{
									{
										Type:           op.TransformationType,
										Transformation: op.Transformation{Type: transformation.Absolute},
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
								Pipeline: applied.NewPipeline([]applied.Union{
									{
										Type:   op.RollupType,
										Rollup: applied.Rollup{ID: []byte("foo")},
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
								Pipeline: applied.NewPipeline([]applied.Union{
									{
										Type:   op.RollupType,
										Rollup: applied.Rollup{AggregationID: aggregation.MustCompressTypes(aggregation.Sum)},
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
		require.Equal(t, input.expected, input.metadatas.IsDefault())
	}
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
		var res StagedMetadatas
		for i, pb := range input.sequence {
			require.NoError(t, res.FromProto(pb))
			require.Equal(t, input.expected[i], res)
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
