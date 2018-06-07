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
	"github.com/m3db/m3metrics/op"
	"github.com/m3db/m3metrics/op/applied"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/transformation"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
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
								Pipeline: applied.Pipeline{
									Operations: []applied.Union{
										{
											Type:           op.TransformationType,
											Transformation: op.Transformation{Type: transformation.Absolute},
										},
									},
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
								Pipeline: applied.Pipeline{
									Operations: []applied.Union{
										{
											Type:   op.RollupType,
											Rollup: applied.Rollup{ID: []byte("foo")},
										},
									},
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
								Pipeline: applied.Pipeline{
									Operations: []applied.Union{
										{
											Type:   op.RollupType,
											Rollup: applied.Rollup{AggregationID: aggregation.MustCompressTypes(aggregation.Sum)},
										},
									},
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
