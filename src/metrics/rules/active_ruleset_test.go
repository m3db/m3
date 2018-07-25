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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/pipeline"
	"github.com/m3db/m3metrics/pipeline/applied"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/transformation"
	xtime "github.com/m3db/m3x/time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

var (
	testStagedMetadatasCmptOpts = []cmp.Option{
		cmpopts.EquateEmpty(),
	}
	testIDWithMetadatasCmpOpts = []cmp.Option{
		cmpopts.EquateEmpty(),
	}
)

func TestActiveRuleSetCutoverTimesWithMappingRules(t *testing.T) {
	as := newActiveRuleSet(
		0,
		testMappingRules(t),
		nil,
		testTagsFilterOptions(),
		mockNewID,
		nil,
	)
	expectedCutovers := []int64{5000, 8000, 10000, 15000, 20000, 22000, 24000, 30000, 34000, 35000, 100000}
	require.Equal(t, expectedCutovers, as.cutoverTimesAsc)
}

func TestActiveRuleSetCutoverTimesWithRollupRules(t *testing.T) {
	as := newActiveRuleSet(
		0,
		nil,
		testRollupRules(t),
		testTagsFilterOptions(),
		mockNewID,
		nil,
	)
	expectedCutovers := []int64{10000, 15000, 20000, 22000, 24000, 30000, 34000, 35000, 38000, 90000, 100000, 120000}
	require.Equal(t, expectedCutovers, as.cutoverTimesAsc)
}

func TestActiveRuleSetCutoverTimesWithMappingRulesAndRollupRules(t *testing.T) {
	as := newActiveRuleSet(
		0,
		testMappingRules(t),
		testRollupRules(t),
		testTagsFilterOptions(),
		mockNewID,
		nil,
	)
	expectedCutovers := []int64{5000, 8000, 10000, 15000, 20000, 22000, 24000, 30000, 34000, 35000, 38000, 90000, 100000, 120000}
	require.Equal(t, expectedCutovers, as.cutoverTimesAsc)
}

func TestActiveRuleSetForwardMatchWithMappingRules(t *testing.T) {
	inputs := []testMatchInput{
		{
			id:            "mtagName1=mtagValue1",
			matchFrom:     25000,
			matchTo:       25001,
			expireAtNanos: 30000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 22000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:            "mtagName1=mtagValue1",
			matchFrom:     35000,
			matchTo:       35001,
			expireAtNanos: 100000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 35000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:            "mtagName1=mtagValue2",
			matchFrom:     25000,
			matchTo:       25001,
			expireAtNanos: 30000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:                  "mtagName1=mtagValue3",
			matchFrom:           25000,
			matchTo:             25001,
			expireAtNanos:       30000,
			forExistingIDResult: metadata.DefaultStagedMetadatas,
		},
		{
			id:            "mtagName1=mtagValue1",
			matchFrom:     10000,
			matchTo:       40000,
			expireAtNanos: 100000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 10000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Count),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 15000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Count),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Mean),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 20000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Mean),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 22000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 22000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 30000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 34000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 35000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:            "mtagName1=mtagValue3",
			matchFrom:     4000,
			matchTo:       9000,
			expireAtNanos: 10000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.DefaultStagedMetadata,
				metadata.StagedMetadata{
					CutoverNanos: 5000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 8000,
					Tombstoned:   false,
					Metadata:     metadata.DefaultMetadata,
				},
			},
		},
		{
			id:            "mtagName1=mtagValue2",
			matchFrom:     10000,
			matchTo:       40000,
			expireAtNanos: 100000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.DefaultStagedMetadata,
				metadata.StagedMetadata{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:            "shouldDropTagName1=shouldDropTagValue1",
			matchFrom:     25000,
			matchTo:       25001,
			expireAtNanos: 30000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 20000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: metadata.DropPipelineMetadatas,
					},
				},
			},
		},
		{
			id:            "shouldDrop2TagName1=shouldDrop2TagValue1",
			matchFrom:     25000,
			matchTo:       25001,
			expireAtNanos: 30000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 20000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: metadata.DropPipelineMetadatas,
					},
				},
			},
		},
		{
			id:            "shouldNotDropTagName1=shouldNotDropTagValue1",
			matchFrom:     25000,
			matchTo:       25001,
			expireAtNanos: 30000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 20000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
			},
		},
	}

	as := newActiveRuleSet(
		0,
		testMappingRules(t),
		nil,
		testTagsFilterOptions(),
		mockNewID,
		nil,
	)
	for i, input := range inputs {
		t.Run(fmt.Sprintf("input %d", i), func(t *testing.T) {
			res := as.ForwardMatch(b(input.id), input.matchFrom, input.matchTo)
			require.Equal(t, input.expireAtNanos, res.expireAtNanos)
			require.True(t, cmp.Equal(input.forExistingIDResult, res.ForExistingIDAt(0), testStagedMetadatasCmptOpts...))
			require.Equal(t, 0, res.NumNewRollupIDs())
		})
	}
}

func TestActiveRuleSetForwardMatchWithRollupRules(t *testing.T) {
	inputs := []testMatchInput{
		{
			id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
			matchFrom:     25000,
			matchTo:       25001,
			expireAtNanos: 30000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 22000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							metadata.DefaultPipelineMetadata,
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
								Pipeline: applied.NewPipeline([]applied.OpUnion{
									{
										Type: pipeline.RollupOpType,
										Rollup: applied.RollupOp{
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Last),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
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
											ID:            b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
										},
									},
								}),
							},
						},
					},
				},
			},
			forNewRollupIDsResult: []IDWithMetadatas{
				{
					ID: b("rName3|rtagName1=rtagValue1"),
					Metadatas: metadata.StagedMetadatas{
						metadata.StagedMetadata{
							CutoverNanos: 22000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
										},
									},
								},
							},
						},
					},
				},
				{
					ID: b("rName3|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						metadata.StagedMetadata{
							CutoverNanos: 22000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
											policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			id:            "rtagName1=rtagValue2",
			matchFrom:     25000,
			matchTo:       25001,
			expireAtNanos: 30000,
			forExistingIDResult: metadata.StagedMetadatas{
				{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata:     metadata.DefaultMetadata,
				},
			},
			forNewRollupIDsResult: []IDWithMetadatas{
				{
					ID: b("rName4|rtagName1=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						metadata.StagedMetadata{
							CutoverNanos: 24000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
										},
									},
								},
							},
						},
					},
				},
				{
					ID: b("rName5|rtagName1=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						metadata.StagedMetadata{
							CutoverNanos: 24000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			id:                  "rtagName5=rtagValue5",
			matchFrom:           25000,
			matchTo:             25001,
			expireAtNanos:       30000,
			forExistingIDResult: metadata.DefaultStagedMetadatas,
		},
		{
			id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
			matchFrom:     10000,
			matchTo:       40000,
			expireAtNanos: 90000,
			forExistingIDResult: metadata.StagedMetadatas{
				{
					CutoverNanos: 10000,
					Tombstoned:   false,
					Metadata:     metadata.DefaultMetadata,
				},
				{
					CutoverNanos: 15000,
					Tombstoned:   false,
					Metadata:     metadata.DefaultMetadata,
				},
				{
					CutoverNanos: 20000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							metadata.DefaultPipelineMetadata,
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
								Pipeline: applied.NewPipeline([]applied.OpUnion{
									{
										Type: pipeline.RollupOpType,
										Rollup: applied.RollupOp{
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
						},
					},
				},
				{
					CutoverNanos: 22000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							metadata.DefaultPipelineMetadata,
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
								Pipeline: applied.NewPipeline([]applied.OpUnion{
									{
										Type: pipeline.RollupOpType,
										Rollup: applied.RollupOp{
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Last),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
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
											ID:            b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
										},
									},
								}),
							},
						},
					},
				},
				{
					CutoverNanos: 22000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							metadata.DefaultPipelineMetadata,
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
								Pipeline: applied.NewPipeline([]applied.OpUnion{
									{
										Type: pipeline.RollupOpType,
										Rollup: applied.RollupOp{
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Last),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
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
											ID:            b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
										},
									},
								}),
							},
						},
					},
				},
				{
					CutoverNanos: 30000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							metadata.DefaultPipelineMetadata,
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
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
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Last),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
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
											ID:            b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
										},
									},
								}),
							},
						},
					},
				},
				{
					CutoverNanos: 34000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							metadata.DefaultPipelineMetadata,
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
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
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Last),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
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
											ID:            b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
										},
									},
								}),
							},
						},
					},
				},
				{
					CutoverNanos: 35000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							metadata.DefaultPipelineMetadata,
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
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
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
						},
					},
				},
				{
					CutoverNanos: 38000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							metadata.DefaultPipelineMetadata,
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
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
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
						},
					},
				},
			},
			forNewRollupIDsResult: []IDWithMetadatas{
				{
					ID: b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 10000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 15000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 20000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 22000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 24000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 30000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 34000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 35000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 38000,
							Tombstoned:   true,
						},
					},
				},
				{
					ID: b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 15000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 20000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 22000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 24000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 30000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 34000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 35000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(45*time.Second, xtime.Second, 12*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 38000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(45*time.Second, xtime.Second, 12*time.Hour),
										},
									},
								},
							},
						},
					},
				},
				{
					ID: b("rName3|rtagName1=rtagValue1"),
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 22000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 22000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 30000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 34000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 35000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 38000,
							Tombstoned:   true,
						},
					},
				},
				{
					ID: b("rName3|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 22000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
											policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 22000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
											policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 30000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
											policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 34000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 35000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 38000,
							Tombstoned:   true,
						},
					},
				},
			},
		},
	}

	as := newActiveRuleSet(
		0,
		nil,
		testRollupRules(t),
		testTagsFilterOptions(),
		mockNewID,
		nil,
	)
	for i, input := range inputs {
		t.Run(fmt.Sprintf("input %d", i), func(t *testing.T) {
			res := as.ForwardMatch(b(input.id), input.matchFrom, input.matchTo)
			require.Equal(t, input.expireAtNanos, res.expireAtNanos)
			require.True(t, cmp.Equal(input.forExistingIDResult, res.ForExistingIDAt(0), testStagedMetadatasCmptOpts...))
			require.Equal(t, len(input.forNewRollupIDsResult), res.NumNewRollupIDs())
			for i := 0; i < len(input.forNewRollupIDsResult); i++ {
				rollup := res.ForNewRollupIDsAt(i, 0)
				require.True(t, cmp.Equal(input.forNewRollupIDsResult[i], rollup, testIDWithMetadatasCmpOpts...))
			}
		})
	}
}

func TestActiveRuleSetForwardMatchWithMappingRulesAndRollupRules(t *testing.T) {
	inputs := []testMatchInput{
		{
			id:            "mtagName1=mtagValue1,rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
			matchFrom:     25000,
			matchTo:       25001,
			expireAtNanos: 30000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 22000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
								Pipeline: applied.NewPipeline([]applied.OpUnion{
									{
										Type: pipeline.RollupOpType,
										Rollup: applied.RollupOp{
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Last),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
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
											ID:            b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
										},
									},
								}),
							},
						},
					},
				},
			},
			forNewRollupIDsResult: []IDWithMetadatas{
				{
					ID: b("rName3|rtagName1=rtagValue1"),
					Metadatas: metadata.StagedMetadatas{
						metadata.StagedMetadata{
							CutoverNanos: 22000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
										},
									},
								},
							},
						},
					},
				},
				{
					ID: b("rName3|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						metadata.StagedMetadata{
							CutoverNanos: 22000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
											policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			id:            "mtagName1=mtagValue1,rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
			matchFrom:     35000,
			matchTo:       35001,
			expireAtNanos: 38000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 35000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
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
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
						},
					},
				},
			},
			forNewRollupIDsResult: []IDWithMetadatas{
				{
					ID: b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 35000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(45*time.Second, xtime.Second, 12*time.Hour),
										},
									},
								},
							},
						},
					},
				},
				{
					ID: b("rName3|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 35000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			id:            "mtagName1=mtagValue2,rtagName1=rtagValue2",
			matchFrom:     25000,
			matchTo:       25001,
			expireAtNanos: 30000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
			},
			forNewRollupIDsResult: []IDWithMetadatas{
				{
					ID: b("rName4|rtagName1=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						metadata.StagedMetadata{
							CutoverNanos: 24000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
										},
									},
								},
							},
						},
					},
				},
				{
					ID: b("rName5|rtagName1=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						metadata.StagedMetadata{
							CutoverNanos: 24000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			id:                  "mtagName1=mtagValue3,rtagName5=rtagValue5",
			matchFrom:           25000,
			matchTo:             25001,
			expireAtNanos:       30000,
			forExistingIDResult: metadata.DefaultStagedMetadatas,
		},
		{
			id:            "mtagName1=mtagValue1,rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
			matchFrom:     10000,
			matchTo:       40000,
			expireAtNanos: 90000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 10000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Count),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 15000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Count),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Mean),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 20000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Mean),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
								Pipeline: applied.NewPipeline([]applied.OpUnion{
									{
										Type: pipeline.RollupOpType,
										Rollup: applied.RollupOp{
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 22000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
								Pipeline: applied.NewPipeline([]applied.OpUnion{
									{
										Type: pipeline.RollupOpType,
										Rollup: applied.RollupOp{
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Last),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
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
											ID:            b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
										},
									},
								}),
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 22000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
								Pipeline: applied.NewPipeline([]applied.OpUnion{
									{
										Type: pipeline.RollupOpType,
										Rollup: applied.RollupOp{
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Last),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
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
											ID:            b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
										},
									},
								}),
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 30000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
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
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Last),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
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
											ID:            b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
										},
									},
								}),
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 34000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
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
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Last),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
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
											ID:            b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
										},
									},
								}),
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 35000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
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
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 38000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
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
											ID:            b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
											AggregationID: aggregation.DefaultID,
										},
									},
								}),
							},
						},
					},
				},
			},
			forNewRollupIDsResult: []IDWithMetadatas{
				{
					ID: b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 10000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 15000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 20000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 22000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 24000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 30000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 34000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 35000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 38000,
							Tombstoned:   true,
						},
					},
				},
				{
					ID: b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 15000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 20000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 22000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 24000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 30000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 34000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 35000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(45*time.Second, xtime.Second, 12*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 38000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(45*time.Second, xtime.Second, 12*time.Hour),
										},
									},
								},
							},
						},
					},
				},
				{
					ID: b("rName3|rtagName1=rtagValue1"),
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 22000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 22000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 30000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 34000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 35000,
							Tombstoned:   true,
						},
						{
							CutoverNanos: 38000,
							Tombstoned:   true,
						},
					},
				},
				{
					ID: b("rName3|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 22000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
											policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 22000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
											policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 30000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
											policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 34000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 35000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
										},
									},
								},
							},
						},
						{
							CutoverNanos: 38000,
							Tombstoned:   true,
						},
					},
				},
			},
		},
		{
			id:            "mtagName1=mtagValue3",
			matchFrom:     4000,
			matchTo:       9000,
			expireAtNanos: 10000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.DefaultStagedMetadata,
				metadata.StagedMetadata{
					CutoverNanos: 5000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 8000,
					Tombstoned:   false,
					Metadata:     metadata.DefaultMetadata,
				},
			},
		},
		{
			id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3,shouldDropTagName1=shouldDropTagValue1",
			matchFrom:     35000,
			matchTo:       35001,
			expireAtNanos: 38000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 35000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: metadata.DropPipelineMetadatas,
					},
				},
			},
			forNewRollupIDsResult: []IDWithMetadatas{
				{
					ID: b("rName2|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 35000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(45*time.Second, xtime.Second, 12*time.Hour),
										},
									},
								},
							},
						},
					},
				},
				{
					ID: b("rName3|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 35000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.DefaultID,
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
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

	as := newActiveRuleSet(
		0,
		testMappingRules(t),
		testRollupRules(t),
		testTagsFilterOptions(),
		mockNewID,
		nil,
	)
	for i, input := range inputs {
		t.Run(fmt.Sprintf("input %d", i), func(t *testing.T) {
			res := as.ForwardMatch(b(input.id), input.matchFrom, input.matchTo)
			require.Equal(t, input.expireAtNanos, res.expireAtNanos)
			require.True(t, cmp.Equal(input.forExistingIDResult, res.ForExistingIDAt(0), testStagedMetadatasCmptOpts...))
			require.Equal(t, len(input.forNewRollupIDsResult), res.NumNewRollupIDs())
			for i := 0; i < len(input.forNewRollupIDsResult); i++ {
				rollup := res.ForNewRollupIDsAt(i, 0)
				require.True(t, cmp.Equal(input.forNewRollupIDsResult[i], rollup, testIDWithMetadatasCmpOpts...))
			}
		})
	}
}

func TestActiveRuleSetReverseMatchWithMappingRulesForNonRollupID(t *testing.T) {
	inputs := []testMatchInput{
		{
			id:              "mtagName1=mtagValue1",
			matchFrom:       25000,
			matchTo:         25001,
			metricType:      metric.CounterType,
			aggregationType: aggregation.Sum,
			expireAtNanos:   30000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 22000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:              "mtagName1=mtagValue1",
			matchFrom:       35000,
			matchTo:         35001,
			metricType:      metric.CounterType,
			aggregationType: aggregation.Sum,
			expireAtNanos:   100000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 35000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
							},
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:              "mtagName1=mtagValue2",
			matchFrom:       25000,
			matchTo:         25001,
			metricType:      metric.CounterType,
			aggregationType: aggregation.Sum,
			expireAtNanos:   30000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:                  "mtagName1=mtagValue3",
			matchFrom:           25000,
			matchTo:             25001,
			metricType:          metric.CounterType,
			aggregationType:     aggregation.Sum,
			expireAtNanos:       30000,
			forExistingIDResult: metadata.DefaultStagedMetadatas,
		},
		{
			id:                  "mtagName1=mtagValue3",
			matchFrom:           25000,
			matchTo:             25001,
			metricType:          metric.CounterType,
			aggregationType:     aggregation.Min,
			expireAtNanos:       30000,
			forExistingIDResult: nil,
		},
		{
			id:                  "mtagName1=mtagValue1",
			matchFrom:           10000,
			matchTo:             12000,
			metricType:          metric.CounterType,
			aggregationType:     aggregation.Sum,
			expireAtNanos:       15000,
			forExistingIDResult: nil,
		},
		{
			id:              "mtagName1=mtagValue1",
			matchFrom:       10000,
			matchTo:         21000,
			metricType:      metric.CounterType,
			aggregationType: aggregation.Sum,
			expireAtNanos:   22000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 20000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:              "mtagName1=mtagValue1",
			matchFrom:       10000,
			matchTo:         40000,
			metricType:      metric.TimerType,
			aggregationType: aggregation.Count,
			expireAtNanos:   100000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 10000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Count),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 15000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Count),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 20000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 22000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 22000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 30000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 34000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
							},
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 35000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:              "mtagName1=mtagValue2",
			matchFrom:       10000,
			matchTo:         40000,
			metricType:      metric.CounterType,
			aggregationType: aggregation.Sum,
			expireAtNanos:   100000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.DefaultStagedMetadata,
				metadata.StagedMetadata{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:            "shouldDropTagName1=shouldDropTagValue1",
			matchFrom:     25000,
			matchTo:       25001,
			metricType:      metric.CounterType,
			aggregationType: aggregation.Sum,
			expireAtNanos: 30000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 20000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: metadata.DropPipelineMetadatas,
					},
				},
			},
		},
		{
			id:            "shouldDrop2TagName1=shouldDrop2TagValue1",
			matchFrom:     25000,
			matchTo:       25001,
			metricType:      metric.CounterType,
			aggregationType: aggregation.Sum,
			expireAtNanos: 30000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 20000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: metadata.DropPipelineMetadatas,
					},
				},
			},
		},
		{
			id:            "shouldNotDropTagName1=shouldNotDropTagValue1",
			matchFrom:     25000,
			matchTo:       25001,
			metricType:      metric.CounterType,
			aggregationType: aggregation.Sum,
			expireAtNanos: 30000,
			forExistingIDResult: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 20000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
						},
					},
				},
			},
		},
	}

	isMultiAggregationTypesAllowed := true
	aggTypesOpts := aggregation.NewTypesOptions()
	as := newActiveRuleSet(
		0,
		testMappingRules(t),
		nil,
		testTagsFilterOptions(),
		mockNewID,
		func([]byte, []byte) bool { return false },
	)
	for i, input := range inputs {
		t.Run(fmt.Sprintf("input %d", i), func(t *testing.T) {
			res := as.ReverseMatch(b(input.id), input.matchFrom, input.matchTo,
			input.metricType, input.aggregationType, isMultiAggregationTypesAllowed, aggTypesOpts)
			require.Equal(t, input.expireAtNanos, res.expireAtNanos)
			require.True(t, cmp.Equal(input.forExistingIDResult, res.ForExistingIDAt(0), testStagedMetadatasCmptOpts...))
		})
	}
}

func TestActiveRuleSetReverseMatchWithRollupRulesForRollupID(t *testing.T) {
	inputs := []testMatchInput{
		{
			id:              "rName4|rtagName1=rtagValue2",
			matchFrom:       25000,
			matchTo:         25001,
			metricType:      metric.CounterType,
			aggregationType: aggregation.Sum,
			expireAtNanos:   30000,
			forExistingIDResult: metadata.StagedMetadatas{
				{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:                  "rName4|rtagName1=rtagValue2",
			matchFrom:           25000,
			matchTo:             25001,
			metricType:          metric.CounterType,
			aggregationType:     aggregation.Min,
			expireAtNanos:       30000,
			forExistingIDResult: nil,
		},
		{
			id:                  "rName4|rtagName1=rtagValue2",
			matchFrom:           25000,
			matchTo:             25001,
			metricType:          metric.UnknownType,
			aggregationType:     aggregation.UnknownType,
			expireAtNanos:       30000,
			forExistingIDResult: nil,
		},
		{
			id:              "rName4|rtagName1=rtagValue2",
			matchFrom:       25000,
			matchTo:         25001,
			metricType:      metric.TimerType,
			aggregationType: aggregation.P99,
			expireAtNanos:   30000,
			forExistingIDResult: metadata.StagedMetadatas{
				{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:                  "rName4|rtagName2=rtagValue2",
			matchFrom:           25000,
			matchTo:             25001,
			metricType:          metric.CounterType,
			aggregationType:     aggregation.Sum,
			expireAtNanos:       30000,
			forExistingIDResult: nil,
		},
		{
			id:                  "rName4|rtagName1=rtagValue2",
			matchFrom:           10000,
			matchTo:             10001,
			expireAtNanos:       15000,
			forExistingIDResult: nil,
		},
		{
			id:              "rName5|rtagName1=rtagValue2",
			matchFrom:       130000,
			matchTo:         130001,
			metricType:      metric.TimerType,
			aggregationType: aggregation.P99,
			expireAtNanos:   math.MaxInt64,
			forExistingIDResult: metadata.StagedMetadatas{
				{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute),
								},
							},
						},
					},
				},
			},
		},
		{
			id:              "rName5|rtagName1=rtagValue2",
			matchFrom:       130000,
			matchTo:         130001,
			metricType:      metric.GaugeType,
			aggregationType: aggregation.Last,
			expireAtNanos:   math.MaxInt64,
			forExistingIDResult: metadata.StagedMetadatas{
				{
					CutoverNanos: 24000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute),
								},
							},
						},
					},
				},
			},
		},
		{
			id:              "rName7|rtagName1=rtagValue2",
			matchFrom:       130000,
			matchTo:         130001,
			metricType:      metric.TimerType,
			aggregationType: aggregation.P90,
			expireAtNanos:   math.MaxInt64,
			forExistingIDResult: metadata.StagedMetadatas{
				{
					CutoverNanos: 120000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Sum, aggregation.P90),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			id:              "rName83|rtagName1=rtagValue2",
			matchFrom:       130000,
			matchTo:         130001,
			metricType:      metric.TimerType,
			aggregationType: aggregation.Min,
			expireAtNanos:   math.MaxInt64,
			forExistingIDResult: metadata.StagedMetadatas{
				{
					CutoverNanos: 90000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.MustCompressTypes(aggregation.Min),
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour),
								},
							},
						},
					},
				},
			},
		},
	}

	isMultiAggregationTypesAllowed := true
	aggTypesOpts := aggregation.NewTypesOptions()
	as := newActiveRuleSet(
		0,
		nil,
		testRollupRules(t),
		testTagsFilterOptions(),
		mockNewID,
		func([]byte, []byte) bool { return true },
	)
	for _, input := range inputs {
		res := as.ReverseMatch(b(input.id), input.matchFrom, input.matchTo, input.metricType, input.aggregationType, isMultiAggregationTypesAllowed, aggTypesOpts)
		require.Equal(t, input.expireAtNanos, res.expireAtNanos)
		require.Equal(t, input.forExistingIDResult, res.ForExistingIDAt(0))
		require.Equal(t, 0, res.NumNewRollupIDs())
	}
}

func testMappingRules(t *testing.T) []*mappingRule {
	filter1, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{"mtagName1": filters.FilterValue{Pattern: "mtagValue1"}},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)
	filter2, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{"mtagName1": filters.FilterValue{Pattern: "mtagValue2"}},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)
	filter3, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{"mtagName1": filters.FilterValue{Pattern: "mtagValue3"}},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)
	filter4, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{"mtagName1": filters.FilterValue{Pattern: "mtagValue4"}},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)
	filter5, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{"shouldDropTagName1": filters.FilterValue{Pattern: "shouldDropTagValue1"}},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)
	filter6, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{"shouldDrop2TagName1": filters.FilterValue{Pattern: "shouldDrop2TagValue1"}},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)
	filter7, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{"shouldNotDropTagName1": filters.FilterValue{Pattern: "shouldNotDropTagValue1"}},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)

	mappingRule1 := &mappingRule{
		uuid: "mappingRule1",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:          "mappingRule1.snapshot1",
				tombstoned:    false,
				cutoverNanos:  10000,
				filter:        filter1,
				aggregationID: aggregation.MustCompressTypes(aggregation.Count),
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
				},
			},
			&mappingRuleSnapshot{
				name:          "mappingRule1.snapshot1",
				tombstoned:    false,
				cutoverNanos:  15000,
				filter:        filter1,
				aggregationID: aggregation.MustCompressTypes(aggregation.Count),
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
				},
			},
			&mappingRuleSnapshot{
				name:          "mappingRule1.snapshot2",
				tombstoned:    false,
				cutoverNanos:  20000,
				filter:        filter1,
				aggregationID: aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
					policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
					policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
				},
			},
			&mappingRuleSnapshot{
				name:          "mappingRule1.snapshot3",
				tombstoned:    false,
				cutoverNanos:  30000,
				filter:        filter1,
				aggregationID: aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
				},
			},
		},
	}

	mappingRule2 := &mappingRule{
		uuid: "mappingRule2",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:          "mappingRule2.snapshot1",
				tombstoned:    false,
				cutoverNanos:  15000,
				filter:        filter1,
				aggregationID: aggregation.MustCompressTypes(aggregation.Mean),
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
				},
			},
			&mappingRuleSnapshot{
				name:          "mappingRule2.snapshot2",
				tombstoned:    false,
				cutoverNanos:  22000,
				filter:        filter1,
				aggregationID: aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
			&mappingRuleSnapshot{
				name:          "mappingRule2.snapshot3",
				tombstoned:    true,
				cutoverNanos:  35000,
				filter:        filter1,
				aggregationID: aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
		},
	}

	mappingRule3 := &mappingRule{
		uuid: "mappingRule3",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:          "mappingRule3.snapshot1",
				tombstoned:    false,
				cutoverNanos:  22000,
				filter:        filter1,
				aggregationID: aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			},
			&mappingRuleSnapshot{
				name:          "mappingRule3.snapshot2",
				tombstoned:    false,
				cutoverNanos:  34000,
				filter:        filter1,
				aggregationID: aggregation.MustCompressTypes(aggregation.Sum),
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
		},
	}

	mappingRule4 := &mappingRule{
		uuid: "mappingRule4",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:          "mappingRule4.snapshot1",
				tombstoned:    false,
				cutoverNanos:  24000,
				filter:        filter2,
				aggregationID: aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
				},
			},
		},
	}

	mappingRule5 := &mappingRule{
		uuid: "mappingRule5",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:          "mappingRule5.snapshot1",
				tombstoned:    false,
				cutoverNanos:  100000,
				filter:        filter1,
				aggregationID: aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
				},
			},
		},
	}

	mappingRule6 := &mappingRule{
		uuid: "mappingRule6",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:          "mappingRule6.snapshot1",
				tombstoned:    false,
				cutoverNanos:  5000,
				filter:        filter3,
				aggregationID: aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
				},
			},
			&mappingRuleSnapshot{
				name:          "mappingRule6.snapshot2",
				tombstoned:    false,
				cutoverNanos:  8000,
				filter:        filter4,
				aggregationID: aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
		},
	}

	// Mapping rule 7 and 8 should combine to effectively be a drop when combined as
	// mapping rule 8 explicitly says must be dropped
	mappingRule7 := &mappingRule{
		uuid: "mappingRule7",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:          "mappingRule7.snapshot1",
				tombstoned:    false,
				cutoverNanos:  20000,
				filter:        filter5,
				aggregationID: aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
				},
			},
		},
	}

	mappingRule8 := &mappingRule{
		uuid: "mappingRule8",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:            "mappingRule8.snapshot1",
				tombstoned:      false,
				cutoverNanos:    20000,
				filter:          filter5,
				aggregationID:   aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{},
				dropPolicy:      policy.DropMust,
			},
		},
	}

	// Mapping rule 9 should effectively be a drop since no other mapping rules match and
	// mapping rule 9 explicitly says must be dropped except if there is another match
	mappingRule9 := &mappingRule{
		uuid: "mappingRule9",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:            "mappingRule9.snapshot1",
				tombstoned:      false,
				cutoverNanos:    20000,
				filter:          filter6,
				aggregationID:   aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{},
				dropPolicy:      policy.DropIfOnlyMatch,
			},
		},
	}

	// Mapping rule 10 and 11 should combine to effectively be a no-drop when combined as
	// mapping rule 10 explicitly says drop only if no other drops
	mappingRule10 := &mappingRule{
		uuid: "mappingRule10",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:          "mappingRule10.snapshot1",
				tombstoned:    false,
				cutoverNanos:  20000,
				filter:        filter7,
				aggregationID: aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
				},
			},
		},
	}

	mappingRule11 := &mappingRule{
		uuid: "mappingRule11",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:            "mappingRule11.snapshot1",
				tombstoned:      false,
				cutoverNanos:    20000,
				filter:          filter7,
				aggregationID:   aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{},
				dropPolicy:      policy.DropIfOnlyMatch,
			},
		},
	}

	return []*mappingRule{mappingRule1, mappingRule2, mappingRule3, mappingRule4,
		mappingRule5, mappingRule6, mappingRule7, mappingRule8, mappingRule9,
		mappingRule10, mappingRule11}
}

func testRollupRules(t *testing.T) []*rollupRule {
	filter1, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{
			"rtagName1": filters.FilterValue{Pattern: "rtagValue1"},
			"rtagName2": filters.FilterValue{Pattern: "rtagValue2"},
		},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)
	filter2, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{
			"rtagName1": filters.FilterValue{Pattern: "rtagValue2"},
		},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)

	rollupRule1 := &rollupRule{
		uuid: "rollupRule1",
		snapshots: []*rollupRuleSnapshot{
			&rollupRuleSnapshot{
				name:         "rollupRule1.snapshot1",
				tombstoned:   false,
				cutoverNanos: 10000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName1"),
									Tags:          bs("rtagName1", "rtagName2"),
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
						},
					},
				},
			},
			&rollupRuleSnapshot{
				name:         "rollupRule1.snapshot2",
				tombstoned:   false,
				cutoverNanos: 20000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.AggregationOpType,
								Aggregation: pipeline.AggregationOp{
									Type: aggregation.Sum,
								},
							},
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName1"),
									Tags:          bs("rtagName1", "rtagName2"),
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
							policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
							policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
						},
					},
				},
			},
			&rollupRuleSnapshot{
				name:         "rollupRule1.snapshot3",
				tombstoned:   false,
				cutoverNanos: 30000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{
									Type: transformation.PerSecond,
								},
							},
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName1"),
									Tags:          bs("rtagName1", "rtagName2"),
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
						},
					},
				},
			},
		},
	}

	rollupRule2 := &rollupRule{
		uuid: "rollupRule2",
		snapshots: []*rollupRuleSnapshot{
			&rollupRuleSnapshot{
				name:         "rollupRule2.snapshot1",
				tombstoned:   false,
				cutoverNanos: 15000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName2"),
									Tags:          bs("rtagName1", "rtagName2"),
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
						},
					},
				},
			},
			&rollupRuleSnapshot{
				name:         "rollupRule2.snapshot2",
				tombstoned:   false,
				cutoverNanos: 22000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.AggregationOpType,
								Aggregation: pipeline.AggregationOp{
									Type: aggregation.Last,
								},
							},
							{
								Type: pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{
									Type: transformation.PerSecond,
								},
							},
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName2"),
									Tags:          bs("rtagName1", "rtagName2"),
									AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
						},
					},
				},
			},
			&rollupRuleSnapshot{
				name:         "rollupRule2.snapshot3",
				tombstoned:   false,
				cutoverNanos: 35000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName2"),
									Tags:          bs("rtagName1", "rtagName2"),
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(45*time.Second, xtime.Second, 12*time.Hour),
						},
					},
				},
			},
		},
	}

	rollupRule3 := &rollupRule{
		uuid: "rollupRule3",
		snapshots: []*rollupRuleSnapshot{
			&rollupRuleSnapshot{
				name:         "rollupRule3.snapshot1",
				tombstoned:   false,
				cutoverNanos: 22000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName3"),
									Tags:          bs("rtagName1", "rtagName2"),
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
							policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
						},
					},
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName3"),
									Tags:          bs("rtagName1"),
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
						},
					},
				},
			},
			&rollupRuleSnapshot{
				name:         "rollupRule3.snapshot2",
				tombstoned:   false,
				cutoverNanos: 34000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName3"),
									Tags:          bs("rtagName1", "rtagName2"),
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
						},
					},
				},
			},
			&rollupRuleSnapshot{
				name:         "rollupRule3.snapshot3",
				tombstoned:   true,
				cutoverNanos: 38000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName3"),
									Tags:          bs("rtagName1", "rtagName2"),
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
						},
					},
				},
			},
		},
	}

	rollupRule4 := &rollupRule{
		uuid: "rollupRule4",
		snapshots: []*rollupRuleSnapshot{
			&rollupRuleSnapshot{
				name:         "rollupRule4.snapshot1",
				tombstoned:   false,
				cutoverNanos: 24000,
				filter:       filter2,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName4"),
									Tags:          bs("rtagName1"),
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
						},
					},
				},
			},
		},
	}

	rollupRule5 := &rollupRule{
		uuid: "rollupRule5",
		snapshots: []*rollupRuleSnapshot{
			&rollupRuleSnapshot{
				name:               "rollupRule5.snapshot1",
				tombstoned:         false,
				cutoverNanos:       24000,
				filter:             filter2,
				lastUpdatedAtNanos: 123456,
				lastUpdatedBy:      "test",
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName5"),
									Tags:          bs("rtagName1"),
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute),
						},
					},
				},
			},
		},
	}

	rollupRule6 := &rollupRule{
		uuid: "rollupRule6",
		snapshots: []*rollupRuleSnapshot{
			&rollupRuleSnapshot{
				name:         "rollupRule6.snapshot1",
				tombstoned:   false,
				cutoverNanos: 100000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName6"),
									Tags:          bs("rtagName1", "rtagName2"),
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
						},
					},
				},
			},
		},
	}

	rollupRule7 := &rollupRule{
		uuid: "rollupRule7",
		snapshots: []*rollupRuleSnapshot{
			&rollupRuleSnapshot{
				name:               "rollupRule7.snapshot1",
				tombstoned:         false,
				cutoverNanos:       120000,
				filter:             filter2,
				lastUpdatedAtNanos: 125000,
				lastUpdatedBy:      "test",
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName7"),
									Tags:          bs("rtagName1"),
									AggregationID: aggregation.MustCompressTypes(aggregation.Sum, aggregation.P90),
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour),
						},
					},
				},
			},
		},
	}

	rollupRule8 := &rollupRule{
		uuid: "rollupRule8",
		snapshots: []*rollupRuleSnapshot{
			&rollupRuleSnapshot{
				name:               "rollupRule8.snapshot1",
				tombstoned:         false,
				cutoverNanos:       90000,
				filter:             filter2,
				lastUpdatedAtNanos: 95000,
				lastUpdatedBy:      "test",
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName81"),
									Tags:          bs("rtagName1", "rtagName2", "rtagName3"),
									AggregationID: aggregation.MustCompressTypes(aggregation.Sum, aggregation.P90),
								},
							},
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName82"),
									Tags:          bs("rtagName1", "rtagName2"),
									AggregationID: aggregation.MustCompressTypes(aggregation.Count),
								},
							},
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       b("rName83"),
									Tags:          bs("rtagName1"),
									AggregationID: aggregation.MustCompressTypes(aggregation.Min),
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour),
						},
					},
				},
			},
		},
	}

	return []*rollupRule{rollupRule1, rollupRule2, rollupRule3, rollupRule4, rollupRule5, rollupRule6, rollupRule7, rollupRule8}
}
