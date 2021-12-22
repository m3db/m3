// Copyright (c) 2020 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/matcher/namespace"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/query/models"
	xtime "github.com/m3db/m3/src/x/time"

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
	)
	expectedCutovers := []int64{10000, 15000, 20000, 22000, 24000, 30000, 34000, 35000, 38000, 90000, 100000, 120000}
	require.Equal(t, expectedCutovers, as.cutoverTimesAsc)
}

func TestActiveRuleSetLatestRollupRules(t *testing.T) {
	rules := testRollupRules(t)
	as := newActiveRuleSet(
		0,
		nil,
		rules,
		testTagsFilterOptions(),
		mockNewID,
	)
	timeNanos := int64(95000)
	rollupView, err := as.LatestRollupRules(nil, timeNanos)
	require.NoError(t, err)
	// rr3 is tombstoned, so it should not be returned
	require.Equal(t, len(rules)-1, len(rollupView))
	for _, rr := range rollupView {
		// explicitly check that rollupRule3 is not returned..
		require.NotEqual(t, rr.Name, "rollupRule3.snapshot3")
		// ..and that we only get the non-Tombstoned entries.
		require.False(t, rr.Tombstoned)
	}
}

func TestActiveRuleSetCutoverTimesWithMappingRulesAndRollupRules(t *testing.T) {
	as := newActiveRuleSet(
		0,
		testMappingRules(t),
		testRollupRules(t),
		testTagsFilterOptions(),
		mockNewID,
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
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
							metadata.DropPipelineMetadata,
						},
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
						Pipelines: metadata.DropIfOnlyMatchPipelineMetadatas,
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
							metadata.DropIfOnlyMatchPipelineMetadata,
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
	)
	for i, input := range inputs {
		input := input
		t.Run(fmt.Sprintf("input %d", i), func(t *testing.T) {
			res, err := as.ForwardMatch(input.ID(), input.matchFrom, input.matchTo, testMatchOptions())
			require.NoError(t, err)
			require.Equal(t, input.expireAtNanos, res.expireAtNanos)
			require.True(t, cmp.Equal(input.forExistingIDResult, res.ForExistingIDAt(0), testStagedMetadatasCmptOpts...))
			require.Equal(t, 0, res.NumNewRollupIDs())
		})
	}
}

func TestActiveRuleSetForwardMatchWithAnyKeepOriginal(t *testing.T) {
	inputs := []testMatchInput{
		{
			id:           "rtagName1=rtagValue1",
			matchFrom:    25000,
			matchTo:      25001,
			keepOriginal: true,
		},
	}

	as := newActiveRuleSet(
		0,
		nil,
		testKeepOriginalRollupRules(t),
		testTagsFilterOptions(),
		mockNewID,
	)

	for i, input := range inputs {
		input := input
		t.Run(fmt.Sprintf("input %d", i), func(t *testing.T) {
			res, err := as.ForwardMatch(input.ID(), input.matchFrom, input.matchTo, testMatchOptions())
			require.NoError(t, err)
			require.Equal(t, res.keepOriginal, input.keepOriginal)
			require.Equal(t, 3, res.NumNewRollupIDs())
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
			keepOriginal:  true,
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
			keepOriginal:  false,
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
			keepOriginal:        false,
			forExistingIDResult: metadata.DefaultStagedMetadatas,
		},
		{
			id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
			matchFrom:     10000,
			matchTo:       40000,
			expireAtNanos: 90000,
			keepOriginal:  false,
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
		//nolint:dupl
		{
			id:            "rtagName1=rtagValue3,rtagName2=rtagValue2,rtagName3=rtagValue3",
			matchFrom:     100000,
			matchTo:       110000,
			expireAtNanos: 120000,
			keepOriginal:  false,
			forExistingIDResult: metadata.StagedMetadatas{
				{
					CutoverNanos: 100000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							metadata.DefaultPipelineMetadata,
						},
					},
				},
			},
			forNewRollupIDsResult: []IDWithMetadatas{
				{
					ID: b("rName9A|rtagName2=rtagValue2,rtagName3=rtagValue3"), // rtagName1 is excluded away.
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 100000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.MustCompressTypes(aggregation.Last),
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour),
										},
										Pipeline: applied.Pipeline{Operations: []applied.OpUnion{}},
									},
								},
							},
						},
					},
				},
				{
					ID: b("rName9B|rtagName2=rtagValue2,rtagName3=rtagValue3"), // rtagName1 is excluded away.
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 100000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.MustCompressTypes(aggregation.Last),
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour),
										},
										Pipeline: applied.Pipeline{Operations: []applied.OpUnion{}},
									},
								},
							},
						},
					},
				},
				{
					ID: b("rName9C|rtagName2=rtagValue2"), // rtagName1 and rtagName3 are excluded away.
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 100000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.MustCompressTypes(aggregation.Last),
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour),
										},
										Pipeline: applied.Pipeline{Operations: []applied.OpUnion{}},
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
		nil,
		testRollupRules(t),
		testTagsFilterOptions(),
		mockNewID,
	)

	for i, input := range inputs {
		input := input
		t.Run(fmt.Sprintf("input %d", i), func(t *testing.T) {
			res, err := as.ForwardMatch(input.ID(), input.matchFrom, input.matchTo, testMatchOptions())
			require.NoError(t, err)
			require.Equal(t, input.expireAtNanos, res.expireAtNanos)
			require.True(t, cmp.Equal(input.forExistingIDResult, res.ForExistingIDAt(0), testStagedMetadatasCmptOpts...))
			require.Equal(t, len(input.forNewRollupIDsResult), res.NumNewRollupIDs())
			require.Equal(t, input.keepOriginal, res.KeepOriginal())
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
			keepOriginal:  true,
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
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
								},
							},
							metadata.DropPipelineMetadata,
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
		//nolint:dupl
		{
			id:            "rtagName1=rtagValue3,rtagName2=rtagValue2,rtagName3=rtagValue3",
			matchFrom:     100000,
			matchTo:       110000,
			expireAtNanos: 120000,
			keepOriginal:  false,
			forExistingIDResult: metadata.StagedMetadatas{
				{
					CutoverNanos: 100000,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							metadata.DefaultPipelineMetadata,
						},
					},
				},
			},
			forNewRollupIDsResult: []IDWithMetadatas{
				{
					ID: b("rName9A|rtagName2=rtagValue2,rtagName3=rtagValue3"), // rtagName1 is excluded away.
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 100000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.MustCompressTypes(aggregation.Last),
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour),
										},
										Pipeline: applied.Pipeline{Operations: []applied.OpUnion{}},
									},
								},
							},
						},
					},
				},
				{
					ID: b("rName9B|rtagName2=rtagValue2,rtagName3=rtagValue3"), // rtagName1 is excluded away.
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 100000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.MustCompressTypes(aggregation.Last),
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour),
										},
										Pipeline: applied.Pipeline{Operations: []applied.OpUnion{}},
									},
								},
							},
						},
					},
				},
				{
					ID: b("rName9C|rtagName2=rtagValue2"), // rtagName1 and rtagName3 are excluded away.
					Metadatas: metadata.StagedMetadatas{
						{
							CutoverNanos: 100000,
							Tombstoned:   false,
							Metadata: metadata.Metadata{
								Pipelines: []metadata.PipelineMetadata{
									{
										AggregationID: aggregation.MustCompressTypes(aggregation.Last),
										StoragePolicies: policy.StoragePolicies{
											policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
											policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour),
										},
										Pipeline: applied.Pipeline{Operations: []applied.OpUnion{}},
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
	)
	for i, input := range inputs {
		input := input
		t.Run(fmt.Sprintf("input %d", i), func(t *testing.T) {
			res, err := as.ForwardMatch(input.ID(), input.matchFrom, input.matchTo, testMatchOptions())
			require.NoError(t, err)
			require.Equal(t, input.expireAtNanos, res.expireAtNanos)
			require.True(t, cmp.Equal(input.forExistingIDResult, res.ForExistingIDAt(0), testStagedMetadatasCmptOpts...))
			require.Equal(t, len(input.forNewRollupIDsResult), res.NumNewRollupIDs())
			require.Equal(t, input.keepOriginal, res.KeepOriginal())
			for i := 0; i < len(input.forNewRollupIDsResult); i++ {
				rollup := res.ForNewRollupIDsAt(i, 0)
				require.True(t, cmp.Equal(input.forNewRollupIDsResult[i], rollup, testIDWithMetadatasCmpOpts...))
			}
		})
	}
}

func TestMatchedKeepOriginal(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rollup.r2",
		[]string{"foo"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	filter, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{
			"foo": filters.FilterValue{Pattern: "bar"},
			"baz": filters.FilterValue{Pattern: "bat"},
		},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)

	var (
		targets = []rollupTarget{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type:   pipeline.RollupOpType,
						Rollup: rr1,
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
				},
			},
		}
		rollups = []*rollupRule{
			{
				uuid: "rollup",
				snapshots: []*rollupRuleSnapshot{
					{
						name:         "rollup.1.nokeep",
						cutoverNanos: 0,
						filter:       filter,
						keepOriginal: false,
						targets:      targets,
					},
					{
						name:         "rollup.2.keep",
						cutoverNanos: 10000,
						filter:       filter,
						keepOriginal: true,
						targets:      targets,
					},
					{
						name:         "rollup.2.nokeep",
						tombstoned:   false,
						cutoverNanos: 20000,
						filter:       filter,
						targets:      targets,
					},
				},
			},
		}
		as = newActiveRuleSet(
			0,
			nil,
			rollups,
			testTagsFilterOptions(),
			mockNewID,
		)
	)

	cases := map[string]struct {
		expectKeepOriginal bool
		cutoverNanos       int64
	}{
		"latest-nokeep-first": {
			expectKeepOriginal: false,
			cutoverNanos:       0,
		},
		"latest-keep-nested": {
			expectKeepOriginal: true,
			cutoverNanos:       10000,
		},
		"latest-nokeep-last": {
			expectKeepOriginal: false,
			cutoverNanos:       20000,
		},
	}

	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			res, err := as.ForwardMatch(
				namespace.NewTestID("baz=bat,foo=bar", "ns"),
				tt.cutoverNanos,
				tt.cutoverNanos+10000,
				testMatchOptions(),
			)
			require.NoError(t, err)
			require.Equal(t, 1, res.NumNewRollupIDs())
			require.Equal(t, tt.expectKeepOriginal, res.KeepOriginal())
		})
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

	tags := []models.Tag{{Name: []byte("service")}}
	mappingRule1 := &mappingRule{
		uuid: "mappingRule1",
		snapshots: []*mappingRuleSnapshot{
			{
				name:          "mappingRule1.snapshot1",
				tombstoned:    false,
				cutoverNanos:  10000,
				filter:        filter1,
				aggregationID: aggregation.MustCompressTypes(aggregation.Count),
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
				},
				tags: tags,
			},
			{
				name:          "mappingRule1.snapshot1",
				tombstoned:    false,
				cutoverNanos:  15000,
				filter:        filter1,
				aggregationID: aggregation.MustCompressTypes(aggregation.Count),
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
				},
				tags: tags,
			},
			{
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
				tags: tags,
			},
			{
				name:          "mappingRule1.snapshot3",
				tombstoned:    false,
				cutoverNanos:  30000,
				filter:        filter1,
				aggregationID: aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour),
				},
				tags: tags,
			},
		},
	}

	mappingRule2 := &mappingRule{
		uuid: "mappingRule2",
		snapshots: []*mappingRuleSnapshot{
			{
				name:          "mappingRule2.snapshot1",
				tombstoned:    false,
				cutoverNanos:  15000,
				filter:        filter1,
				aggregationID: aggregation.MustCompressTypes(aggregation.Mean),
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
				},
			},
			{
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
			{
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
			{
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
			{
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
			{
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
			{
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
			{
				name:          "mappingRule6.snapshot1",
				tombstoned:    false,
				cutoverNanos:  5000,
				filter:        filter3,
				aggregationID: aggregation.DefaultID,
				storagePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
				},
			},
			{
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

	// Mapping rule 7 and 8 should combine to have the the aggregation as per
	// mapping rule 7 to occur with the metrics being dropped for the default
	// aggregation as per mapping rule 8 which explicitly says must be dropped
	mappingRule7 := &mappingRule{
		uuid: "mappingRule7",
		snapshots: []*mappingRuleSnapshot{
			{
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
			{
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
			{
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

	// Mapping rule 10 and 11 should combine to have the the aggregation as per
	// mapping rule 10 to occur with the metrics being dropped for the default
	// aggregation as per mapping rule 11 which says it must be dropped on
	// match
	mappingRule10 := &mappingRule{
		uuid: "mappingRule10",
		snapshots: []*mappingRuleSnapshot{
			{
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
			{
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

func testKeepOriginalRollupRules(t *testing.T) []*rollupRule {
	filter, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{
			"rtagName1": filters.FilterValue{Pattern: "rtagValue1"},
		},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)

	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName2",
		[]string{"rtagName1"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr3, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName3",
		[]string{"rtagName1"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	rollupRule1 := &rollupRule{
		uuid: "rollupRule1",
		snapshots: []*rollupRuleSnapshot{
			{
				name:         "rollupRule1.snapshot",
				tombstoned:   false,
				cutoverNanos: 0,
				filter:       filter,
				keepOriginal: false,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
						},
					},
				},
			},
		},
	}

	rollupRule2 := &rollupRule{
		uuid: "rollupRule2",
		snapshots: []*rollupRuleSnapshot{
			{
				name:         "rollupRule2.snapshot",
				tombstoned:   false,
				cutoverNanos: 0,
				filter:       filter,
				keepOriginal: true,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr2,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
						},
					},
				},
			},
		},
	}

	rollupRule3 := &rollupRule{
		uuid: "rollupRule3",
		snapshots: []*rollupRuleSnapshot{
			{
				name:         "rollupRule3.snapshot",
				tombstoned:   false,
				cutoverNanos: 0,
				filter:       filter,
				keepOriginal: true,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr3,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
						},
					},
				},
			},
		},
	}

	return []*rollupRule{rollupRule1, rollupRule2, rollupRule3}
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
	filter3, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{
			"rtagName1": filters.FilterValue{Pattern: "rtagValue3"},
		},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)

	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName1",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName2",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr2_2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName2",
		[]string{"rtagName1", "rtagName2"},
		aggregation.MustCompressTypes(aggregation.Sum),
	)
	require.NoError(t, err)
	rr3, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName3",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr3_2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName3",
		[]string{"rtagName1"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr4, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName4",
		[]string{"rtagName1"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr5, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName5",
		[]string{"rtagName1"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr6, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName6",
		[]string{"rtagName1", "rtagName2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr7, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName7",
		[]string{"rtagName1"},
		aggregation.MustCompressTypes(aggregation.Sum, aggregation.P90),
	)
	require.NoError(t, err)
	rr8, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName81",
		[]string{"rtagName1", "rtagName2", "rtagName3"},
		aggregation.MustCompressTypes(aggregation.Sum, aggregation.P90),
	)
	require.NoError(t, err)
	rr8_2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName82",
		[]string{"rtagName1", "rtagName2"},
		aggregation.MustCompressTypes(aggregation.Count),
	)
	require.NoError(t, err)
	rr8_3, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"rName83",
		[]string{"rtagName1"},
		aggregation.MustCompressTypes(aggregation.Min),
	)
	require.NoError(t, err)

	rr9, err := pipeline.NewRollupOp(
		pipeline.ExcludeByRollupType,
		"rName9A",
		[]string{"rtagName1"},
		aggregation.MustCompressTypes(aggregation.Last),
	)
	require.NoError(t, err)

	rr10, err := pipeline.NewRollupOp(
		pipeline.ExcludeByRollupType,
		"rName9B",
		// Include a case where an exclude rule references a tag that doesn't
		// exist in the input metric and is lexicographyically less than the
		// other rules. This covers regressions around sorted list merge behavior
		// in the excludeBy (vs groupBy) case.
		[]string{"atagName1", "rtagName1"},
		aggregation.MustCompressTypes(aggregation.Last),
	)
	require.NoError(t, err)

	rr11, err := pipeline.NewRollupOp(
		pipeline.ExcludeByRollupType,
		"rName9C",
		[]string{"rtagName1", "rtagName3"},
		aggregation.MustCompressTypes(aggregation.Last),
	)
	require.NoError(t, err)

	rollupRule1 := &rollupRule{
		uuid: "rollupRule1",
		snapshots: []*rollupRuleSnapshot{
			{
				name:         "rollupRule1.snapshot1",
				tombstoned:   false,
				cutoverNanos: 10000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
						},
					},
				},
			},
			{
				name:         "rollupRule1.snapshot2",
				tombstoned:   false,
				cutoverNanos: 20000,
				filter:       filter1,
				keepOriginal: true,
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
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
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
			{
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
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
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
			{
				name:         "rollupRule2.snapshot1",
				tombstoned:   false,
				cutoverNanos: 15000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr2,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
						},
					},
				},
			},
			{
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
								Type:   pipeline.RollupOpType,
								Rollup: rr2_2,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
						},
					},
				},
			},
			{
				name:         "rollupRule2.snapshot3",
				tombstoned:   false,
				cutoverNanos: 35000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr2,
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
			{
				name:         "rollupRule3.snapshot1",
				tombstoned:   false,
				cutoverNanos: 22000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr3,
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
								Type:   pipeline.RollupOpType,
								Rollup: rr3_2,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
						},
					},
				},
			},
			{
				name:         "rollupRule3.snapshot2",
				tombstoned:   false,
				cutoverNanos: 34000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr3,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
						},
					},
				},
			},
			{
				name:         "rollupRule3.snapshot3",
				tombstoned:   true,
				cutoverNanos: 38000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr3,
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
			{
				name:         "rollupRule4.snapshot1",
				tombstoned:   false,
				cutoverNanos: 24000,
				filter:       filter2,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr4,
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
			{
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
								Type:   pipeline.RollupOpType,
								Rollup: rr5,
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
			{
				name:         "rollupRule6.snapshot1",
				tombstoned:   false,
				cutoverNanos: 100000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr6,
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
			{
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
								Type:   pipeline.RollupOpType,
								Rollup: rr7,
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
			{
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
								Type:   pipeline.RollupOpType,
								Rollup: rr8,
							},
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr8_2,
							},
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr8_3,
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

	rollupRule9 := &rollupRule{
		uuid: "rollupRule9",
		snapshots: []*rollupRuleSnapshot{
			{
				name:               "rollupRule9.snapshot1",
				tombstoned:         false,
				cutoverNanos:       100000,
				filter:             filter3,
				lastUpdatedAtNanos: 105000,
				lastUpdatedBy:      "test",
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr9,
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

	rollupRule10 := &rollupRule{
		uuid: "rollupRule10",
		snapshots: []*rollupRuleSnapshot{
			{
				name:               "rollupRule10.snapshot1",
				tombstoned:         false,
				cutoverNanos:       100000,
				filter:             filter3,
				lastUpdatedAtNanos: 105000,
				lastUpdatedBy:      "test",
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr10,
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

	rollupRule11 := &rollupRule{
		uuid: "rollupRule11",
		snapshots: []*rollupRuleSnapshot{
			{
				name:               "rollupRule11.snapshot1",
				tombstoned:         false,
				cutoverNanos:       100000,
				filter:             filter3,
				lastUpdatedAtNanos: 105000,
				lastUpdatedBy:      "test",
				targets: []rollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr11,
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

	return []*rollupRule{
		rollupRule1,
		rollupRule2,
		rollupRule3,
		rollupRule4,
		rollupRule5,
		rollupRule6,
		rollupRule7,
		rollupRule8,
		rollupRule9,
		rollupRule10,
		rollupRule11,
	}
}
