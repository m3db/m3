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

package view

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestRuleSetSort(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"name",
		[]string{"tag2", "tag1"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"name",
		[]string{"tag2", "tag1"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	ruleset := RuleSet{
		Namespace:     "testNamespace",
		Version:       1,
		CutoverMillis: 1234,
		MappingRules: []MappingRule{
			{
				ID:            "uuid1",
				Name:          "foo",
				Filter:        "filter1",
				AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
				},
			},
			{
				ID:            "uuid2",
				Name:          "bar",
				Filter:        "filter2",
				AggregationID: aggregation.DefaultID,
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
		},
		RollupRules: []RollupRule{
			{
				ID:            "uuid3",
				Name:          "car",
				CutoverMillis: 1234,
				Filter:        "filter3",
				Targets: []RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
						},
					},
				},
				LastUpdatedAtMillis: 1234,
				LastUpdatedBy:       "john",
			},
			{
				ID:            "uuid4",
				Name:          "baz",
				CutoverMillis: 4567,
				Filter:        "filter4",
				Targets: []RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:        pipeline.AggregationOpType,
								Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
							},
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
							},
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr2,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
							policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
						},
					},
				},
				LastUpdatedAtMillis: 4567,
				LastUpdatedBy:       "jane",
			},
		},
	}
	ruleset.Sort()

	expected := RuleSet{
		Namespace:     "testNamespace",
		Version:       1,
		CutoverMillis: 1234,
		MappingRules: []MappingRule{
			{
				ID:            "uuid2",
				Name:          "bar",
				Filter:        "filter2",
				AggregationID: aggregation.DefaultID,
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
			{
				ID:            "uuid1",
				Name:          "foo",
				Filter:        "filter1",
				AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
				},
			},
		},
		RollupRules: []RollupRule{
			{
				ID:            "uuid4",
				Name:          "baz",
				CutoverMillis: 4567,
				Filter:        "filter4",
				Targets: []RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type:        pipeline.AggregationOpType,
								Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
							},
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
							},
							{
								Type:   pipeline.RollupOpType,
								Rollup: rr1,
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
							policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
						},
					},
				},
				LastUpdatedAtMillis: 4567,
				LastUpdatedBy:       "jane",
			},
			{
				ID:            "uuid3",
				Name:          "car",
				CutoverMillis: 1234,
				Filter:        "filter3",
				Targets: []RollupTarget{
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
				LastUpdatedAtMillis: 1234,
				LastUpdatedBy:       "john",
			},
		},
	}
	require.Equal(t, expected, ruleset)
}

func TestRuleSetsSort(t *testing.T) {
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"name",
		[]string{"tag2", "tag1"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)
	rr2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"name",
		[]string{"tag2", "tag1"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	rulesets := RuleSets{
		"ns1": &RuleSet{
			Namespace:     "ns1",
			Version:       1,
			CutoverMillis: 1234,
			MappingRules: []MappingRule{
				{
					ID:            "uuid2",
					Name:          "ƒoo",
					Filter:        "filter2",
					AggregationID: aggregation.DefaultID,
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
					},
				},
				{
					ID:            "uuid1",
					Name:          "bar",
					Filter:        "filter1",
					AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
					},
				},
			},
		},
		"ns2": &RuleSet{
			Namespace:     "ns2",
			Version:       1,
			CutoverMillis: 1234,
			RollupRules: []RollupRule{
				{
					ID:            "uuid4",
					Name:          "cat",
					CutoverMillis: 4567,
					Filter:        "filter4",
					Targets: []RollupTarget{
						{
							Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
								{
									Type:        pipeline.AggregationOpType,
									Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
								},
								{
									Type:           pipeline.TransformationOpType,
									Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
								},
								{
									Type:   pipeline.RollupOpType,
									Rollup: rr1,
								},
							}),
							StoragePolicies: policy.StoragePolicies{
								policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
								policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
								policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
							},
						},
					},
					LastUpdatedAtMillis: 4567,
					LastUpdatedBy:       "jane",
				},
				{
					ID:            "uuid3",
					Name:          "baz",
					CutoverMillis: 1234,
					Filter:        "filter3",
					Targets: []RollupTarget{
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
					LastUpdatedAtMillis: 1234,
					LastUpdatedBy:       "john",
				},
			},
		},
	}
	rulesets.Sort()

	expected := RuleSets{
		"ns1": &RuleSet{
			Namespace:     "ns1",
			Version:       1,
			CutoverMillis: 1234,
			MappingRules: []MappingRule{
				{
					ID:            "uuid1",
					Name:          "bar",
					Filter:        "filter1",
					AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
					},
				},
				{
					ID:            "uuid2",
					Name:          "ƒoo",
					Filter:        "filter2",
					AggregationID: aggregation.DefaultID,
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
					},
				},
			},
		},
		"ns2": &RuleSet{
			Namespace:     "ns2",
			Version:       1,
			CutoverMillis: 1234,
			RollupRules: []RollupRule{
				{
					ID:            "uuid3",
					Name:          "baz",
					CutoverMillis: 1234,
					Filter:        "filter3",
					Targets: []RollupTarget{
						{
							Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
								{
									Type:   pipeline.RollupOpType,
									Rollup: rr1,
								},
							}),
							StoragePolicies: policy.StoragePolicies{
								policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
							},
						},
					},
					LastUpdatedAtMillis: 1234,
					LastUpdatedBy:       "john",
				},
				{
					ID:            "uuid4",
					Name:          "cat",
					CutoverMillis: 4567,
					Filter:        "filter4",
					Targets: []RollupTarget{
						{
							Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
								{
									Type:        pipeline.AggregationOpType,
									Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
								},
								{
									Type:           pipeline.TransformationOpType,
									Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
								},
								{
									Type:   pipeline.RollupOpType,
									Rollup: rr2,
								},
							}),
							StoragePolicies: policy.StoragePolicies{
								policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
								policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
								policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
							},
						},
					},
					LastUpdatedAtMillis: 4567,
					LastUpdatedBy:       "jane",
				},
			},
		},
	}
	require.Equal(t, expected, rulesets)
}
