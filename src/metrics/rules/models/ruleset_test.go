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

package models

import (
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/pipeline"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/transformation"
	xtime "github.com/m3db/m3x/time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestRuleSetToToRuleSetSnapshotViewDontGenerateMissingID(t *testing.T) {
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
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       []byte("name"),
									Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
									AggregationID: aggregation.DefaultID,
								},
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
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       []byte("name"),
									Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
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
				},
				LastUpdatedAtMillis: 4567,
				LastUpdatedBy:       "jane",
			},
		},
	}
	res, err := ruleset.ToRuleSetSnapshotView(DontGenerateID)
	require.NoError(t, err)

	expected := &RuleSetSnapshotView{
		Namespace:    "testNamespace",
		Version:      1,
		CutoverNanos: 1234000000,
		MappingRules: map[string]*MappingRuleView{
			"uuid1": &MappingRuleView{
				ID:            "uuid1",
				Name:          "foo",
				Filter:        "filter1",
				AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
				},
			},
			"uuid2": &MappingRuleView{
				ID:            "uuid2",
				Name:          "bar",
				Filter:        "filter2",
				AggregationID: aggregation.DefaultID,
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
				},
			},
		},
		RollupRules: map[string]*RollupRuleView{
			"uuid3": &RollupRuleView{
				ID:           "uuid3",
				Name:         "car",
				CutoverNanos: 1234000000,
				Filter:       "filter3",
				Targets: []RollupTargetView{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       []byte("name"),
									Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
									AggregationID: aggregation.DefaultID,
								},
							},
						}),
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
						},
					},
				},
				LastUpdatedAtNanos: 1234000000,
				LastUpdatedBy:      "john",
			},
			"uuid4": &RollupRuleView{
				ID:           "uuid4",
				Name:         "baz",
				CutoverNanos: 4567000000,
				Filter:       "filter4",
				Targets: []RollupTargetView{
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
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       []byte("name"),
									Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
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
				},
				LastUpdatedAtNanos: 4567000000,
				LastUpdatedBy:      "jane",
			},
		},
	}
	require.Equal(t, expected, res)
}

func TestRuleSetToRuleSetSnapshotViewNoMappingRuleIDDontGenerateMissingID(t *testing.T) {
	ruleset := RuleSet{
		Namespace:     "testNamespace",
		Version:       1,
		CutoverMillis: 1234,
		MappingRules: []MappingRule{
			{
				Name:          "foo",
				Filter:        "filter1",
				AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
				},
			},
		},
	}
	_, err := ruleset.ToRuleSetSnapshotView(DontGenerateID)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "no mapping rule id"))
}

func TestRuleSetToRuleSetSnapshotViewNoRollupRuleIDDontGenerateMissingID(t *testing.T) {
	ruleset := RuleSet{
		Namespace:     "testNamespace",
		Version:       1,
		CutoverMillis: 1234,
		MappingRules: []MappingRule{
			{
				ID:            uuid.New(),
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
				Name:          "car",
				CutoverMillis: 1234,
				Filter:        "filter3",
				Targets: []RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       []byte("name"),
									Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
									AggregationID: aggregation.DefaultID,
								},
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
	_, err := ruleset.ToRuleSetSnapshotView(DontGenerateID)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "no rollup rule id"))
}

func TestRuleSetToRuleSetSnapshotViewNoRuleIDGenerateMissingID(t *testing.T) {
	ruleset := RuleSet{
		Namespace:     "testNamespace",
		Version:       1,
		CutoverMillis: 1234,
		RollupRules: []RollupRule{
			{
				Name:          "car",
				CutoverMillis: 1234,
				Filter:        "filter3",
				Targets: []RollupTarget{
					{
						Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       []byte("name"),
									Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
									AggregationID: aggregation.DefaultID,
								},
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
	res, err := ruleset.ToRuleSetSnapshotView(GenerateID)
	require.NoError(t, err)

	// Assert that the rule IDs have been generated.
	for id := range res.MappingRules {
		require.NotNil(t, uuid.Parse(id))
	}
	for id := range res.RollupRules {
		require.NotNil(t, uuid.Parse(id))
	}
}

func TestRuleSetSort(t *testing.T) {
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
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       []byte("name"),
									Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
									AggregationID: aggregation.DefaultID,
								},
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
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       []byte("name"),
									Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
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
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       []byte("name"),
									Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
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
								Type: pipeline.RollupOpType,
								Rollup: pipeline.RollupOp{
									NewName:       []byte("name"),
									Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
									AggregationID: aggregation.DefaultID,
								},
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
									Type: pipeline.RollupOpType,
									Rollup: pipeline.RollupOp{
										NewName:       []byte("name"),
										Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
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
									Type: pipeline.RollupOpType,
									Rollup: pipeline.RollupOp{
										NewName:       []byte("name"),
										Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
										AggregationID: aggregation.DefaultID,
									},
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
									Type: pipeline.RollupOpType,
									Rollup: pipeline.RollupOp{
										NewName:       []byte("name"),
										Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
										AggregationID: aggregation.DefaultID,
									},
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
									Type: pipeline.RollupOpType,
									Rollup: pipeline.RollupOp{
										NewName:       []byte("name"),
										Tags:          [][]byte{[]byte("tag2"), []byte("tag1")},
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
					},
					LastUpdatedAtMillis: 4567,
					LastUpdatedBy:       "jane",
				},
			},
		},
	}
	require.Equal(t, expected, rulesets)
}
