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
	"encoding/json"
	"testing"

	"github.com/m3db/m3metrics/policy"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestToRuleSetSnapshotView(t *testing.T) {
	mappingRules := []MappingRule{
		*testMappingRule("mr1_id", "mr1"),
		*testMappingRule("mr2_id", "mr2"),
	}
	rollupRules := []RollupRule{
		*testRollupRule("rr1_id", "rr1", []RollupTarget{*testRollupTarget("target1")}),
		*testRollupRule("rr2_id", "rr2", []RollupTarget{*testRollupTarget("target2")}),
	}
	fixture := testRuleSet("rs_ns", mappingRules, rollupRules)
	expected := &RuleSetSnapshotView{
		Namespace:    "rs_ns",
		Version:      1,
		CutoverNanos: 0,
		MappingRules: map[string]*MappingRuleView{
			"mr1_id": {
				ID:       "mr1_id",
				Name:     "mr1",
				Filter:   "filter",
				Policies: []policy.Policy{},
			},
			"mr2_id": {
				ID:       "mr2_id",
				Name:     "mr2",
				Filter:   "filter",
				Policies: []policy.Policy{},
			},
		},
		RollupRules: map[string]*RollupRuleView{
			"rr1_id": {
				ID:     "rr1_id",
				Name:   "rr1",
				Filter: "filter",
				Targets: []RollupTargetView{
					{
						Name:     "target1",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
			},
			"rr2_id": {
				ID:     "rr2_id",
				Name:   "rr2",
				Filter: "filter",
				Targets: []RollupTargetView{
					{
						Name:     "target2",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
			},
		},
	}
	actual, err := fixture.ToRuleSetSnapshotView(GenerateID)
	require.NoError(t, err)
	require.EqualValues(t, expected, actual)

	rs := NewRuleSet(expected)
	rs.Sort()
	fixture.Sort()
	require.Equal(t, rs, *fixture)
}

func TestToRuleSetSnapshotViewGenerateMissingID(t *testing.T) {
	mappingRules := []MappingRule{
		*testMappingRule("", "mr"),
		*testMappingRule("", "mr"),
	}
	rollupRules := []RollupRule{
		*testRollupRule("", "rr", []RollupTarget{*testRollupTarget("target")}),
		*testRollupRule("", "rr", []RollupTarget{*testRollupTarget("target")}),
	}
	fixture := testRuleSet("namespace", mappingRules, rollupRules)

	actual, err := fixture.ToRuleSetSnapshotView(GenerateID)
	require.NoError(t, err)
	mrIDs := []string{}
	rrIDs := []string{}

	// Test that generated IDs are UUIDs and add them to their respective lists for further testing.
	for id := range actual.MappingRules {
		require.NotNil(t, uuid.Parse(id))
		mrIDs = append(mrIDs, id)
	}
	for id := range actual.RollupRules {
		require.NotNil(t, uuid.Parse(id))
		rrIDs = append(rrIDs, id)
	}

	expected := &RuleSetSnapshotView{
		Namespace:    "namespace",
		Version:      1,
		CutoverNanos: 0,
		MappingRules: map[string]*MappingRuleView{
			mrIDs[0]: {
				ID:       mrIDs[0],
				Name:     "mr",
				Filter:   "filter",
				Policies: []policy.Policy{},
			},
			mrIDs[1]: {
				ID:       mrIDs[1],
				Name:     "mr",
				Filter:   "filter",
				Policies: []policy.Policy{},
			},
		},
		RollupRules: map[string]*RollupRuleView{
			rrIDs[0]: {
				ID:     rrIDs[0],
				Name:   "rr",
				Filter: "filter",
				Targets: []RollupTargetView{
					{
						Name:     "target",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
			},
			rrIDs[1]: {
				ID:     rrIDs[1],
				Name:   "rr",
				Filter: "filter",
				Targets: []RollupTargetView{
					{
						Name:     "target",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
			},
		},
	}
	require.EqualValues(t, expected, actual)
}

func TestToRuleSetSnapshotViewFailMissingMappingRuleID(t *testing.T) {
	mappingRules := []MappingRule{
		*testMappingRule("", "mr"),
		*testMappingRule("id1", "mr"),
	}
	rollupRules := []RollupRule{
		*testRollupRule("id2", "rr", []RollupTarget{*testRollupTarget("target")}),
		*testRollupRule("id3", "rr", []RollupTarget{*testRollupTarget("target")}),
	}
	fixture := testRuleSet("namespace", mappingRules, rollupRules)

	_, err := fixture.ToRuleSetSnapshotView(DontGenerateID)
	require.Error(t, err)
}

func TestToRuleSetSnapshotViewFailMissingRollupRuleID(t *testing.T) {
	mappingRules := []MappingRule{
		*testMappingRule("id1", "mr"),
		*testMappingRule("id2", "mr"),
	}
	rollupRules := []RollupRule{
		*testRollupRule("id3", "rr", []RollupTarget{*testRollupTarget("target")}),
		*testRollupRule("", "rr", []RollupTarget{*testRollupTarget("target")}),
	}
	fixture := testRuleSet("namespace", mappingRules, rollupRules)

	_, err := fixture.ToRuleSetSnapshotView(DontGenerateID)
	require.Error(t, err)
}

func TestRuleSetsSort(t *testing.T) {
	ruleset := `
		{
			"id": "namespace",
			"mappingRules":[
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_1",
					"policies":[
						"10s:2d|Count,P99,P9999",
						"1m:40d|Count,P99,P9999",
						"1m:40d|Count,P9999"
					]
				}
			],
			"rollupRules":[
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_1",
					"targets":[
						{
							"name": "rollup_target_1",
							"tags": [
								"tag1","tag2","tag3"
							],
							"policies":[
								"10s:2d|Count,P99,P9999",
								"1m:40d|Count,P99,P9999",
								"1m:40d|Count,P9999"
							]
						},
						{
							"name": "rollup_target_2",
							"tags": [
								"tag1","tag2","tag3"
							],
							"policies":[
								"1m:40d|Count,P99,P9999",
								"1m:40d|Count,P9999",
								"10s:2d|Count,P99,P9999"
							]
						}
					]
				}
			]
		}
	`
	var rs1 RuleSet
	err := json.Unmarshal([]byte(ruleset), &rs1)
	require.NoError(t, err)
	var rs2 RuleSet
	err = json.Unmarshal([]byte(ruleset), &rs2)
	require.NoError(t, err)

	rulesets := RuleSets(map[string]*RuleSet{"rs1": &rs1, "rs2": &rs2})

	expectedBlob := `
		{
			"id": "namespace",
			"mappingRules":[
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_1",
					"policies":[
						"10s:2d|Count,P99,P9999",
						"1m:40d|Count,P9999",
						"1m:40d|Count,P99,P9999"
					]
				}
			],
			"rollupRules":[
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_1",
					"targets":[
						{
							"name": "rollup_target_1",
							"tags": [
								"tag1","tag2","tag3"
							],
							"policies":[
								"10s:2d|Count,P99,P9999",
								"1m:40d|Count,P9999",
								"1m:40d|Count,P99,P9999"
							]
						},
						{
							"name": "rollup_target_2",
							"tags": [
								"tag1","tag2","tag3"
							],
							"policies":[
								"10s:2d|Count,P99,P9999",
								"1m:40d|Count,P9999",
								"1m:40d|Count,P99,P9999"
							]
						}
					]
				}
			]
		}
	`

	var expected RuleSet
	err = json.Unmarshal([]byte(expectedBlob), &expected)
	require.NoError(t, err)

	rulesets.Sort()

	require.Equal(t, expected, *rulesets["rs1"])
	require.Equal(t, expected, *rulesets["rs2"])
}

func TestRuleSetSort(t *testing.T) {
	ruleset := `
		{
			"id": "namespace",
			"mappingRules":[
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_1",
					"policies":[
						"10s:2d|Count,P99,P9999",
						"1m:40d|Count,P99,P9999",
						"1m:40d|Count,P9999"
					]
				}
			],
			"rollupRules":[
				{
					"name":"sample_rollup_rule_1",
					"filter":"filter_1",
					"targets":[
						{
							"name": "rollup_target_1",
							"tags": [
								"tag1","tag2","tag3"
							],
							"policies":[
								"10s:2d|Count,P99,P9999",
								"1m:40d|Count,P99,P9999",
								"1m:40d|Count,P9999"
							]
						},
						{
							"name": "rollup_target_2",
							"tags": [
								"tag1","tag2","tag3"
							],
							"policies":[
								"1m:40d|Count,P99,P9999",
								"1m:40d|Count,P9999",
								"10s:2d|Count,P99,P9999"
							]
						}
					]
				}
			]
		}
	`

	expectedRuleSet := `
	{
		"id": "namespace",
		"mappingRules":[
			{
				"name":"sample_mapping_rule_1",
				"filter":"filter_1",
				"policies":[
					"10s:2d|Count,P99,P9999",
					"1m:40d|Count,P9999",
					"1m:40d|Count,P99,P9999"
				]
			}
		],
		"rollupRules":[
			{
				"name":"sample_rollup_rule_1",
				"filter":"filter_1",
				"targets":[
					{
						"name": "rollup_target_1",
						"tags": [
							"tag1","tag2","tag3"
						],
						"policies":[
							"10s:2d|Count,P99,P9999",
							"1m:40d|Count,P9999",
							"1m:40d|Count,P99,P9999"
						]
					},
					{
						"name": "rollup_target_2",
						"tags": [
							"tag1","tag2","tag3"
						],
						"policies":[
							"10s:2d|Count,P99,P9999",
							"1m:40d|Count,P9999",
							"1m:40d|Count,P99,P9999"
						]
					}
				]
			}
		]
	}
`
	var rs RuleSet
	err := json.Unmarshal([]byte(ruleset), &rs)
	require.NoError(t, err)
	var expected RuleSet
	err = json.Unmarshal([]byte(expectedRuleSet), &expected)
	require.NoError(t, err)

	rs.Sort()
	require.Equal(t, expected, rs)
}

func TestRuleSetSortByRollupRuleNameAsc(t *testing.T) {
	ruleset := `
		{
			"id": "namespace",
			"rollupRules":[
				{
					"name":"sample_rollup_rule_2",
					"filter":"filter_1",
					"targets":[]
				},
				{
					"name":"sample_rollup_rule_1",
					"filter":"filter_2",
					"targets":[]
				}
			]
		}
	`

	expectedRuleSet := `
		{
			"id": "namespace",
			"rollupRules":[
				{
					"name":"sample_rollup_rule_1",
					"filter":"filter_2",
					"targets":[]
				},
				{
					"name":"sample_rollup_rule_2",
					"filter":"filter_1",
					"targets":[]
				}
			]
		}
	`
	var rs RuleSet
	err := json.Unmarshal([]byte(ruleset), &rs)
	require.NoError(t, err)
	var expected RuleSet
	err = json.Unmarshal([]byte(expectedRuleSet), &expected)
	require.NoError(t, err)

	rs.Sort()
	require.Equal(t, expected, rs)
}

func TestRuleSetSortByMappingNameAsc(t *testing.T) {
	ruleset := `
		{
			"id": "namespace",
			"mappingRules":[
				{
					"name":"sample_mapping_rule_2",
					"filter":"filter_1",
					"policies":[]
				},
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_2",
					"policies":[]
				}
			]
		}
	`

	expectedRuleSet := `
		{
			"id": "namespace",
			"mappingRules":[
				{
					"name":"sample_mapping_rule_1",
					"filter":"filter_2",
					"policies":[]
				},
				{
					"name":"sample_mapping_rule_2",
					"filter":"filter_1",
					"policies":[]
				}
			]
		}
	`
	var rs RuleSet
	err := json.Unmarshal([]byte(ruleset), &rs)
	require.NoError(t, err)
	var expected RuleSet
	err = json.Unmarshal([]byte(expectedRuleSet), &expected)
	require.NoError(t, err)

	rs.Sort()
	require.Equal(t, expected, rs)
}

func testRuleSet(namespace string, mappingRules []MappingRule,
	rollupRules []RollupRule) *RuleSet {
	return &RuleSet{
		Namespace:     namespace,
		Version:       1,
		CutoverMillis: 0,
		MappingRules:  mappingRules,
		RollupRules:   rollupRules,
	}
}
