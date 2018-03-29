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
	"time"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestToRollupTargetView(t *testing.T) {
	fixture := testRollupTarget("name")
	expected := RollupTargetView{
		Name:     "name",
		Tags:     []string{"tag"},
		Policies: []policy.Policy{},
	}
	require.EqualValues(t, expected, fixture.ToRollupTargetView())
}

func TestNewRollupTarget(t *testing.T) {
	fixture := testRollupTargetView("name")
	expected := RollupTarget{
		Name:     "name",
		Tags:     []string{"tag"},
		Policies: []policy.Policy{},
	}
	require.EqualValues(t, expected, NewRollupTarget(*fixture))
}

func TestNewRollupRule(t *testing.T) {
	targets := []RollupTargetView{
		*testRollupTargetView("target1"),
		*testRollupTargetView("target2"),
	}
	fixture := testRollupRuleView("rr_id", "rr_name", targets)
	expected := RollupRule{
		ID:     "rr_id",
		Name:   "rr_name",
		Filter: "filter",
		Targets: []RollupTarget{
			{
				Name:     "target1",
				Tags:     []string{"tag"},
				Policies: []policy.Policy{},
			},
			{
				Name:     "target2",
				Tags:     []string{"tag"},
				Policies: []policy.Policy{},
			},
		},
		CutoverMillis:       0,
		LastUpdatedBy:       "",
		LastUpdatedAtMillis: 0,
	}
	require.EqualValues(t, expected, NewRollupRule(fixture))
}

func TestToRollupRuleView(t *testing.T) {
	targets := []RollupTarget{
		*testRollupTarget("target1"),
		*testRollupTarget("target2"),
	}
	fixture := testRollupRule("id", "name", targets)
	expected := &RollupRuleView{
		ID:     "id",
		Name:   "name",
		Filter: "filter",
		Targets: []RollupTargetView{
			{
				Name:     "target1",
				Tags:     []string{"tag"},
				Policies: []policy.Policy{},
			},
			{
				Name:     "target2",
				Tags:     []string{"tag"},
				Policies: []policy.Policy{},
			},
		},
	}
	require.EqualValues(t, expected, fixture.ToRollupRuleView())
}

func TestRollupRuleSort(t *testing.T) {
	rollupRule := `
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
					"name": "rollup_target_1",
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
	`
	var rr RollupRule
	err := json.Unmarshal([]byte(rollupRule), &rr)
	require.NoError(t, err)

	expected := `["10s:2d|Count,P99,P9999","1m:40d|Count,P9999","1m:40d|Count,P99,P9999"]`
	rr.Sort()
	targetPolicy1 := rr.Targets[0].Policies
	targetPolicy2 := rr.Targets[1].Policies
	actual1, err1 := json.Marshal(targetPolicy1)
	require.NoError(t, err1)
	actual2, err2 := json.Marshal(targetPolicy2)
	require.NoError(t, err2)

	require.Equal(t, expected, string(actual1))
	require.Equal(t, expected, string(actual2))
}

func TestRollupRuleSortTargetByNameAsc(t *testing.T) {
	rollupRule := `
		{
			"name":"sample_mapping_rule_1",
			"filter":"filter_1",
			"targets":[
				{
					"name": "rollup_target_2",
					"tags": [
						"tag1","tag2","tag3"
					],
					"policies":[]
				},
				{
					"name": "rollup_target_1",
					"tags": [
						"tag1","tag3"
					],
					"policies":[]
				}
			]
		}
	`
	var rr RollupRule
	err := json.Unmarshal([]byte(rollupRule), &rr)
	require.NoError(t, err)

	expectedJSON := `
		{
			"name":"sample_mapping_rule_1",
			"filter":"filter_1",
			"targets":[
				{
					"name": "rollup_target_1",
					"tags": [
						"tag1","tag3"
					],
					"policies":[]
				},
				{
					"name": "rollup_target_2",
					"tags": [
						"tag1","tag2","tag3"
					],
					"policies":[]
				}
			]
		}
	`

	var expected RollupRule
	err = json.Unmarshal([]byte(expectedJSON), &expected)
	require.NoError(t, err)

	rr.Sort()

	require.Equal(t, expected, rr)
}

func TestRollupRuleEqual(t *testing.T) {
	rrJSON := `
		{
			"name":"rollup_rule_1",
			"filter":"filter1",
			"targets":[
				{
					"name": "rollup_target_1",
					"tags": [
						"tag1","tag2","tag3"
					],
					"policies":[
						"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
					]
				}
			]
		}
	`

	var rr1 RollupRule
	err := json.Unmarshal([]byte(rrJSON), &rr1)
	require.NoError(t, err)
	var rr2 RollupRule
	err = json.Unmarshal([]byte(rrJSON), &rr2)
	require.NoError(t, err)

	require.True(t, rr1.Equals(&rr2))
}

func TestRollupRuleNotEqual(t *testing.T) {
	rrJSON1 := `
		{
			"name":"rollup_rule_1",
			"filter":"filter1",
			"targets":[
				{
					"name": "rollup_target_1",
					"tags": [
						"tag1","tag2","tag3"
					],
					"policies":[
						"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
					]
				}
			]
		}
	`
	rrJSON2 := `
		{
			"name":"rollup_rule_1",
			"filter":"filter2",
			"targets":[
				{
					"name": "rollup_target_1",
					"tags": [
						"tag1","tag2","tag3"
					],
					"policies":[
						"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
					]
				}
			]
		}
	`
	rrJSON3 := `
		{
			"name":"rollup_rule_1",
			"filter":"filter1",
			"targets":[
				{
					"name": "rollup_target_1",
					"tags": [
						"tag1","tag2"
					],
					"policies":[
						"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
					]
				}
			]
		}
	`

	var rr1 RollupRule
	err := json.Unmarshal([]byte(rrJSON1), &rr1)
	require.NoError(t, err)
	var rr2 RollupRule
	err = json.Unmarshal([]byte(rrJSON2), &rr2)
	require.NoError(t, err)
	var rr3 RollupRule
	err = json.Unmarshal([]byte(rrJSON3), &rr3)
	require.NoError(t, err)

	require.False(t, rr1.Equals(&rr2))
	require.False(t, rr1.Equals(&rr3))
	require.False(t, rr2.Equals(&rr3))
}

func TestRollupRuleNilCases(t *testing.T) {
	var rr1 *RollupRule

	require.True(t, rr1.Equals(nil))

	var rr2 RollupRule
	rollupRule := &rr2
	require.False(t, rollupRule.Equals(rr1))
}

func TestRollupTargetsEqual(t *testing.T) {
	rtJSON := `
		[
			{
				"name": "rollup_target_1",
				"tags": [
					"tag1","tag2","tag3"
				],
				"policies":[
					"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
				]
			},
			{
				"name": "rollup_target_2",
				"tags": [
					"tag1","tag2"
				],
				"policies":[
					"10s:2d|Count,P99,P9999"
				]
			}
		]
	`
	var rt1 []RollupTarget
	err := json.Unmarshal([]byte(rtJSON), &rt1)
	require.NoError(t, err)
	var rt2 []RollupTarget
	err = json.Unmarshal([]byte(rtJSON), &rt2)
	require.NoError(t, err)

	require.True(t, rollupTargets(rt1).Equals(rollupTargets(rt2)))
}

func TestRollupTargetsNotEqual(t *testing.T) {
	rtJSON1 := `
		[
			{
				"name": "rollup_target_1",
				"tags": [
					"tag1","tag2","tag3"
				],
				"policies":[
					"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
				]
			},
			{
				"name": "rollup_target_2",
				"tags": [
					"tag1","tag2"
				],
				"policies":[
					"10s:2d|Count,P99,P9999"
				]
			}
		]
	`
	rtJSON2 := `
		[
			{
				"name": "rollup_target_2",
				"tags": [
					"tag1","tag2","tag4"
				],
				"policies":[
					"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
				]
			},
			{
				"name": "rollup_target_2",
				"tags": [
					"tag1","tag2"
				],
				"policies":[
					"10s:2d|Count,P99,P9999"
				]
			}
		]
	`
	var rt1 []RollupTarget
	err := json.Unmarshal([]byte(rtJSON1), &rt1)
	require.NoError(t, err)
	var rt2 []RollupTarget
	err = json.Unmarshal([]byte(rtJSON2), &rt2)
	require.NoError(t, err)

	require.False(t, rollupTargets(rt1).Equals(rollupTargets(rt2)))
}

func TestRollupTargetEqual(t *testing.T) {
	rtJSON1 := `
		{
			"name": "rollup_target_1",
			"tags": [
				"tag1","tag2","tag3"
			],
			"policies":[
				"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
			]
		}
	`
	rtJSON2 := `
		{
			"name": "rollup_target_1",
			"tags": [
				"tag1","tag2","tag3"
			],
			"policies":[
				"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
			]
		}
	`
	var rt1 RollupTarget
	err := json.Unmarshal([]byte(rtJSON1), &rt1)
	require.NoError(t, err)
	var rt2 RollupTarget
	err = json.Unmarshal([]byte(rtJSON2), &rt2)
	require.NoError(t, err)

	require.True(t, rt1.Equals(&rt2))
}

func TestRollupTargetNotEqual(t *testing.T) {
	rtJSONs := []string{
		`
		{
			"name": "rollup_target_1",
			"tags": [
				"tag1","tag3","tag2"
			],
			"policies":[
				"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
			]
		}
	`,
		`
		{
			"name": "rollup_target_2",
			"tags": [
				"tag1","tag3","tag2"
			],
			"policies":[
				"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
			]
		}
	`,
		`
		{
			"name": "rollup_target_1",
			"tags": [
				"tag1","tag2","tag3"
			],
			"policies":[
				"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
			]
		}
	`,
		`
		{
			"name": "rollup_target_1",
			"tags": [
				"tag1","tag2","tag3"
			],
			"policies":[
				"1m:40d|Count,P99,P9999","10s:2d|Count,P99,P9999"
			]
		}
	`,
		`
		{
			"name": "rollup_target_1",
			"tags": [
				"tag1","tag2"
			],
			"policies":[
				"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
			]
		}
	`,
	}

	targets := make([]RollupTarget, len(rtJSONs))
	for i, rtJSON := range rtJSONs {
		require.NoError(t, json.Unmarshal([]byte(rtJSON), &targets[i]))
	}

	for i := 1; i < len(targets); i++ {
		require.False(t, targets[0].Equals(&targets[i]))
	}
}

func TestRollupTargetSort(t *testing.T) {
	rtJSON1 := `
		{
			"name": "rollup_target_1",
			"tags": [
				"tag1","tag2","tag3"
			],
			"policies":[
				"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
			]
		}
	`
	rtJSON2 := `
		{
			"name": "rollup_target_1",
			"tags": [
				"tag1","tag2","tag3"
			],
			"policies":[
				"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
			]
		}
	`
	var rt1 RollupTarget
	err := json.Unmarshal([]byte(rtJSON1), &rt1)
	require.NoError(t, err)
	var rt2 RollupTarget
	err = json.Unmarshal([]byte(rtJSON2), &rt2)
	require.NoError(t, err)

	require.True(t, rt1.Equals(&rt2))
}

func TestRollupTargetNilCases(t *testing.T) {
	var rt1 *RollupTarget

	require.True(t, rt1.Equals(nil))

	var rt2 RollupTarget
	rollupTarget := &rt2
	require.False(t, rollupTarget.Equals(rt1))
}

func TestRollupTargetSameTransform(t *testing.T) {
	policies := []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*24*time.Hour), aggregation.DefaultID),
	}
	target := RollupTargetView{Name: "foo", Tags: []string{"bar1", "bar2"}}
	inputs := []struct {
		target RollupTargetView
		result bool
	}{
		{
			target: RollupTargetView{Name: "foo", Tags: []string{"bar1", "bar2"}, Policies: policies},
			result: true,
		},
		{
			target: RollupTargetView{Name: "foo", Tags: []string{"bar2", "bar1"}, Policies: policies},
			result: true,
		},
		{
			target: RollupTargetView{Name: "foo", Tags: []string{"bar1"}},
			result: false,
		},
		{
			target: RollupTargetView{Name: "foo", Tags: []string{"bar1", "bar2", "bar3"}},
			result: false,
		},
		{
			target: RollupTargetView{Name: "foo", Tags: []string{"bar1", "bar3"}},
			result: false,
		},
		{
			target: RollupTargetView{Name: "baz", Tags: []string{"bar1", "bar2"}},
			result: false,
		},
		{
			target: RollupTargetView{Name: "baz", Tags: []string{"bar2", "bar1"}},
			result: false,
		},
	}
	for _, input := range inputs {
		require.Equal(t, input.result, target.SameTransform(input.target))
	}
}

func TestNewRollupRuleHistoryJSON(t *testing.T) {
	id := "id"
	targets := []RollupTargetView{
		*testRollupTargetView("target1"),
		*testRollupTargetView("target2"),
	}
	hist := []*RollupRuleView{
		testRollupRuleView(id, "name1", targets),
		testRollupRuleView(id, "name2", targets),
	}
	expected := RollupRuleSnapshots{
		RollupRules: []RollupRule{
			{
				ID:     id,
				Name:   "name1",
				Filter: "filter",
				Targets: []RollupTarget{
					{
						Name:     "target1",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
					{
						Name:     "target2",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
				CutoverMillis:       0,
				LastUpdatedBy:       "",
				LastUpdatedAtMillis: 0,
			},
			{
				ID:     id,
				Name:   "name2",
				Filter: "filter",
				Targets: []RollupTarget{
					{
						Name:     "target1",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
					{
						Name:     "target2",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
				CutoverMillis:       0,
				LastUpdatedBy:       "",
				LastUpdatedAtMillis: 0,
			},
		},
	}
	require.EqualValues(t, expected, NewRollupRuleSnapshots(hist))
}

func testRollupTarget(name string) *RollupTarget {
	return &RollupTarget{
		Name:     name,
		Tags:     []string{"tag"},
		Policies: []policy.Policy{},
	}
}

func testRollupTargetView(name string) *RollupTargetView {
	return &RollupTargetView{
		Name:     name,
		Tags:     []string{"tag"},
		Policies: []policy.Policy{},
	}
}

func testRollupRule(id, name string, targets []RollupTarget) *RollupRule {
	return &RollupRule{
		ID:      id,
		Name:    name,
		Filter:  "filter",
		Targets: targets,
	}
}

// nolint:unparam
func testRollupRuleView(id, name string, targets []RollupTargetView) *RollupRuleView {
	return &RollupRuleView{
		ID:      id,
		Name:    name,
		Filter:  "filter",
		Targets: targets,
	}
}
