// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

type testRollupTargetData struct {
	target RollupTarget
	result bool
}

type testMappingsData struct {
	id     string
	result []policy.Policy
}

type testRollupsData struct {
	id     string
	result []RollupTarget
}

func TestRollupTargetSameTransform(t *testing.T) {
	policies := []policy.Policy{
		policy.NewPolicy(10*time.Second, xtime.Second, 2*24*time.Hour),
	}
	target := RollupTarget{Name: "foo", Tags: []string{"bar1", "bar2"}}
	inputs := []testRollupTargetData{
		{
			target: RollupTarget{Name: "foo", Tags: []string{"bar1", "bar2"}, Policies: policies},
			result: true,
		},
		{
			target: RollupTarget{Name: "baz", Tags: []string{"bar1", "bar2"}},
			result: false,
		},
		{
			target: RollupTarget{Name: "foo", Tags: []string{"bar1", "bar3"}},
			result: false,
		},
	}
	for _, input := range inputs {
		require.Equal(t, input.result, target.sameTransform(input.target))
	}
}

func TestRollupTargetClone(t *testing.T) {
	policies := []policy.Policy{
		policy.NewPolicy(10*time.Second, xtime.Second, 2*24*time.Hour),
	}
	target := RollupTarget{Name: "foo", Tags: []string{"bar1", "bar2"}, Policies: policies}
	cloned := target.clone()

	// Cloned object should look exactly the same as the original one
	require.Equal(t, target, cloned)

	// Change references in the cloned object should not mutate the original object
	cloned.Tags[0] = "bar3"
	cloned.Policies[0] = policy.EmptyPolicy
	require.Equal(t, target.Tags, []string{"bar1", "bar2"})
	require.Equal(t, target.Policies, policies)
}

func testMappingRulesConfig() []*schema.MappingRule {
	return []*schema.MappingRule{
		&schema.MappingRule{
			TagFilters: map[string]string{"mtagName1": "mtagValue1"},
			Policies: []*schema.Policy{
				&schema.Policy{
					Resolution: &schema.Resolution{
						WindowSize: int64(10 * time.Second),
						Precision:  int64(time.Second),
					},
					Retention: &schema.Retention{
						Period: int64(6 * time.Hour),
					},
				},
				&schema.Policy{
					Resolution: &schema.Resolution{
						WindowSize: int64(5 * time.Minute),
						Precision:  int64(time.Minute),
					},
					Retention: &schema.Retention{
						Period: int64(48 * time.Hour),
					},
				},
				&schema.Policy{
					Resolution: &schema.Resolution{
						WindowSize: int64(10 * time.Minute),
						Precision:  int64(time.Minute),
					},
					Retention: &schema.Retention{
						Period: int64(48 * time.Hour),
					},
				},
			},
		},
		&schema.MappingRule{
			TagFilters: map[string]string{"mtagName1": "mtagValue1"},
			Policies: []*schema.Policy{
				&schema.Policy{
					Resolution: &schema.Resolution{
						WindowSize: int64(10 * time.Second),
						Precision:  int64(time.Second),
					},
					Retention: &schema.Retention{
						Period: int64(2 * time.Hour),
					},
				},
				&schema.Policy{
					Resolution: &schema.Resolution{
						WindowSize: int64(time.Minute),
						Precision:  int64(time.Minute),
					},
					Retention: &schema.Retention{
						Period: int64(time.Hour),
					},
				},
			},
		},
		&schema.MappingRule{
			TagFilters: map[string]string{"mtagName1": "mtagValue1"},
			Policies: []*schema.Policy{
				&schema.Policy{
					Resolution: &schema.Resolution{
						WindowSize: int64(10 * time.Second),
						Precision:  int64(time.Second),
					},
					Retention: &schema.Retention{
						Period: int64(12 * time.Hour),
					},
				},
				&schema.Policy{
					Resolution: &schema.Resolution{
						WindowSize: int64(time.Minute),
						Precision:  int64(time.Minute),
					},
					Retention: &schema.Retention{
						Period: int64(24 * time.Hour),
					},
				},
				&schema.Policy{
					Resolution: &schema.Resolution{
						WindowSize: int64(5 * time.Minute),
						Precision:  int64(time.Minute),
					},
					Retention: &schema.Retention{
						Period: int64(48 * time.Hour),
					},
				},
			},
		},
		&schema.MappingRule{
			TagFilters: map[string]string{"mtagName1": "mtagValue2"},
			Policies: []*schema.Policy{
				&schema.Policy{
					Resolution: &schema.Resolution{
						WindowSize: int64(10 * time.Second),
						Precision:  int64(time.Second),
					},
					Retention: &schema.Retention{
						Period: int64(24 * time.Hour),
					},
				},
			},
		},
	}
}

func testRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		{
			TagFilters: map[string]string{"rtagName1": "rtagValue1"},
			Targets: []*schema.RollupTarget{
				&schema.RollupTarget{
					Name: "rName1",
					Tags: []string{"rtag1", "rtag2"},
					Policies: []*schema.Policy{
						&schema.Policy{
							Resolution: &schema.Resolution{
								WindowSize: int64(10 * time.Second),
								Precision:  int64(time.Second),
							},
							Retention: &schema.Retention{
								Period: int64(6 * time.Hour),
							},
						},
						&schema.Policy{
							Resolution: &schema.Resolution{
								WindowSize: int64(5 * time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &schema.Retention{
								Period: int64(48 * time.Hour),
							},
						},
						&schema.Policy{
							Resolution: &schema.Resolution{
								WindowSize: int64(10 * time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &schema.Retention{
								Period: int64(48 * time.Hour),
							},
						},
					},
				},
			},
		},
		{
			TagFilters: map[string]string{"rtagName1": "rtagValue1"},
			Targets: []*schema.RollupTarget{
				&schema.RollupTarget{
					Name: "rName1",
					Tags: []string{"rtag1", "rtag2"},
					Policies: []*schema.Policy{
						&schema.Policy{
							Resolution: &schema.Resolution{
								WindowSize: int64(10 * time.Second),
								Precision:  int64(time.Second),
							},
							Retention: &schema.Retention{
								Period: int64(2 * time.Hour),
							},
						},
						&schema.Policy{
							Resolution: &schema.Resolution{
								WindowSize: int64(time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &schema.Retention{
								Period: int64(time.Hour),
							},
						},
					},
				},
			},
		},
		{
			TagFilters: map[string]string{"rtagName1": "rtagValue1"},
			Targets: []*schema.RollupTarget{
				&schema.RollupTarget{
					Name: "rName1",
					Tags: []string{"rtag1", "rtag2"},
					Policies: []*schema.Policy{
						&schema.Policy{
							Resolution: &schema.Resolution{
								WindowSize: int64(10 * time.Second),
								Precision:  int64(time.Second),
							},
							Retention: &schema.Retention{
								Period: int64(12 * time.Hour),
							},
						},
						&schema.Policy{
							Resolution: &schema.Resolution{
								WindowSize: int64(time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &schema.Retention{
								Period: int64(24 * time.Hour),
							},
						},
						&schema.Policy{
							Resolution: &schema.Resolution{
								WindowSize: int64(5 * time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &schema.Retention{
								Period: int64(48 * time.Hour),
							},
						},
					},
				},
				&schema.RollupTarget{
					Name: "rName2",
					Tags: []string{"rtag1"},
					Policies: []*schema.Policy{
						&schema.Policy{
							Resolution: &schema.Resolution{
								WindowSize: int64(10 * time.Second),
								Precision:  int64(time.Second),
							},
							Retention: &schema.Retention{
								Period: int64(24 * time.Hour),
							},
						},
					},
				},
			},
		},
		{
			TagFilters: map[string]string{"rtagName1": "rtagValue2"},
			Targets: []*schema.RollupTarget{
				&schema.RollupTarget{
					Name: "rName3",
					Tags: []string{"rtag1", "rtag2"},
					Policies: []*schema.Policy{
						&schema.Policy{
							Resolution: &schema.Resolution{
								WindowSize: int64(time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &schema.Retention{
								Period: int64(time.Hour),
							},
						},
					},
				},
			},
		},
	}
}

func TestRuleSetMatchMappingRules(t *testing.T) {
	ruleSetConfig := &schema.RuleSet{
		Version:      1,
		Cutover:      time.Now().UnixNano(),
		MappingRules: testMappingRulesConfig(),
	}
	ruleSet, err := NewRuleSet(ruleSetConfig, filters.NewMockSortedTagIterator)
	require.NoError(t, err)

	inputs := []testMappingsData{
		{
			id: "mtagName1=mtagValue1",
			result: []policy.Policy{
				policy.NewPolicy(10*time.Second, xtime.Second, 12*time.Hour),
				policy.NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
				policy.NewPolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
			},
		},
		{
			id: "mtagName1=mtagValue2",
			result: []policy.Policy{
				policy.NewPolicy(10*time.Second, xtime.Second, 24*time.Hour),
			},
		},
	}
	for _, input := range inputs {
		res := ruleSet.Match(input.id)
		require.Equal(t, ruleSet.Version(), res.Version)
		require.Equal(t, ruleSet.Cutover(), res.Cutover)
		require.Equal(t, input.result, res.Mappings)
	}
}

func TestRuleSetMatchRollupRules(t *testing.T) {
	ruleSetConfig := &schema.RuleSet{
		RollupRules: testRollupRulesConfig(),
	}
	ruleSet, err := NewRuleSet(ruleSetConfig, filters.NewMockSortedTagIterator)
	require.NoError(t, err)

	inputs := []testRollupsData{
		{
			id: "rtagName1=rtagValue1",
			result: []RollupTarget{
				{
					Name: "rName1",
					Tags: []string{"rtag1", "rtag2"},
					Policies: []policy.Policy{
						policy.NewPolicy(10*time.Second, xtime.Second, 12*time.Hour),
						policy.NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
						policy.NewPolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
					},
				},
				{
					Name: "rName2",
					Tags: []string{"rtag1"},
					Policies: []policy.Policy{
						policy.NewPolicy(10*time.Second, xtime.Second, 24*time.Hour),
					},
				},
			},
		},
		{
			id: "rtagName1=rtagValue2",
			result: []RollupTarget{
				{
					Name: "rName3",
					Tags: []string{"rtag1", "rtag2"},
					Policies: []policy.Policy{
						policy.NewPolicy(time.Minute, xtime.Minute, time.Hour),
					},
				},
			},
		},
	}
	for _, input := range inputs {
		res := ruleSet.Match(input.id)
		require.Equal(t, ruleSet.Version(), res.Version)
		require.Equal(t, ruleSet.Cutover(), res.Cutover)
		require.Equal(t, input.result, res.Rollups)
	}
}

func TestTombstonedRuleSetMatch(t *testing.T) {
	ruleSetConfig := &schema.RuleSet{
		Version:      1,
		Cutover:      time.Now().UnixNano(),
		Tombstoned:   true,
		MappingRules: testMappingRulesConfig(),
		RollupRules:  testRollupRulesConfig(),
	}
	ruleSet, err := NewRuleSet(ruleSetConfig, filters.NewMockSortedTagIterator)
	require.NoError(t, err)

	expected := MatchResult{
		Version: ruleSet.Version(),
		Cutover: ruleSet.Cutover(),
	}
	id := "rtagName1=rtagValue1"
	require.Equal(t, expected, ruleSet.Match(id))
}
