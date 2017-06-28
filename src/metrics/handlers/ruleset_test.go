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

package handlers

import (
	"testing"
	"time"

	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/stretchr/testify/require"
)

const (
	testKeyFmt    = "rules/%s"
	testNamespace = "ns"
)

var (
	testRuleSet = &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    true,
		CutoverTime:   34923,
		MappingRules: []*schema.MappingRule{
			&schema.MappingRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*schema.MappingRuleSnapshot{
					&schema.MappingRuleSnapshot{
						Name:        "foo",
						Tombstoned:  false,
						CutoverTime: 12345,
						TagFilters: map[string]string{
							"tag1": "value1",
							"tag2": "value2",
						},
						Policies: []*schema.Policy{
							&schema.Policy{
								StoragePolicy: &schema.StoragePolicy{
									Resolution: &schema.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &schema.Retention{
										Period: int64(24 * time.Hour),
									},
								},
								AggregationTypes: []schema.AggregationType{
									schema.AggregationType_P999,
								},
							},
						},
					},
					&schema.MappingRuleSnapshot{
						Name:        "foo",
						Tombstoned:  false,
						CutoverTime: 67890,
						TagFilters: map[string]string{
							"tag3": "value3",
							"tag4": "value4",
						},
						Policies: []*schema.Policy{
							&schema.Policy{
								StoragePolicy: &schema.StoragePolicy{
									Resolution: &schema.Resolution{
										WindowSize: int64(time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &schema.Retention{
										Period: int64(24 * time.Hour),
									},
								},
							},
							&schema.Policy{
								StoragePolicy: &schema.StoragePolicy{
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
					},
				},
			},
			&schema.MappingRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*schema.MappingRuleSnapshot{
					&schema.MappingRuleSnapshot{
						Name:        "dup",
						Tombstoned:  false,
						CutoverTime: 12345,
						TagFilters: map[string]string{
							"tag1": "value1",
							"tag2": "value2",
						},
						Policies: []*schema.Policy{
							&schema.Policy{
								StoragePolicy: &schema.StoragePolicy{
									Resolution: &schema.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &schema.Retention{
										Period: int64(24 * time.Hour),
									},
								},
								AggregationTypes: []schema.AggregationType{
									schema.AggregationType_P999,
								},
							},
						},
					},
				},
			},
		},
		RollupRules: []*schema.RollupRule{
			&schema.RollupRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*schema.RollupRuleSnapshot{
					&schema.RollupRuleSnapshot{
						Name:        "foo2",
						Tombstoned:  false,
						CutoverTime: 12345,
						TagFilters: map[string]string{
							"tag1": "value1",
							"tag2": "value2",
						},
						Targets: []*schema.RollupTarget{
							&schema.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*schema.Policy{
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
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
					},
					&schema.RollupRuleSnapshot{
						Name:        "bar",
						Tombstoned:  true,
						CutoverTime: 67890,
						TagFilters: map[string]string{
							"tag3": "value3",
							"tag4": "value4",
						},
						Targets: []*schema.RollupTarget{
							&schema.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*schema.Policy{
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
											Resolution: &schema.Resolution{
												WindowSize: int64(time.Minute),
												Precision:  int64(time.Minute),
											},
											Retention: &schema.Retention{
												Period: int64(24 * time.Hour),
											},
										},
									},
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
											Resolution: &schema.Resolution{
												WindowSize: int64(5 * time.Minute),
												Precision:  int64(time.Minute),
											},
											Retention: &schema.Retention{
												Period: int64(48 * time.Hour),
											},
										},
										AggregationTypes: []schema.AggregationType{
											schema.AggregationType_MEAN,
										},
									},
								},
							},
						},
					},
				},
			},
			&schema.RollupRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*schema.RollupRuleSnapshot{
					&schema.RollupRuleSnapshot{
						Name:        "foo",
						Tombstoned:  false,
						CutoverTime: 12345,
						TagFilters: map[string]string{
							"tag1": "value1",
							"tag2": "value2",
						},
						Targets: []*schema.RollupTarget{
							&schema.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*schema.Policy{
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
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
					},
					&schema.RollupRuleSnapshot{
						Name:        "baz",
						Tombstoned:  false,
						CutoverTime: 67890,
						TagFilters: map[string]string{
							"tag3": "value3",
							"tag4": "value4",
						},
						Targets: []*schema.RollupTarget{
							&schema.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*schema.Policy{
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
											Resolution: &schema.Resolution{
												WindowSize: int64(time.Minute),
												Precision:  int64(time.Minute),
											},
											Retention: &schema.Retention{
												Period: int64(24 * time.Hour),
											},
										},
									},
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
											Resolution: &schema.Resolution{
												WindowSize: int64(5 * time.Minute),
												Precision:  int64(time.Minute),
											},
											Retention: &schema.Retention{
												Period: int64(48 * time.Hour),
											},
										},
										AggregationTypes: []schema.AggregationType{
											schema.AggregationType_MEAN,
										},
									},
								},
							},
						},
					},
				},
			},
			&schema.RollupRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*schema.RollupRuleSnapshot{
					&schema.RollupRuleSnapshot{
						Name:        "dup",
						Tombstoned:  false,
						CutoverTime: 12345,
						TagFilters: map[string]string{
							"tag1": "value1",
							"tag2": "value2",
						},
						Targets: []*schema.RollupTarget{
							&schema.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*schema.Policy{
									&schema.Policy{
										StoragePolicy: &schema.StoragePolicy{
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
					},
				},
			},
		},
	}
)

func TestRuleSetKey(t *testing.T) {
	expected := "rules/ns"
	actual := RuleSetKey(testKeyFmt, testNamespace)
	require.Equal(t, expected, actual)
}

func TestRuleSet(t *testing.T) {
	store := mem.NewStore()
	rulesSetKey := RuleSetKey(testKeyFmt, testNamespace)
	store.Set(rulesSetKey, testRuleSet)
	_, s, err := RuleSet(store, rulesSetKey)
	require.NoError(t, err)
	require.NotNil(t, s)
}

func TestRuleSetError(t *testing.T) {
	store := mem.NewStore()
	rulesSetKey := RuleSetKey(testKeyFmt, testNamespace)
	store.Set(rulesSetKey, &schema.Namespace{Name: "x"})
	_, s, err := RuleSet(store, "blah")
	require.Error(t, err)
	require.Nil(t, s)
}

func TestValidateRuleSetTombstoned(t *testing.T) {
	store := mem.NewStore()
	rulesSetKey := RuleSetKey(testKeyFmt, testNamespace)
	store.Set(rulesSetKey, testRuleSet)
	_, _, err := ValidateRuleSet(store, rulesSetKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tombstoned")
}

func TestValidateRuleSetInvalid(t *testing.T) {
	store := mem.NewStore()
	rulesSetKey := RuleSetKey(testKeyFmt, testNamespace)
	store.Set(rulesSetKey, nil)
	_, _, err := ValidateRuleSet(store, rulesSetKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "could not read")
}

func TestRule(t *testing.T) {
	store := mem.NewStore()
	rulesSetKey := RuleSetKey(testKeyFmt, testNamespace)
	store.Set(rulesSetKey, testRuleSet)
	_, r, err := RuleSet(store, rulesSetKey)
	require.NoError(t, err)

	m, s, err := Rule(r, "foo")
	require.Nil(t, s)
	require.NoError(t, err)
	require.EqualValues(t, m, testRuleSet.MappingRules[0])

	m, s, err = Rule(r, "baz")
	require.Nil(t, m)
	require.NoError(t, err)
	require.EqualValues(t, s, testRuleSet.RollupRules[1])
}

func TestRuleDup(t *testing.T) {
	store := mem.NewStore()
	rulesSetKey := RuleSetKey(testKeyFmt, testNamespace)
	store.Set(rulesSetKey, testRuleSet)
	_, r, err := RuleSet(store, rulesSetKey)
	require.NoError(t, err)

	m, s, err := Rule(r, "dup")
	require.Error(t, err)
	require.Equal(t, errMultipleMatches, err)
	require.Nil(t, m)
	require.Nil(t, s)
}
