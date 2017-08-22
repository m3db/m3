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
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

var (
	compressor                = policy.NewAggregationIDCompressor()
	compressedMax, _          = compressor.Compress(policy.AggregationTypes{policy.Max})
	compressedCount, _        = compressor.Compress(policy.AggregationTypes{policy.Count})
	compressedMin, _          = compressor.Compress(policy.AggregationTypes{policy.Min})
	compressedMean, _         = compressor.Compress(policy.AggregationTypes{policy.Mean})
	compressedP999, _         = compressor.Compress(policy.AggregationTypes{policy.P999})
	compressedCountAndMean, _ = compressor.Compress(policy.AggregationTypes{policy.Count, policy.Mean})

	marshalTestSchema = &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
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
				},
			},
		},
		RollupRules: []*schema.RollupRule{
			&schema.RollupRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c69",
				Snapshots: []*schema.RollupRuleSnapshot{
					&schema.RollupRuleSnapshot{
						Name:        "bar",
						Tombstoned:  false,
						CutoverTime: 12345,
						TagFilters: map[string]string{
							"tag1": "value1",
							"tag2": "value2",
						},
						Targets: []*schema.RollupTarget{
							&schema.RollupTarget{
								Name: "test",
								Tags: []string{"foo", "bar"},
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
			},
		},
	}

	marshalTestString = `
		{"uuid":"ruleset",
		 "version":1,
		 "namespace":"namespace",
		 "createdAt":1234,
		 "lastUpdatedAt":5678,
		 "tombstoned":false,
		 "cutoverNanos":34923,
		 "mappingRules":[{
			 "uuid":"12669817-13ae-40e6-ba2f-33087b262c68",
			 "snapshots":[{
				 "name":"foo",
				 "tombstoned":false,
				 "cutoverNanos":12345,
				 "filters":{"tag1":"value1","tag2":"value2"},
				 "policies":["10s@1s:24h0m0s|P999"]
				}]
			}],
			"rollupRules":[{
				"uuid":"12669817-13ae-40e6-ba2f-33087b262c69",
				"snapshots":[{
					"name":"bar",
					"tombstoned":false,
					"cutoverNanos":12345,
					"filters":{"tag1":"value1","tag2":"value2"},
					"targets":[{
						"name":"test",
						"tags":["bar","foo"],
						"policies":["10s@1s:24h0m0s|P999"]
					}]
				}]
			}]
		}`
)

func TestMatchModeUnmarshalYAML(t *testing.T) {
	inputs := []struct {
		str         string
		expected    MatchMode
		expectedErr bool
	}{
		{
			str:      "forward",
			expected: ForwardMatch,
		},
		{
			str:      "reverse",
			expected: ReverseMatch,
		},
		{
			str:         "bad",
			expectedErr: true,
		},
	}
	for _, input := range inputs {
		var m MatchMode
		err := yaml.Unmarshal([]byte(input.str), &m)

		if input.expectedErr {
			require.Error(t, err)
			continue
		}

		require.NoError(t, err)
		require.Equal(t, input.expected, m)
	}
}

func TestActiveRuleSetMappingPoliciesForNonRollupID(t *testing.T) {
	inputs := []testMappingsData{
		{
			id:            "mtagName1=mtagValue1",
			matchFrom:     25000,
			matchTo:       25001,
			matchMode:     ForwardMatch,
			expireAtNanos: 30000,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					22000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
					},
				),
			},
		},
		{
			id:            "mtagName1=mtagValue1",
			matchFrom:     35000,
			matchTo:       35001,
			matchMode:     ForwardMatch,
			expireAtNanos: 100000,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					35000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
					},
				),
			},
		},
		{
			id:            "mtagName1=mtagValue2",
			matchFrom:     25000,
			matchTo:       25001,
			matchMode:     ForwardMatch,
			expireAtNanos: 30000,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					24000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
					},
				),
			},
		},
		{
			id:            "mtagName1=mtagValue3",
			matchFrom:     25000,
			matchTo:       25001,
			matchMode:     ForwardMatch,
			expireAtNanos: 30000,
			result:        policy.DefaultPoliciesList,
		},
		{
			id:            "mtagName1=mtagValue1",
			matchFrom:     10000,
			matchTo:       40000,
			matchMode:     ForwardMatch,
			expireAtNanos: 100000,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					10000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), compressedCount),
					},
				),
				policy.NewStagedPolicies(
					15000,
					false,
					[]policy.Policy{
						// different policies same resolution, merge aggregation types
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), compressedCountAndMean),
					},
				),
				policy.NewStagedPolicies(
					20000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), compressedMean),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
					},
				),
				policy.NewStagedPolicies(
					22000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
					},
				),
				policy.NewStagedPolicies(
					30000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
					},
				),
				policy.NewStagedPolicies(
					34000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
					},
				),
			},
		},
		{
			id:            "mtagName1=mtagValue2",
			matchFrom:     10000,
			matchTo:       40000,
			matchMode:     ForwardMatch,
			expireAtNanos: 100000,
			result: policy.PoliciesList{
				policy.DefaultStagedPolicies,
				policy.NewStagedPolicies(
					24000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
					},
				),
			},
		},
	}

	mappingRules := testMappingRules(t)
	as := newActiveRuleSet(
		0,
		mappingRules,
		nil,
		testTagsFilterOptions(),
		mockNewID,
		nil,
	)
	expectedCutovers := []int64{10000, 15000, 20000, 22000, 24000, 30000, 34000, 35000, 100000}
	require.Equal(t, expectedCutovers, as.cutoverTimesAsc)
	for _, input := range inputs {
		res := as.MatchAll(b(input.id), input.matchFrom, input.matchTo, input.matchMode)
		require.Equal(t, input.expireAtNanos, res.expireAtNanos)
		require.Equal(t, input.result, res.MappingsAt(0))
	}
}

func TestActiveRuleSetMappingPoliciesForRollupID(t *testing.T) {
	inputs := []testMappingsData{
		{
			id:            "rName4|rtagName1=rtagValue2",
			matchFrom:     25000,
			matchTo:       25001,
			matchMode:     ReverseMatch,
			expireAtNanos: 30000,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					24000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), policy.DefaultAggregationID),
					},
				),
			},
		},
		{
			id:            "rName4|rtagName2=rtagValue2",
			matchFrom:     25000,
			matchTo:       25001,
			matchMode:     ReverseMatch,
			expireAtNanos: 30000,
			result:        nil,
		},
		{
			id:            "rName4|rtagName1=rtagValue2",
			matchFrom:     10000,
			matchTo:       10001,
			matchMode:     ReverseMatch,
			expireAtNanos: 15000,
			result:        nil,
		},
		{
			id:            "rName3|rtagName1=rtagValue2",
			matchFrom:     25000,
			matchTo:       25001,
			matchMode:     ReverseMatch,
			expireAtNanos: 30000,
			result:        nil,
		},
	}

	rollupRules := testRollupRules(t)
	as := newActiveRuleSet(
		0,
		nil,
		rollupRules,
		testTagsFilterOptions(),
		mockNewID,
		func([]byte, []byte) bool { return true },
	)
	expectedCutovers := []int64{10000, 15000, 20000, 22000, 24000, 30000, 34000, 35000, 38000, 100000}
	require.Equal(t, expectedCutovers, as.cutoverTimesAsc)
	for _, input := range inputs {
		res := as.MatchAll(b(input.id), input.matchFrom, input.matchTo, input.matchMode)
		require.Equal(t, input.expireAtNanos, res.expireAtNanos)
		require.Equal(t, input.result, res.MappingsAt(0))
		require.Nil(t, res.rollups)
	}
}

func TestActiveRuleSetRollupResults(t *testing.T) {
	inputs := []testRollupResultsData{
		{
			id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
			matchFrom:     25000,
			matchTo:       25001,
			matchMode:     ForwardMatch,
			expireAtNanos: 30000,
			result: []RollupResult{
				{
					ID: b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					PoliciesList: policy.PoliciesList{
						policy.NewStagedPolicies(
							22000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
							},
						),
					},
				},
				{
					ID: b("rName2|rtagName1=rtagValue1"),
					PoliciesList: policy.PoliciesList{
						policy.NewStagedPolicies(
							22000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
							},
						),
					},
				},
			},
		},
		{
			id:            "rtagName1=rtagValue2",
			matchFrom:     25000,
			matchTo:       25001,
			matchMode:     ForwardMatch,
			expireAtNanos: 30000,
			result: []RollupResult{
				{
					ID: b("rName4|rtagName1=rtagValue2"),
					PoliciesList: policy.PoliciesList{
						policy.NewStagedPolicies(
							24000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), policy.DefaultAggregationID),
							},
						),
					},
				},
			},
		},
		{
			id:            "rtagName5=rtagValue5",
			matchFrom:     25000,
			matchTo:       25001,
			matchMode:     ForwardMatch,
			expireAtNanos: 30000,
			result:        []RollupResult{},
		},
		{
			id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
			matchFrom:     10000,
			matchTo:       40000,
			matchMode:     ForwardMatch,
			expireAtNanos: 100000,
			result: []RollupResult{
				{
					ID: b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					PoliciesList: policy.PoliciesList{
						policy.NewStagedPolicies(
							10000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
							},
						),
						policy.NewStagedPolicies(
							20000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
							},
						),
						policy.NewStagedPolicies(
							22000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
							},
						),
						policy.NewStagedPolicies(
							30000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
							},
						),
						policy.NewStagedPolicies(
							34000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
							},
						),
						policy.NewStagedPolicies(
							35000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(45*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
							},
						),
						policy.NewStagedPolicies(
							38000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(45*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
							},
						),
					},
				},
				{
					ID: b("rName2|rtagName1=rtagValue1"),
					PoliciesList: policy.PoliciesList{
						policy.NewStagedPolicies(
							22000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
							},
						),
						policy.NewStagedPolicies(
							34000,
							true,
							nil,
						),
					},
				},
			},
		},
	}

	rollupRules := testRollupRules(t)
	as := newActiveRuleSet(
		0,
		nil,
		rollupRules,
		testTagsFilterOptions(),
		mockNewID,
		nil,
	)
	expectedCutovers := []int64{10000, 15000, 20000, 22000, 24000, 30000, 34000, 35000, 38000, 100000}
	require.Equal(t, expectedCutovers, as.cutoverTimesAsc)
	for _, input := range inputs {
		res := as.MatchAll(b(input.id), input.matchFrom, input.matchTo, input.matchMode)
		require.Equal(t, input.expireAtNanos, res.expireAtNanos)
		require.Equal(t, len(input.result), res.NumRollups())
		for i := 0; i < len(input.result); i++ {
			rollup, tombstoned := res.RollupsAt(i, 0)
			id, policies := rollup.ID, rollup.PoliciesList
			require.False(t, tombstoned)
			require.Equal(t, input.result[i].ID, id)
			require.Equal(t, input.result[i].PoliciesList, policies)
		}
	}
}

func TestRuleSetProperties(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1
	rs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
	}
	newRuleSet, err := NewRuleSetFromSchema(version, rs, opts)
	require.NoError(t, err)
	ruleSet := newRuleSet.(*ruleSet)

	require.Equal(t, "ruleset", ruleSet.uuid)
	require.Equal(t, []byte("namespace"), ruleSet.Namespace())
	require.Equal(t, 1, ruleSet.Version())
	require.Equal(t, int64(34923), ruleSet.CutoverNanos())
	require.Equal(t, false, ruleSet.Tombstoned())
}

func TestRuleSetMarshalJSON(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1

	newRuleSet, err := NewRuleSetFromSchema(version, marshalTestSchema, opts)
	require.NoError(t, err)

	res, err := json.Marshal(newRuleSet)
	require.NoError(t, err)

	var bb bytes.Buffer
	err = json.Compact(&bb, []byte(marshalTestString))
	require.NoError(t, err)

	require.Equal(t, bb.String(), string(res))
}

func TestRuleSetUnmarshalJSON(t *testing.T) {
	version := 1
	newRuleSet, err := newMutableRuleSetFromSchema(version, marshalTestSchema)
	require.NoError(t, err)

	data, err := json.Marshal(newRuleSet)
	require.NoError(t, err)
	var rs ruleSet
	err = json.Unmarshal(data, &rs)
	require.NoError(t, err)

	expected, err := newRuleSet.Schema()
	require.NoError(t, err)

	actual, err := rs.Schema()
	require.NoError(t, err)

	require.Equal(t, expected, actual)
}

func TestRuleSetSchema(t *testing.T) {
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	rs, err := newMutableRuleSetFromSchema(version, expectedRs)
	require.NoError(t, err)
	res, err := rs.Schema()
	require.NoError(t, err)
	require.Equal(t, expectedRs, res)
}

func TestRuleSetActiveSet(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1
	rs := &schema.RuleSet{
		MappingRules: testMappingRulesConfig(),
		RollupRules:  testRollupRulesConfig(),
	}
	newRuleSet, err := NewRuleSetFromSchema(version, rs, opts)
	require.NoError(t, err)

	allInputs := []struct {
		activeSetTime time.Time
		mappingInputs []testMappingsData
		rollupInputs  []testRollupResultsData
	}{
		{
			activeSetTime: time.Unix(0, 0),
			mappingInputs: []testMappingsData{
				{
					id:            "mtagName1=mtagValue1",
					matchFrom:     25000,
					matchTo:       25001,
					matchMode:     ForwardMatch,
					expireAtNanos: 30000,
					result: policy.PoliciesList{
						policy.NewStagedPolicies(
							22000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), compressedMax),
								// the aggregation type came in from policy merging
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), compressedMin),
								policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
							},
						),
					},
				},
				{
					id:            "mtagName1=mtagValue1",
					matchFrom:     35000,
					matchTo:       35001,
					matchMode:     ForwardMatch,
					expireAtNanos: 100000,
					result: policy.PoliciesList{
						policy.NewStagedPolicies(
							35000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
							},
						),
					},
				},
				{
					id:            "mtagName1=mtagValue2",
					matchFrom:     25000,
					matchTo:       25001,
					matchMode:     ForwardMatch,
					expireAtNanos: 30000,
					result: policy.PoliciesList{
						policy.NewStagedPolicies(
							24000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), compressedP999),
							},
						),
					},
				},
				{
					id:            "mtagName1=mtagValue3",
					matchFrom:     25000,
					matchTo:       25001,
					matchMode:     ForwardMatch,
					expireAtNanos: 30000,
					result:        policy.DefaultPoliciesList,
				},
			},
			rollupInputs: []testRollupResultsData{
				{
					id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
					matchFrom:     25000,
					matchTo:       25001,
					matchMode:     ForwardMatch,
					expireAtNanos: 30000,
					result: []RollupResult{
						{
							ID: b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
							PoliciesList: policy.PoliciesList{
								policy.NewStagedPolicies(
									22000,
									false,
									[]policy.Policy{
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
										policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
										policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
									},
								),
							},
						},
						{
							ID: b("rName2|rtagName1=rtagValue1"),
							PoliciesList: policy.PoliciesList{
								policy.NewStagedPolicies(
									22000,
									false,
									[]policy.Policy{
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
									},
								),
							},
						},
					},
				},
				{
					id:            "rtagName1=rtagValue2",
					matchFrom:     25000,
					matchTo:       25001,
					matchMode:     ForwardMatch,
					expireAtNanos: 30000,
					result: []RollupResult{
						{
							ID: b("rName4|rtagName1=rtagValue2"),
							PoliciesList: policy.PoliciesList{
								policy.NewStagedPolicies(
									24000,
									false,
									[]policy.Policy{
										policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), policy.DefaultAggregationID),
									},
								),
							},
						},
					},
				},
				{
					id:            "rtagName5=rtagValue5",
					matchFrom:     25000,
					matchTo:       25001,
					matchMode:     ForwardMatch,
					expireAtNanos: 30000,
					result:        []RollupResult{},
				},
			},
		},
		{
			activeSetTime: time.Unix(0, 30000),
			mappingInputs: []testMappingsData{
				{
					id:            "mtagName1=mtagValue1",
					matchFrom:     35000,
					matchTo:       35001,
					matchMode:     ForwardMatch,
					expireAtNanos: 100000,
					result: policy.PoliciesList{
						policy.NewStagedPolicies(
							35000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
							},
						),
					},
				},
				{
					id:            "mtagName1=mtagValue2",
					matchFrom:     35000,
					matchTo:       35001,
					matchMode:     ForwardMatch,
					expireAtNanos: 100000,
					result: policy.PoliciesList{
						policy.NewStagedPolicies(
							24000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), compressedP999),
							},
						),
					},
				},
				{
					id:            "mtagName1=mtagValue3",
					matchFrom:     35000,
					matchTo:       35001,
					matchMode:     ForwardMatch,
					expireAtNanos: 100000,
					result:        policy.DefaultPoliciesList,
				},
			},
			rollupInputs: []testRollupResultsData{
				{
					id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
					matchFrom:     35000,
					matchTo:       35001,
					matchMode:     ForwardMatch,
					expireAtNanos: 100000,
					result: []RollupResult{
						{
							ID: b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
							PoliciesList: policy.PoliciesList{
								policy.NewStagedPolicies(
									35000,
									false,
									[]policy.Policy{
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
										policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
										policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
									},
								),
							},
						},
					},
				},
				{
					id:            "rtagName1=rtagValue2",
					matchFrom:     35000,
					matchTo:       35001,
					matchMode:     ForwardMatch,
					expireAtNanos: 100000,
					result: []RollupResult{
						{
							ID: b("rName4|rtagName1=rtagValue2"),
							PoliciesList: policy.PoliciesList{
								policy.NewStagedPolicies(
									24000,
									false,
									[]policy.Policy{
										policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), policy.DefaultAggregationID),
									},
								),
							},
						},
					},
				},
				{
					id:            "rtagName5=rtagValue5",
					matchFrom:     35000,
					matchTo:       35001,
					matchMode:     ForwardMatch,
					expireAtNanos: 100000,
					result:        []RollupResult{},
				},
			},
		},
		{
			activeSetTime: time.Unix(0, 200000),
			mappingInputs: []testMappingsData{
				{
					id:            "mtagName1=mtagValue1",
					matchFrom:     250000,
					matchTo:       250001,
					matchMode:     ForwardMatch,
					expireAtNanos: timeNanosMax,
					result: policy.PoliciesList{
						policy.NewStagedPolicies(
							100000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
							},
						),
					},
				},
				{
					id:            "mtagName1=mtagValue2",
					matchFrom:     250000,
					matchTo:       250001,
					matchMode:     ForwardMatch,
					expireAtNanos: timeNanosMax,
					result: policy.PoliciesList{
						policy.NewStagedPolicies(
							24000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), compressedP999),
							},
						),
					},
				},
				{
					id:            "mtagName1=mtagValue3",
					matchFrom:     250000,
					matchTo:       250001,
					matchMode:     ForwardMatch,
					expireAtNanos: timeNanosMax,
					result:        policy.DefaultPoliciesList,
				},
			},
			rollupInputs: []testRollupResultsData{
				{
					id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
					matchFrom:     250000,
					matchTo:       250001,
					matchMode:     ForwardMatch,
					expireAtNanos: timeNanosMax,
					result: []RollupResult{
						{
							ID: b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
							PoliciesList: policy.PoliciesList{
								policy.NewStagedPolicies(
									100000,
									false,
									[]policy.Policy{
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
										policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
										policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
									},
								),
							},
						},
						{
							ID: b("rName3|rtagName1=rtagValue1,rtagName2=rtagValue2"),
							PoliciesList: policy.PoliciesList{
								policy.NewStagedPolicies(
									100000,
									false,
									[]policy.Policy{
										policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
									},
								),
							},
						},
					},
				},
				{
					id:            "rtagName1=rtagValue2",
					matchFrom:     250000,
					matchTo:       250001,
					matchMode:     ForwardMatch,
					expireAtNanos: timeNanosMax,
					result: []RollupResult{
						{
							ID: b("rName4|rtagName1=rtagValue2"),
							PoliciesList: policy.PoliciesList{
								policy.NewStagedPolicies(
									24000,
									false,
									[]policy.Policy{
										policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), policy.DefaultAggregationID),
									},
								),
							},
						},
					},
				},
				{
					id:            "rtagName5=rtagValue5",
					matchFrom:     250000,
					matchTo:       250001,
					matchMode:     ForwardMatch,
					expireAtNanos: timeNanosMax,
					result:        []RollupResult{},
				},
			},
		},
	}

	for _, inputs := range allInputs {
		as := newRuleSet.ActiveSet(inputs.activeSetTime.UnixNano())
		for _, input := range inputs.mappingInputs {
			res := as.MatchAll(b(input.id), input.matchFrom, input.matchTo, input.matchMode)
			require.Equal(t, input.expireAtNanos, res.expireAtNanos)
			require.Equal(t, input.result, res.MappingsAt(0))
		}
		for _, input := range inputs.rollupInputs {
			res := as.MatchAll(b(input.id), input.matchFrom, input.matchTo, input.matchMode)
			require.Equal(t, input.expireAtNanos, res.expireAtNanos)
			require.Equal(t, len(input.result), res.NumRollups())
			for i := 0; i < len(input.result); i++ {
				rollup, tombstoned := res.RollupsAt(i, 0)
				id, policies := rollup.ID, rollup.PoliciesList
				require.False(t, tombstoned)
				require.Equal(t, input.result[i].ID, id)
				require.Equal(t, input.result[i].PoliciesList, policies)
			}
		}
	}
}

func testMappingRules(t *testing.T) []*mappingRule {
	filter1, err := filters.NewTagsFilter(
		map[string]string{"mtagName1": "mtagValue1"},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)
	filter2, err := filters.NewTagsFilter(
		map[string]string{"mtagName1": "mtagValue2"},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)

	mappingRule1 := &mappingRule{
		uuid: "mappingRule1",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:         "mappingRule1.snapshot1",
				tombstoned:   false,
				cutoverNanos: 10000,
				filter:       filter1,
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), compressedCount),
				},
			},
			&mappingRuleSnapshot{
				name:         "mappingRule1.snapshot2",
				tombstoned:   false,
				cutoverNanos: 20000,
				filter:       filter1,
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
					policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
				},
			},
			&mappingRuleSnapshot{
				name:         "mappingRule1.snapshot3",
				tombstoned:   false,
				cutoverNanos: 30000,
				filter:       filter1,
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
				},
			},
		},
	}

	mappingRule2 := &mappingRule{
		uuid: "mappingRule2",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:         "mappingRule2.snapshot1",
				tombstoned:   false,
				cutoverNanos: 15000,
				filter:       filter1,
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), compressedMean),
				},
			},
			&mappingRuleSnapshot{
				name:         "mappingRule2.snapshot2",
				tombstoned:   false,
				cutoverNanos: 22000,
				filter:       filter1,
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
					policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
				},
			},
			&mappingRuleSnapshot{
				name:         "mappingRule2.snapshot3",
				tombstoned:   true,
				cutoverNanos: 35000,
				filter:       filter1,
				policies:     []policy.Policy{},
			},
		},
	}

	mappingRule3 := &mappingRule{
		uuid: "mappingRule3",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:         "mappingRule3.snapshot1",
				tombstoned:   false,
				cutoverNanos: 22000,
				filter:       filter1,
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
					policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
					policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
				},
			},
			&mappingRuleSnapshot{
				name:         "mappingRule3.snapshot2",
				tombstoned:   false,
				cutoverNanos: 34000,
				filter:       filter1,
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
					policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
				},
			},
		},
	}

	mappingRule4 := &mappingRule{
		uuid: "mappingRule4",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:         "mappingRule4.snapshot1",
				tombstoned:   false,
				cutoverNanos: 24000,
				filter:       filter2,
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
				},
			},
		},
	}

	mappingRule5 := &mappingRule{
		uuid: "mappingRule5",
		snapshots: []*mappingRuleSnapshot{
			&mappingRuleSnapshot{
				name:         "mappingRule5.snapshot1",
				tombstoned:   false,
				cutoverNanos: 100000,
				filter:       filter1,
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
				},
			},
		},
	}

	return []*mappingRule{mappingRule1, mappingRule2, mappingRule3, mappingRule4, mappingRule5}
}

func testRollupRules(t *testing.T) []*rollupRule {
	filter1, err := filters.NewTagsFilter(
		map[string]string{
			"rtagName1": "rtagValue1",
			"rtagName2": "rtagValue2",
		},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)
	filter2, err := filters.NewTagsFilter(
		map[string]string{
			"rtagName1": "rtagValue2",
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
				targets: []RollupTarget{
					{
						Name: b("rName1"),
						Tags: [][]byte{b("rtagName1"), b("rtagName2")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
						},
					},
				},
			},
			&rollupRuleSnapshot{
				name:         "rollupRule1.snapshot2",
				tombstoned:   false,
				cutoverNanos: 20000,
				filter:       filter1,
				targets: []RollupTarget{
					{
						Name: b("rName1"),
						Tags: [][]byte{b("rtagName1"), b("rtagName2")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
							policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
						},
					},
				},
			},
			&rollupRuleSnapshot{
				name:         "rollupRule1.snapshot3",
				tombstoned:   false,
				cutoverNanos: 30000,
				filter:       filter1,
				targets: []RollupTarget{
					{
						Name: b("rName1"),
						Tags: [][]byte{b("rtagName1"), b("rtagName2")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
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
				targets: []RollupTarget{
					{
						Name: b("rName1"),
						Tags: [][]byte{b("rtagName1"), b("rtagName2")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
						},
					},
				},
			},
			&rollupRuleSnapshot{
				name:         "rollupRule2.snapshot2",
				tombstoned:   false,
				cutoverNanos: 22000,
				filter:       filter1,
				targets: []RollupTarget{
					{
						Name: b("rName1"),
						Tags: [][]byte{b("rtagName1"), b("rtagName2")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
						},
					},
				},
			},
			&rollupRuleSnapshot{
				name:         "rollupRule2.snapshot3",
				tombstoned:   false,
				cutoverNanos: 35000,
				filter:       filter1,
				targets: []RollupTarget{
					{
						Name: b("rName1"),
						Tags: [][]byte{b("rtagName1"), b("rtagName2")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(45*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
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
				targets: []RollupTarget{
					{
						Name: b("rName1"),
						Tags: [][]byte{b("rtagName1"), b("rtagName2")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
							policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
						},
					},
					{
						Name: b("rName2"),
						Tags: [][]byte{b("rtagName1")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
						},
					},
				},
			},
			&rollupRuleSnapshot{
				name:         "rollupRule3.snapshot2",
				tombstoned:   false,
				cutoverNanos: 34000,
				filter:       filter1,
				targets: []RollupTarget{
					{
						Name: b("rName1"),
						Tags: [][]byte{b("rtagName1"), b("rtagName2")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
						},
					},
				},
			},
			&rollupRuleSnapshot{
				name:         "rollupRule3.snapshot3",
				tombstoned:   true,
				cutoverNanos: 38000,
				filter:       filter1,
				targets:      []RollupTarget{},
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
				targets: []RollupTarget{
					{
						Name: b("rName3"),
						Tags: [][]byte{b("rtagName1"), b("rtagName2")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
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
				name:         "rollupRule5.snapshot1",
				tombstoned:   false,
				cutoverNanos: 24000,
				filter:       filter2,
				targets: []RollupTarget{
					{
						Name: b("rName4"),
						Tags: [][]byte{b("rtagName1")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), policy.DefaultAggregationID),
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
				targets: []RollupTarget{
					{
						Name: b("rName3"),
						Tags: [][]byte{b("rtagName1"), b("rtagName2")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
						},
					},
				},
			},
		},
	}

	return []*rollupRule{rollupRule1, rollupRule2, rollupRule3, rollupRule4, rollupRule5, rollupRule6}
}

func testMappingRulesConfig() []*schema.MappingRule {
	return []*schema.MappingRule{
		&schema.MappingRule{
			Uuid: "mappingRule1",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:        "mappingRule1.snapshot1",
					Tombstoned:  false,
					CutoverTime: 10000,
					TagFilters:  map[string]string{"mtagName1": "mtagValue1"},
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
				&schema.MappingRuleSnapshot{
					Name:        "mappingRule1.snapshot2",
					Tombstoned:  false,
					CutoverTime: 20000,
					TagFilters:  map[string]string{"mtagName1": "mtagValue1"},
					Policies: []*schema.Policy{
						&schema.Policy{
							StoragePolicy: &schema.StoragePolicy{
								Resolution: &schema.Resolution{
									WindowSize: int64(10 * time.Second),
									Precision:  int64(time.Second),
								},
								Retention: &schema.Retention{
									Period: int64(6 * time.Hour),
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
						&schema.Policy{
							StoragePolicy: &schema.StoragePolicy{
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
				&schema.MappingRuleSnapshot{
					Name:        "mappingRule1.snapshot3",
					Tombstoned:  false,
					CutoverTime: 30000,
					TagFilters:  map[string]string{"mtagName1": "mtagValue1"},
					Policies: []*schema.Policy{
						&schema.Policy{
							StoragePolicy: &schema.StoragePolicy{
								Resolution: &schema.Resolution{
									WindowSize: int64(30 * time.Second),
									Precision:  int64(time.Second),
								},
								Retention: &schema.Retention{
									Period: int64(6 * time.Hour),
								},
							},
						},
					},
				},
			},
		},
		&schema.MappingRule{
			Uuid: "mappingRule2",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:        "mappingRule2.snapshot1",
					Tombstoned:  false,
					CutoverTime: 15000,
					TagFilters:  map[string]string{"mtagName1": "mtagValue1"},
					Policies: []*schema.Policy{
						&schema.Policy{
							StoragePolicy: &schema.StoragePolicy{
								Resolution: &schema.Resolution{
									WindowSize: int64(10 * time.Second),
									Precision:  int64(time.Second),
								},
								Retention: &schema.Retention{
									Period: int64(12 * time.Hour),
								},
							},
						},
					},
				},
				&schema.MappingRuleSnapshot{
					Name:        "mappingRule2.snapshot2",
					Tombstoned:  false,
					CutoverTime: 22000,
					TagFilters:  map[string]string{"mtagName1": "mtagValue1"},
					Policies: []*schema.Policy{
						&schema.Policy{
							StoragePolicy: &schema.StoragePolicy{
								Resolution: &schema.Resolution{
									WindowSize: int64(10 * time.Second),
									Precision:  int64(time.Second),
								},
								Retention: &schema.Retention{
									Period: int64(2 * time.Hour),
								},
							},
						},
						&schema.Policy{
							StoragePolicy: &schema.StoragePolicy{
								Resolution: &schema.Resolution{
									WindowSize: int64(time.Minute),
									Precision:  int64(time.Minute),
								},
								Retention: &schema.Retention{
									Period: int64(time.Hour),
								},
							},
							AggregationTypes: []schema.AggregationType{
								schema.AggregationType_MIN,
							},
						},
					},
				},
				&schema.MappingRuleSnapshot{
					Name:        "mappingRule2.snapshot3",
					Tombstoned:  true,
					CutoverTime: 35000,
					TagFilters:  map[string]string{"mtagName1": "mtagValue1"},
					Policies:    []*schema.Policy{},
				},
			},
		},
		&schema.MappingRule{
			Uuid: "mappingRule3",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:        "mappingRule3.snapshot1",
					Tombstoned:  false,
					CutoverTime: 22000,
					TagFilters:  map[string]string{"mtagName1": "mtagValue1"},
					Policies: []*schema.Policy{
						&schema.Policy{
							StoragePolicy: &schema.StoragePolicy{
								Resolution: &schema.Resolution{
									WindowSize: int64(10 * time.Second),
									Precision:  int64(time.Second),
								},
								Retention: &schema.Retention{
									Period: int64(12 * time.Hour),
								},
							},
							AggregationTypes: []schema.AggregationType{
								schema.AggregationType_MAX,
							},
						},
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
				&schema.MappingRuleSnapshot{
					Name:        "mappingRule3.snapshot2",
					Tombstoned:  false,
					CutoverTime: 34000,
					TagFilters:  map[string]string{"mtagName1": "mtagValue1"},
					Policies: []*schema.Policy{
						&schema.Policy{
							StoragePolicy: &schema.StoragePolicy{
								Resolution: &schema.Resolution{
									WindowSize: int64(10 * time.Second),
									Precision:  int64(time.Second),
								},
								Retention: &schema.Retention{
									Period: int64(2 * time.Hour),
								},
							},
						},
						&schema.Policy{
							StoragePolicy: &schema.StoragePolicy{
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
		},
		&schema.MappingRule{
			Uuid: "mappingRule4",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:        "mappingRule4.snapshot1",
					Tombstoned:  false,
					CutoverTime: 24000,
					TagFilters:  map[string]string{"mtagName1": "mtagValue2"},
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
		&schema.MappingRule{
			Uuid: "mappingRule5",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:        "mappingRule5.snapshot1",
					Tombstoned:  false,
					CutoverTime: 100000,
					TagFilters:  map[string]string{"mtagName1": "mtagValue1"},
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
	}
}

func testRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:        "rollupRule1.snapshot1",
					Tombstoned:  false,
					CutoverTime: 10000,
					TagFilters: map[string]string{
						"rtagName1": "rtagValue1",
						"rtagName2": "rtagValue2",
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
					Name:        "rollupRule1.snapshot2",
					Tombstoned:  false,
					CutoverTime: 20000,
					TagFilters: map[string]string{
						"rtagName1": "rtagValue1",
						"rtagName2": "rtagValue2",
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
											Period: int64(6 * time.Hour),
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
								&schema.Policy{
									StoragePolicy: &schema.StoragePolicy{
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
				},
				&schema.RollupRuleSnapshot{
					Name:        "rollupRule1.snapshot3",
					Tombstoned:  false,
					CutoverTime: 30000,
					TagFilters: map[string]string{
						"rtagName1": "rtagValue1",
						"rtagName2": "rtagValue2",
					},
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name: "rName1",
							Tags: []string{"rtagName1", "rtagName2"},
							Policies: []*schema.Policy{
								&schema.Policy{
									StoragePolicy: &schema.StoragePolicy{
										Resolution: &schema.Resolution{
											WindowSize: int64(30 * time.Second),
											Precision:  int64(time.Second),
										},
										Retention: &schema.Retention{
											Period: int64(6 * time.Hour),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		&schema.RollupRule{
			Uuid: "rollupRule2",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:        "rollupRule2.snapshot1",
					Tombstoned:  false,
					CutoverTime: 15000,
					TagFilters: map[string]string{
						"rtagName1": "rtagValue1",
						"rtagName2": "rtagValue2",
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
											Period: int64(12 * time.Hour),
										},
									},
								},
							},
						},
					},
				},
				&schema.RollupRuleSnapshot{
					Name:        "rollupRule2.snapshot2",
					Tombstoned:  false,
					CutoverTime: 22000,
					TagFilters: map[string]string{
						"rtagName1": "rtagValue1",
						"rtagName2": "rtagValue2",
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
											Period: int64(2 * time.Hour),
										},
									},
								},
								&schema.Policy{
									StoragePolicy: &schema.StoragePolicy{
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
				},
				&schema.RollupRuleSnapshot{
					Name:        "rollupRule2.snapshot3",
					Tombstoned:  true,
					CutoverTime: 35000,
					TagFilters: map[string]string{
						"rtagName1": "rtagValue1",
						"rtagName2": "rtagValue2",
					},
					Targets: []*schema.RollupTarget{},
				},
			},
		},
		&schema.RollupRule{
			Uuid: "rollupRule3",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:        "rollupRule3.snapshot1",
					Tombstoned:  false,
					CutoverTime: 22000,
					TagFilters: map[string]string{
						"rtagName1": "rtagValue1",
						"rtagName2": "rtagValue2",
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
											Period: int64(12 * time.Hour),
										},
									},
								},
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
						&schema.RollupTarget{
							Name: "rName2",
							Tags: []string{"rtagName1"},
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
					Name:        "rollupRule3.snapshot2",
					Tombstoned:  false,
					CutoverTime: 34000,
					TagFilters: map[string]string{
						"rtagName1": "rtagValue1",
						"rtagName2": "rtagValue2",
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
											Period: int64(2 * time.Hour),
										},
									},
								},
								&schema.Policy{
									StoragePolicy: &schema.StoragePolicy{
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
				},
			},
		},
		&schema.RollupRule{
			Uuid: "rollupRule4",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:        "rollupRule4.snapshot1",
					Tombstoned:  false,
					CutoverTime: 24000,
					TagFilters: map[string]string{
						"rtagName1": "rtagValue2",
					},
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name: "rName3",
							Tags: []string{"rtagName1", "rtagName2"},
							Policies: []*schema.Policy{
								&schema.Policy{
									StoragePolicy: &schema.StoragePolicy{
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
				},
			},
		},
		&schema.RollupRule{
			Uuid: "rollupRule5",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:        "rollupRule5.snapshot1",
					Tombstoned:  false,
					CutoverTime: 24000,
					TagFilters: map[string]string{
						"rtagName1": "rtagValue2",
					},
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name: "rName4",
							Tags: []string{"rtagName1"},
							Policies: []*schema.Policy{
								&schema.Policy{
									StoragePolicy: &schema.StoragePolicy{
										Resolution: &schema.Resolution{
											WindowSize: int64(time.Second),
											Precision:  int64(time.Second),
										},
										Retention: &schema.Retention{
											Period: int64(time.Minute),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		&schema.RollupRule{
			Uuid: "rollupRule6",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:        "rollupRule6.snapshot1",
					Tombstoned:  false,
					CutoverTime: 100000,
					TagFilters: map[string]string{
						"rtagName1": "rtagValue1",
						"rtagName2": "rtagValue2",
					},
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name: "rName3",
							Tags: []string{"rtagName1", "rtagName2"},
							Policies: []*schema.Policy{
								&schema.Policy{
									StoragePolicy: &schema.StoragePolicy{
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
				},
			},
		},
	}
}

func testRuleSetOptions() Options {
	return NewOptions().
		SetTagsFilterOptions(testTagsFilterOptions()).
		SetNewRollupIDFn(mockNewID)
}

func mockNewID(name []byte, tags []id.TagPair) []byte {
	if len(tags) == 0 {
		return name
	}
	var buf bytes.Buffer
	buf.Write(name)
	if len(tags) > 0 {
		buf.WriteString("|")
		for idx, p := range tags {
			buf.Write(p.Name)
			buf.WriteString("=")
			buf.Write(p.Value)
			if idx < len(tags)-1 {
				buf.WriteString(",")
			}
		}
	}
	return buf.Bytes()
}

func testTagsFilterOptions() filters.TagsFilterOptions {
	return filters.TagsFilterOptions{
		NameTagKey: []byte("name"),
		NameAndTagsFn: func(b []byte) ([]byte, []byte, error) {
			idx := bytes.Index(b, []byte("|"))
			if idx == -1 {
				return nil, b, nil
			}
			return b[:idx], b[idx+1:], nil
		},
		SortedTagIteratorFn: filters.NewMockSortedTagIterator,
	}
}

func initMutableTest() (MutableRuleSet, *ruleSet, RuleSetUpdateHelper, error) {
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:          "ruleset",
		Namespace:     "namespace",
		CreatedAt:     1234,
		LastUpdatedAt: 5678,
		Tombstoned:    false,
		CutoverTime:   34923,
		MappingRules:  testMappingRulesConfig(),
		RollupRules:   testRollupRulesConfig(),
	}

	mutable, err := newMutableRuleSetFromSchema(version, expectedRs)
	rs := mutable.(*ruleSet)
	return mutable, rs, NewRuleSetUpdateHelper(10), err
}

// newMutableRuleSetFromSchema creates a new MutableRuleSet from a schema object.
func newMutableRuleSetFromSchema(version int, rs *schema.RuleSet) (MutableRuleSet, error) {
	// Takes a blank Options stuct because none of the mutation functions need the options.
	roRuleSet, err := NewRuleSetFromSchema(version, rs, NewOptions())
	if err != nil {
		return nil, err
	}
	return roRuleSet.(*ruleSet), nil
}

func TestAddMappingRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)
	_, err = rs.getMappingRuleByName("foo")
	require.Error(t, err)

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	data := MappingRuleData{
		Name:           "foo",
		Filters:        newFilters,
		Policies:       p,
		UpdateMetadata: helper.NewUpdateMetadata(),
	}
	err = mutable.AddMappingRule(data)
	require.NoError(t, err)

	_, err = rs.getMappingRuleByName("foo")
	require.NoError(t, err)
}

func TestAddMappingRuleDup(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	m, err := rs.getMappingRuleByName("mappingRule5.snapshot1")
	require.NoError(t, err)
	require.NotNil(t, m)

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	data := MappingRuleData{
		Name:           "mappingRule5.snapshot1",
		Filters:        newFilters,
		Policies:       p,
		UpdateMetadata: helper.NewUpdateMetadata(),
	}

	err = mutable.AddMappingRule(data)
	require.Error(t, err)
}

func TestAddMappingRuleRevive(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	m, err := rs.getMappingRuleByName("mappingRule5.snapshot1")
	require.NoError(t, err)
	require.NotNil(t, m)

	dd := DeleteData{ID: "mappingRule5"}
	err = mutable.DeleteMappingRule(dd)
	require.NoError(t, err)

	newFilters := map[string]string{"test": "bar"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	data := MappingRuleData{
		Name:           "mappingRule5.snapshot1",
		Filters:        newFilters,
		Policies:       p,
		UpdateMetadata: helper.NewUpdateMetadata(),
	}
	err = rs.AddMappingRule(data)
	require.NoError(t, err)

	mr, err := rs.getMappingRuleByID("mappingRule5")
	require.NoError(t, err)
	require.Equal(t, mr.snapshots[len(mr.snapshots)-1].rawFilters, newFilters)
}

func TestUpdateMappingRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	mutableClone, err := mutable.Clone()
	require.NoError(t, err)

	_, err = rs.getMappingRuleByID("mappingRule5")
	require.NoError(t, err)

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	data := MappingRuleData{
		Name:           "foo",
		Filters:        newFilters,
		Policies:       p,
		UpdateMetadata: helper.NewUpdateMetadata(),
	}

	update := MappingRuleUpdate{
		Data: data,
		ID:   "mappingRule5",
	}

	err = mutableClone.UpdateMappingRule(update)
	require.NoError(t, err)

	res, err := mutableClone.(*ruleSet).getMappingRuleByID("mappingRule5")
	require.NoError(t, err)
	n, err := res.Name()
	require.NoError(t, err)
	require.Equal(t, "foo", n)

	orig, err := rs.getMappingRuleByID("mappingRule5")
	require.NoError(t, err)
	n, err = orig.Name()
	require.NoError(t, err)
	require.Equal(t, "mappingRule5.snapshot1", n)
}

func TestDeleteMappingRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	m, err := rs.getMappingRuleByID("mappingRule5")
	require.NoError(t, err)
	require.NotNil(t, m)

	dd := DeleteData{ID: "mappingRule5", UpdateMetadata: helper.NewUpdateMetadata()}
	err = mutable.DeleteMappingRule(dd)
	require.NoError(t, err)

	m, err = rs.getMappingRuleByID("mappingRule5")
	require.NoError(t, err)
	require.True(t, m.Tombstoned())
}

func TestAddRollupRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	_, err = rs.getRollupRuleByID("foo")
	require.Error(t, err)

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}

	newTargets := []RollupTarget{
		RollupTarget{
			Name:     b("blah"),
			Tags:     bs("a"),
			Policies: p,
		},
	}
	data := RollupRuleData{
		Name:           "foo",
		Filters:        newFilters,
		Targets:        newTargets,
		UpdateMetadata: helper.NewUpdateMetadata(),
	}

	err = mutable.AddRollupRule(data)
	require.NoError(t, err)

	res, err := rs.getRollupRuleByName("foo")
	require.NotEmpty(t, res.uuid)
	require.NoError(t, err)
}

func TestAddRollupRuleDup(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	r, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)
	require.NotNil(t, r)

	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}

	newTargets := []RollupTarget{
		RollupTarget{
			Name:     b("blah"),
			Tags:     bs("a"),
			Policies: p,
		},
	}
	newFilters := map[string]string{"test": "bar"}
	rrd := RollupRuleData{
		Name:           "rollupRule5.snapshot1",
		Filters:        newFilters,
		Targets:        newTargets,
		UpdateMetadata: helper.NewUpdateMetadata(),
	}
	err = mutable.AddRollupRule(rrd)
	require.Error(t, err)
}

func TestReviveRollupRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	rr, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)
	updateMeta := helper.NewUpdateMetadata()

	dd := DeleteData{ID: rr.uuid, UpdateMetadata: updateMeta}
	err = mutable.DeleteRollupRule(dd)
	require.NoError(t, err)

	rr, err = rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)
	require.True(t, rr.Tombstoned())

	snapshot := rr.snapshots[len(rr.snapshots)-1]

	rrd := RollupRuleData{
		Name:           "rollupRule5.snapshot1",
		Filters:        snapshot.rawFilters,
		Targets:        snapshot.targets,
		UpdateMetadata: updateMeta,
	}

	err = mutable.AddRollupRule(rrd)
	require.NoError(t, err)

	rr, err = rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)
	require.Equal(t, rr.snapshots[len(rr.snapshots)-1].rawFilters, snapshot.rawFilters)
}

func TestUpdateRollupRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	rr, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)
	updateMeta := helper.NewUpdateMetadata()

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	newTargets := []RollupTarget{
		RollupTarget{
			Name:     b("blah"),
			Tags:     bs("a"),
			Policies: p,
		},
	}

	data := RollupRuleData{
		Name:           "foo",
		Filters:        newFilters,
		Targets:        newTargets,
		UpdateMetadata: updateMeta,
	}

	update := RollupRuleUpdate{
		Data: data,
		ID:   rr.uuid,
	}

	err = mutable.UpdateRollupRule(update)
	require.NoError(t, err)

	_, err = rs.getRollupRuleByName("foo")
	require.NoError(t, err)
}
func TestUpdateRollupRuleDupTarget(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	rr, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)
	updateMeta := helper.NewUpdateMetadata()

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	// Duplicate target from rollupRule4
	newTargets := []RollupTarget{
		RollupTarget{
			Name:     b("rName3"),
			Tags:     bs("rtagName1", "rtagName2"),
			Policies: p,
		},
	}

	data := RollupRuleData{
		Name:           "foo",
		Filters:        newFilters,
		Targets:        newTargets,
		UpdateMetadata: updateMeta,
	}

	update := RollupRuleUpdate{
		Data: data,
		ID:   rr.uuid,
	}

	err = mutable.UpdateRollupRule(update)
	require.Error(t, err)
}

func TestDeleteRollupRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	rr, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)
	updateMeta := helper.NewUpdateMetadata()

	dd := DeleteData{
		ID:             rr.uuid,
		UpdateMetadata: updateMeta,
	}
	err = mutable.DeleteRollupRule(dd)
	require.NoError(t, err)

	rr, err = rs.getRollupRuleByName("rollupRule5.snapshot1")
	require.NoError(t, err)
	require.True(t, rr.Tombstoned())
}

func TestDeleteRuleset(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	err = mutable.Delete(helper.NewUpdateMetadata())
	require.NoError(t, err)

	require.True(t, mutable.Tombstoned())
	for _, m := range rs.mappingRules {
		require.True(t, m.Tombstoned())
	}

	for _, r := range rs.rollupRules {
		require.True(t, r.Tombstoned())
	}
}

func TestReviveRuleSet(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	err = mutable.Delete(helper.NewUpdateMetadata())
	require.NoError(t, err)

	err = mutable.Revive(helper.NewUpdateMetadata())
	require.NoError(t, err)

	require.False(t, rs.Tombstoned())
	for _, m := range rs.mappingRules {
		require.True(t, m.Tombstoned())
	}

	for _, r := range rs.rollupRules {
		require.True(t, r.Tombstoned())
	}
}

type testMappingsData struct {
	id            string
	matchFrom     int64
	matchTo       int64
	matchMode     MatchMode
	expireAtNanos int64
	result        policy.PoliciesList
}

type testRollupResultsData struct {
	id            string
	matchFrom     int64
	matchTo       int64
	matchMode     MatchMode
	expireAtNanos int64
	result        []RollupResult
}

func b(v string) []byte {
	return []byte(v)
}

func bs(v ...string) [][]byte {
	result := make([][]byte, len(v))
	for i, str := range v {
		result[i] = []byte(str)
	}
	return result
}
