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
	"testing"
	"time"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	compressor                = policy.NewAggregationIDCompressor()
	compressedUpper, _        = compressor.Compress(policy.AggregationTypes{policy.Upper})
	compressedCount, _        = compressor.Compress(policy.AggregationTypes{policy.Count})
	compressedLower, _        = compressor.Compress(policy.AggregationTypes{policy.Lower})
	compressedMean, _         = compressor.Compress(policy.AggregationTypes{policy.Mean})
	compressedP999, _         = compressor.Compress(policy.AggregationTypes{policy.P999})
	compressedCountAndMean, _ = compressor.Compress(policy.AggregationTypes{policy.Count, policy.Mean})
)

func TestActiveRuleSetMatchMappingRules(t *testing.T) {
	inputs := []testMappingsData{
		{
			id:            "mtagName1=mtagValue1",
			matchFrom:     time.Unix(0, 25000),
			matchTo:       time.Unix(0, 25001),
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
			matchFrom:     time.Unix(0, 35000),
			matchTo:       time.Unix(0, 35001),
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
			matchFrom:     time.Unix(0, 25000),
			matchTo:       time.Unix(0, 25001),
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
			matchFrom:     time.Unix(0, 25000),
			matchTo:       time.Unix(0, 25001),
			expireAtNanos: 30000,
			result:        policy.DefaultPoliciesList,
		},
		{
			id:            "mtagName1=mtagValue1",
			matchFrom:     time.Unix(0, 10000),
			matchTo:       time.Unix(0, 40000),
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
			matchFrom:     time.Unix(0, 10000),
			matchTo:       time.Unix(0, 40000),
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
		mappingRules,
		nil,
		testTagsFilterOptions(),
		mockNewID,
	)
	expectedCutovers := []int64{10000, 15000, 20000, 22000, 24000, 30000, 34000, 35000, 100000}
	require.Equal(t, expectedCutovers, as.cutoverTimesAsc)
	for _, input := range inputs {
		res := as.MatchAll(b(input.id), input.matchFrom, input.matchTo)
		require.Equal(t, input.expireAtNanos, res.expireAtNanos)
		require.Equal(t, input.result, res.MappingsAt(0))
	}
}

func TestActiveRuleSetMatchRollupRules(t *testing.T) {
	inputs := []testRollupResultsData{
		{
			id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
			matchFrom:     time.Unix(0, 25000),
			matchTo:       time.Unix(0, 25001),
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
			matchFrom:     time.Unix(0, 25000),
			matchTo:       time.Unix(0, 25001),
			expireAtNanos: 30000,
			result: []RollupResult{
				{
					ID: b("rName3|rtagName1=rtagValue2"),
					PoliciesList: policy.PoliciesList{
						policy.NewStagedPolicies(
							24000,
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
			id:            "rtagName5=rtagValue5",
			matchFrom:     time.Unix(0, 25000),
			matchTo:       time.Unix(0, 25001),
			expireAtNanos: 30000,
			result:        []RollupResult{},
		},
		{
			id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
			matchFrom:     time.Unix(0, 10000),
			matchTo:       time.Unix(0, 40000),
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
		nil,
		rollupRules,
		testTagsFilterOptions(),
		mockNewID,
	)
	expectedCutovers := []int64{10000, 15000, 20000, 22000, 24000, 30000, 34000, 35000, 38000, 100000}
	require.Equal(t, expectedCutovers, as.cutoverTimesAsc)
	for _, input := range inputs {
		res := as.MatchAll(b(input.id), input.matchFrom, input.matchTo)
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
	newRuleSet, err := NewRuleSet(version, rs, opts)
	require.NoError(t, err)
	ruleSet := newRuleSet.(*ruleSet)

	require.Equal(t, "ruleset", ruleSet.uuid)
	require.Equal(t, []byte("namespace"), ruleSet.Namespace())
	require.Equal(t, 1, ruleSet.Version())
	require.Equal(t, int64(34923), ruleSet.CutoverNanos())
	require.Equal(t, false, ruleSet.TombStoned())
}

func TestRuleSetActiveSet(t *testing.T) {
	opts := testRuleSetOptions()
	version := 1
	rs := &schema.RuleSet{
		MappingRules: testMappingRulesConfig(),
		RollupRules:  testRollupRulesConfig(),
	}
	newRuleSet, err := NewRuleSet(version, rs, opts)
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
					matchFrom:     time.Unix(0, 25000),
					matchTo:       time.Unix(0, 25001),
					expireAtNanos: 30000,
					result: policy.PoliciesList{
						policy.NewStagedPolicies(
							22000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), compressedUpper),
								// the aggregation type came in from policy merging
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), compressedLower),
								policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
							},
						),
					},
				},
				{
					id:            "mtagName1=mtagValue1",
					matchFrom:     time.Unix(0, 35000),
					matchTo:       time.Unix(0, 35001),
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
					matchFrom:     time.Unix(0, 25000),
					matchTo:       time.Unix(0, 25001),
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
					matchFrom:     time.Unix(0, 25000),
					matchTo:       time.Unix(0, 25001),
					expireAtNanos: 30000,
					result:        policy.DefaultPoliciesList,
				},
			},
			rollupInputs: []testRollupResultsData{
				{
					id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
					matchFrom:     time.Unix(0, 25000),
					matchTo:       time.Unix(0, 25001),
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
					matchFrom:     time.Unix(0, 25000),
					matchTo:       time.Unix(0, 25001),
					expireAtNanos: 30000,
					result: []RollupResult{
						{
							ID: b("rName3|rtagName1=rtagValue2"),
							PoliciesList: policy.PoliciesList{
								policy.NewStagedPolicies(
									24000,
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
					id:            "rtagName5=rtagValue5",
					matchFrom:     time.Unix(0, 25000),
					matchTo:       time.Unix(0, 25001),
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
					matchFrom:     time.Unix(0, 35000),
					matchTo:       time.Unix(0, 35001),
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
					matchFrom:     time.Unix(0, 35000),
					matchTo:       time.Unix(0, 35001),
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
					matchFrom:     time.Unix(0, 35000),
					matchTo:       time.Unix(0, 35001),
					expireAtNanos: 100000,
					result:        policy.DefaultPoliciesList,
				},
			},
			rollupInputs: []testRollupResultsData{
				{
					id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
					matchFrom:     time.Unix(0, 35000),
					matchTo:       time.Unix(0, 35001),
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
					matchFrom:     time.Unix(0, 35000),
					matchTo:       time.Unix(0, 35001),
					expireAtNanos: 100000,
					result: []RollupResult{
						{
							ID: b("rName3|rtagName1=rtagValue2"),
							PoliciesList: policy.PoliciesList{
								policy.NewStagedPolicies(
									24000,
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
					id:            "rtagName5=rtagValue5",
					matchFrom:     time.Unix(0, 35000),
					matchTo:       time.Unix(0, 35001),
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
					matchFrom:     time.Unix(0, 250000),
					matchTo:       time.Unix(0, 250001),
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
					matchFrom:     time.Unix(0, 250000),
					matchTo:       time.Unix(0, 250001),
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
					matchFrom:     time.Unix(0, 250000),
					matchTo:       time.Unix(0, 250001),
					expireAtNanos: timeNanosMax,
					result:        policy.DefaultPoliciesList,
				},
			},
			rollupInputs: []testRollupResultsData{
				{
					id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
					matchFrom:     time.Unix(0, 250000),
					matchTo:       time.Unix(0, 250001),
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
					matchFrom:     time.Unix(0, 250000),
					matchTo:       time.Unix(0, 250001),
					expireAtNanos: timeNanosMax,
					result: []RollupResult{
						{
							ID: b("rName3|rtagName1=rtagValue2"),
							PoliciesList: policy.PoliciesList{
								policy.NewStagedPolicies(
									24000,
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
					id:            "rtagName5=rtagValue5",
					matchFrom:     time.Unix(0, 250000),
					matchTo:       time.Unix(0, 250001),
					expireAtNanos: timeNanosMax,
					result:        []RollupResult{},
				},
			},
		},
	}

	for _, inputs := range allInputs {
		as := newRuleSet.ActiveSet(inputs.activeSetTime)
		for _, input := range inputs.mappingInputs {
			res := as.MatchAll(b(input.id), input.matchFrom, input.matchTo)
			require.Equal(t, input.expireAtNanos, res.expireAtNanos)
			require.Equal(t, input.result, res.MappingsAt(0))
		}
		for _, input := range inputs.rollupInputs {
			res := as.MatchAll(b(input.id), input.matchFrom, input.matchTo)
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
				targets: []rollupTarget{
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
				targets: []rollupTarget{
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
				targets: []rollupTarget{
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
				targets: []rollupTarget{
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
				targets: []rollupTarget{
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
				targets: []rollupTarget{
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
				targets: []rollupTarget{
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
				targets: []rollupTarget{
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
				targets:      []rollupTarget{},
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
				cutoverNanos: 100000,
				filter:       filter1,
				targets: []rollupTarget{
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

	return []*rollupRule{rollupRule1, rollupRule2, rollupRule3, rollupRule4, rollupRule5}
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
								schema.AggregationType_LOWER,
							},
						},
					},
				},
				&schema.MappingRuleSnapshot{
					Name:        "mappingRule2.snapshot3",
					Tombstoned:  true,
					CutoverTime: 35000,
					TagFilters:  map[string]string{"mtagName1": "mtagValue1"},
					Policies:    nil,
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
								schema.AggregationType_UPPER,
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
					Targets: nil,
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

func testTagsFilterOptions() filters.TagsFilterOptions {
	return filters.TagsFilterOptions{
		NameTagKey:          []byte("name"),
		NameAndTagsFn:       func(b []byte) ([]byte, []byte, error) { return nil, b, nil },
		SortedTagIteratorFn: filters.NewMockSortedTagIterator,
	}
}

type testMappingsData struct {
	id            string
	matchFrom     time.Time
	matchTo       time.Time
	expireAtNanos int64
	result        policy.PoliciesList
}

type testRollupResultsData struct {
	id            string
	matchFrom     time.Time
	matchTo       time.Time
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
