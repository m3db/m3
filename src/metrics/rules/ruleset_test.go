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
	"math"
	"testing"
	"time"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	compressor      = policy.NewAggregationIDCompressor()
	compressedMax   = compressor.MustCompress(policy.AggregationTypes{policy.Max})
	compressedCount = compressor.MustCompress(policy.AggregationTypes{policy.Count})
	compressedMin   = compressor.MustCompress(policy.AggregationTypes{policy.Min})
	compressedMean  = compressor.MustCompress(policy.AggregationTypes{policy.Mean})
	compressedP999  = compressor.MustCompress(policy.AggregationTypes{policy.P999})

	now      = time.Now().UnixNano()
	testUser = "test_user"
)

func TestActiveRuleSetForwardMappingPoliciesForNonRollupID(t *testing.T) {
	inputs := []testMappingsData{
		{
			id:            "mtagName1=mtagValue1",
			matchFrom:     25000,
			matchTo:       25001,
			expireAtNanos: 30000,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					22000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
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
			expireAtNanos: 30000,
			result:        policy.DefaultPoliciesList,
		},
		{
			id:            "mtagName1=mtagValue1",
			matchFrom:     10000,
			matchTo:       40000,
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
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), compressedMean),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), compressedCount),
					},
				),
				policy.NewStagedPolicies(
					20000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), compressedMean),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
					},
				),
				policy.NewStagedPolicies(
					22000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
					},
				),
				policy.NewStagedPolicies(
					30000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
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
		policy.NewAggregationTypesOptions(),
	)
	expectedCutovers := []int64{10000, 15000, 20000, 22000, 24000, 30000, 34000, 35000, 100000}
	require.Equal(t, expectedCutovers, as.cutoverTimesAsc)
	for _, input := range inputs {
		res := as.ForwardMatch(b(input.id), input.matchFrom, input.matchTo)
		require.Equal(t, input.expireAtNanos, res.expireAtNanos)
		require.Equal(t, input.result, res.MappingsAt(0))
	}
}

func TestActiveRuleSetReverseMappingPoliciesForNonRollupID(t *testing.T) {
	inputs := []testMappingsData{
		{
			id:              "mtagName1=mtagValue1",
			matchFrom:       25000,
			matchTo:         25001,
			expireAtNanos:   30000,
			metricType:      metric.CounterType,
			aggregationType: policy.Sum,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					22000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
					},
				),
			},
		},
		{
			id:              "mtagName1=mtagValue1",
			matchFrom:       35000,
			matchTo:         35001,
			expireAtNanos:   100000,
			metricType:      metric.CounterType,
			aggregationType: policy.Sum,
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
			id:              "mtagName1=mtagValue2",
			matchFrom:       25000,
			matchTo:         25001,
			expireAtNanos:   30000,
			metricType:      metric.CounterType,
			aggregationType: policy.Sum,
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
			id:              "mtagName1=mtagValue3",
			matchFrom:       25000,
			matchTo:         25001,
			expireAtNanos:   30000,
			metricType:      metric.CounterType,
			aggregationType: policy.Sum,
			result:          policy.DefaultPoliciesList,
		},
		{
			id:              "mtagName1=mtagValue3",
			matchFrom:       25000,
			matchTo:         25001,
			expireAtNanos:   30000,
			metricType:      metric.CounterType,
			aggregationType: policy.Min,
			result:          nil,
		},
		{
			id:              "mtagName1=mtagValue1",
			matchFrom:       10000,
			matchTo:         12000,
			expireAtNanos:   15000,
			metricType:      metric.CounterType,
			aggregationType: policy.Sum,
			result:          nil,
		},
		{
			id:              "mtagName1=mtagValue1",
			matchFrom:       10000,
			matchTo:         21000,
			expireAtNanos:   22000,
			metricType:      metric.CounterType,
			aggregationType: policy.Sum,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					20000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
					},
				),
			},
		},
		{
			id:              "mtagName1=mtagValue1",
			matchFrom:       10000,
			matchTo:         40000,
			expireAtNanos:   100000,
			metricType:      metric.TimerType,
			aggregationType: policy.Count,
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
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), compressedCount),
					},
				),
				policy.NewStagedPolicies(
					20000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
					},
				),
				policy.NewStagedPolicies(
					22000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
					},
				),
				policy.NewStagedPolicies(
					30000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
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
			id:              "mtagName1=mtagValue2",
			matchFrom:       10000,
			matchTo:         40000,
			expireAtNanos:   100000,
			metricType:      metric.CounterType,
			aggregationType: policy.Sum,
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
		func([]byte, []byte) bool { return false },
		policy.NewAggregationTypesOptions(),
	)
	expectedCutovers := []int64{10000, 15000, 20000, 22000, 24000, 30000, 34000, 35000, 100000}
	require.Equal(t, expectedCutovers, as.cutoverTimesAsc)
	for _, input := range inputs {
		res := as.ReverseMatch(b(input.id), input.matchFrom, input.matchTo, input.metricType, input.aggregationType)
		require.Equal(t, input.expireAtNanos, res.expireAtNanos)
		require.Equal(t, input.result, res.MappingsAt(0))
	}
}

func TestActiveRuleSetReverseMappingPoliciesForRollupID(t *testing.T) {
	inputs := []testMappingsData{
		{
			id:              "rName4|rtagName1=rtagValue2",
			matchFrom:       25000,
			matchTo:         25001,
			expireAtNanos:   30000,
			metricType:      metric.CounterType,
			aggregationType: policy.Sum,
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
			id:              "rName4|rtagName1=rtagValue2",
			matchFrom:       25000,
			matchTo:         25001,
			expireAtNanos:   30000,
			metricType:      metric.CounterType,
			aggregationType: policy.Min,
			result:          nil,
		},
		{
			id:              "rName4|rtagName1=rtagValue2",
			matchFrom:       25000,
			matchTo:         25001,
			expireAtNanos:   30000,
			metricType:      metric.UnknownType,
			aggregationType: policy.UnknownAggregationType,
			result:          nil,
		},
		{
			id:              "rName4|rtagName1=rtagValue2",
			matchFrom:       25000,
			matchTo:         25001,
			expireAtNanos:   30000,
			metricType:      metric.TimerType,
			aggregationType: policy.P99,
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
			id:              "rName4|rtagName2=rtagValue2",
			matchFrom:       25000,
			matchTo:         25001,
			expireAtNanos:   30000,
			metricType:      metric.CounterType,
			aggregationType: policy.Sum,
			result:          nil,
		},
		{
			id:            "rName4|rtagName1=rtagValue2",
			matchFrom:     10000,
			matchTo:       10001,
			expireAtNanos: 15000,
			result:        nil,
		},
		{
			id:              "rName5|rtagName1=rtagValue2",
			matchFrom:       130000,
			matchTo:         130001,
			expireAtNanos:   math.MaxInt64,
			metricType:      metric.TimerType,
			aggregationType: policy.P99,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					120000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), policy.MustCompressAggregationTypes(policy.Sum, policy.P90, policy.P99)),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour), policy.MustCompressAggregationTypes(policy.Sum, policy.P99)),
					},
				),
			},
		},
		{
			id:              "rName5|rtagName1=rtagValue2",
			matchFrom:       130000,
			matchTo:         130001,
			expireAtNanos:   math.MaxInt64,
			metricType:      metric.TimerType,
			aggregationType: policy.P90,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					120000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), policy.MustCompressAggregationTypes(policy.Sum, policy.P90, policy.P99)),
					},
				),
			},
		},
		{
			id:              "rName5|rtagName1=rtagValue2",
			matchFrom:       130000,
			matchTo:         130001,
			expireAtNanos:   math.MaxInt64,
			metricType:      metric.GaugeType,
			aggregationType: policy.Last,
			result:          nil,
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
		policy.NewAggregationTypesOptions(),
	)
	expectedCutovers := []int64{10000, 15000, 20000, 22000, 24000, 30000, 34000, 35000, 38000, 100000, 120000}
	require.Equal(t, expectedCutovers, as.cutoverTimesAsc)
	for _, input := range inputs {
		res := as.ReverseMatch(b(input.id), input.matchFrom, input.matchTo, input.metricType, input.aggregationType)
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
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
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
			expireAtNanos: 30000,
			result:        []RollupResult{},
		},
		{
			id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
			matchFrom:     10000,
			matchTo:       40000,
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
							15000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
							},
						),
						policy.NewStagedPolicies(
							20000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
							},
						),
						policy.NewStagedPolicies(
							22000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
							},
						),
						policy.NewStagedPolicies(
							30000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
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
		policy.NewAggregationTypesOptions(),
	)
	expectedCutovers := []int64{10000, 15000, 20000, 22000, 24000, 30000, 34000, 35000, 38000, 100000, 120000}
	require.Equal(t, expectedCutovers, as.cutoverTimesAsc)
	for _, input := range inputs {
		res := as.ForwardMatch(b(input.id), input.matchFrom, input.matchTo)
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
		Uuid:               "ruleset",
		Namespace:          "namespace",
		CreatedAtNanos:     1234,
		LastUpdatedAtNanos: 5678,
		Tombstoned:         false,
		CutoverNanos:       34923,
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

func TestRuleSetSchema(t *testing.T) {
	version := 1

	expectedRs := &schema.RuleSet{
		Uuid:               "ruleset",
		Namespace:          "namespace",
		CreatedAtNanos:     1234,
		LastUpdatedAtNanos: 5678,
		LastUpdatedBy:      "someone",
		Tombstoned:         false,
		CutoverNanos:       34923,
		MappingRules:       testMappingRulesConfig(),
		RollupRules:        testRollupRulesConfig(),
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
					expireAtNanos: 30000,
					result: policy.PoliciesList{
						policy.NewStagedPolicies(
							22000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), compressedMax),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), compressedMin),
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
					expireAtNanos: 30000,
					result:        policy.DefaultPoliciesList,
				},
			},
			rollupInputs: []testRollupResultsData{
				{
					id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
					matchFrom:     25000,
					matchTo:       25001,
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
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
										policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
										policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
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
					expireAtNanos: 100000,
					result:        policy.DefaultPoliciesList,
				},
			},
			rollupInputs: []testRollupResultsData{
				{
					id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
					matchFrom:     35000,
					matchTo:       35001,
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
					expireAtNanos: timeNanosMax,
					result: policy.PoliciesList{
						policy.NewStagedPolicies(
							100000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
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
					expireAtNanos: timeNanosMax,
					result:        policy.DefaultPoliciesList,
				},
			},
			rollupInputs: []testRollupResultsData{
				{
					id:            "rtagName1=rtagValue1,rtagName2=rtagValue2,rtagName3=rtagValue3",
					matchFrom:     250000,
					matchTo:       250001,
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
					expireAtNanos: timeNanosMax,
					result:        []RollupResult{},
				},
			},
		},
	}

	for _, inputs := range allInputs {
		as := newRuleSet.ActiveSet(inputs.activeSetTime.UnixNano())
		for _, input := range inputs.mappingInputs {
			res := as.ForwardMatch(b(input.id), input.matchFrom, input.matchTo)
			require.Equal(t, input.expireAtNanos, res.expireAtNanos)
			require.Equal(t, input.result, res.MappingsAt(0))
		}
		for _, input := range inputs.rollupInputs {
			res := as.ForwardMatch(b(input.id), input.matchFrom, input.matchTo)
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
				name:         "mappingRule1.snapshot1",
				tombstoned:   false,
				cutoverNanos: 15000,
				filter:       filter1,
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), compressedCount),
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
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), policy.DefaultAggregationID),
					policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID),
				},
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
				name:               "rollupRule5.snapshot1",
				tombstoned:         false,
				cutoverNanos:       24000,
				filter:             filter2,
				lastUpdatedAtNanos: 123456,
				lastUpdatedBy:      "test",
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
				targets: []RollupTarget{
					{
						Name: b("rName5"),
						Tags: [][]byte{b("rtagName1")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), policy.MustCompressAggregationTypes(policy.Sum, policy.P90, policy.P99)),
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour), policy.MustCompressAggregationTypes(policy.Sum, policy.P99)),
						},
					},
				},
			},
		},
	}

	return []*rollupRule{rollupRule1, rollupRule2, rollupRule3, rollupRule4, rollupRule5, rollupRule6, rollupRule7}
}

func testMappingRulesConfig() []*schema.MappingRule {
	return []*schema.MappingRule{
		&schema.MappingRule{
			Uuid: "mappingRule1",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:         "mappingRule1.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 10000,
					TagFilters:   map[string]string{"mtagName1": "mtagValue1"},
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
					Name:         "mappingRule1.snapshot2",
					Tombstoned:   false,
					CutoverNanos: 20000,
					TagFilters:   map[string]string{"mtagName1": "mtagValue1"},
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
					Name:         "mappingRule1.snapshot3",
					Tombstoned:   false,
					CutoverNanos: 30000,
					TagFilters:   map[string]string{"mtagName1": "mtagValue1"},
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
					Name:         "mappingRule2.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 15000,
					TagFilters:   map[string]string{"mtagName1": "mtagValue1"},
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
					Name:         "mappingRule2.snapshot2",
					Tombstoned:   false,
					CutoverNanos: 22000,
					TagFilters:   map[string]string{"mtagName1": "mtagValue1"},
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
					Name:         "mappingRule2.snapshot3",
					Tombstoned:   true,
					CutoverNanos: 35000,
					TagFilters:   map[string]string{"mtagName1": "mtagValue1"},
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
			},
		},
		&schema.MappingRule{
			Uuid: "mappingRule3",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:         "mappingRule3.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 22000,
					TagFilters:   map[string]string{"mtagName1": "mtagValue1"},
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
					Name:         "mappingRule3.snapshot2",
					Tombstoned:   false,
					CutoverNanos: 34000,
					TagFilters:   map[string]string{"mtagName1": "mtagValue1"},
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
					Name:         "mappingRule4.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 24000,
					TagFilters:   map[string]string{"mtagName1": "mtagValue2"},
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
					Name:               "mappingRule5.snapshot1",
					Tombstoned:         false,
					CutoverNanos:       100000,
					LastUpdatedAtNanos: 123456,
					LastUpdatedBy:      "test",
					TagFilters:         map[string]string{"mtagName1": "mtagValue1"},
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
					Name:         "rollupRule1.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 10000,
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
					Name:         "rollupRule1.snapshot2",
					Tombstoned:   false,
					CutoverNanos: 20000,
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
					Name:         "rollupRule1.snapshot3",
					Tombstoned:   false,
					CutoverNanos: 30000,
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
					Name:         "rollupRule2.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 15000,
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
					Name:         "rollupRule2.snapshot2",
					Tombstoned:   false,
					CutoverNanos: 22000,
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
					Name:         "rollupRule2.snapshot3",
					Tombstoned:   true,
					CutoverNanos: 35000,
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
			Uuid: "rollupRule3",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:         "rollupRule3.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 22000,
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
					Name:         "rollupRule3.snapshot2",
					Tombstoned:   false,
					CutoverNanos: 34000,
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
					Name:         "rollupRule4.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 24000,
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
					Name:         "rollupRule5.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 24000,
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
					Name:         "rollupRule6.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 100000,
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
		Uuid:               "ruleset",
		Namespace:          "namespace",
		CreatedAtNanos:     1234,
		LastUpdatedAtNanos: 5678,
		LastUpdatedBy:      "someone",
		Tombstoned:         false,
		CutoverNanos:       34923,
		MappingRules:       testMappingRulesConfig(),
		RollupRules:        testRollupRulesConfig(),
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

func TestMappingRules(t *testing.T) {
	_, rs, _, err := initMutableTest()
	require.NoError(t, err)

	mr, err := rs.MappingRules()
	require.NoError(t, err)
	for _, m := range rs.mappingRules {
		require.Contains(t, mr, m.uuid)
		mrv, err := m.mappingRuleView(len(m.snapshots) - 1)
		require.NoError(t, err)
		require.Equal(t, mr[m.uuid][0], mrv)
	}

	require.NotNil(t, mr)
}

func TestRollupRules(t *testing.T) {
	_, rs, _, err := initMutableTest()
	require.NoError(t, err)

	rr, err := rs.RollupRules()
	require.NoError(t, err)
	for _, r := range rs.rollupRules {
		require.Contains(t, rr, r.uuid)
		rrv, err := r.rollupRuleView(len(r.snapshots) - 1)
		require.NoError(t, err)
		require.Equal(t, rr[r.uuid][0], rrv)
	}

	require.NotNil(t, rr)
}

func TestAddMappingRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)
	_, err = rs.getMappingRuleByName("foo")
	require.Error(t, err)

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	view := MappingRuleView{
		Name:     "foo",
		Filters:  newFilters,
		Policies: p,
	}

	newID, err := mutable.AddMappingRule(view, helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)
	mrs, err := mutable.MappingRules()
	require.NoError(t, err)
	require.Contains(t, mrs, newID)

	mr, err := rs.getMappingRuleByName("foo")
	require.NoError(t, err)

	updated := mrs[mr.uuid][0]
	require.Equal(t, updated.Name, view.Name)
	require.Equal(t, updated.ID, mr.uuid)
	require.Equal(t, updated.Filters, view.Filters)
	require.Equal(t, updated.LastUpdatedAtNanos, now)
	require.Equal(t, updated.LastUpdatedBy, testUser)
}

func TestAddMappingRuleDup(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	m, err := rs.getMappingRuleByName("mappingRule5.snapshot1")
	require.NoError(t, err)
	require.NotNil(t, m)

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	view := MappingRuleView{
		Name:     "mappingRule5.snapshot1",
		Filters:  newFilters,
		Policies: p,
	}

	newID, err := mutable.AddMappingRule(view, helper.NewUpdateMetadata(time.Now().UnixNano(), testUser))
	require.Empty(t, newID)
	require.Error(t, err)
}

func TestAddMappingRuleRevive(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	m, err := rs.getMappingRuleByName("mappingRule5.snapshot1")
	require.NoError(t, err)
	require.NotNil(t, m)

	err = mutable.DeleteMappingRule("mappingRule5", helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)

	newFilters := map[string]string{"test": "bar"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	view := MappingRuleView{
		Name:     "mappingRule5.snapshot1",
		Filters:  newFilters,
		Policies: p,
	}
	newID, err := mutable.AddMappingRule(view, helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)

	mrs, err := mutable.MappingRules()
	require.Contains(t, mrs, newID)
	require.NoError(t, err)

	mr, err := rs.getMappingRuleByID("mappingRule5")
	require.NoError(t, err)
	require.Equal(t, mr.snapshots[len(mr.snapshots)-1].rawFilters, newFilters)

	updated := mrs[mr.uuid][0]
	require.NoError(t, err)
	require.Equal(t, updated.Name, "mappingRule5.snapshot1")
	require.Equal(t, updated.ID, mr.uuid)
	require.Equal(t, updated.Filters, newFilters)
	require.Equal(t, updated.LastUpdatedAtNanos, now)
	require.Equal(t, updated.LastUpdatedBy, testUser)
}

func TestUpdateMappingRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	mutableClone := mutable.Clone()
	require.NoError(t, err)

	mr, err := rs.getMappingRuleByID("mappingRule5")
	require.NoError(t, err)

	mrs, err := rs.MappingRules()
	require.NoError(t, err)
	require.Contains(t, mrs, "mappingRule5")

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	view := MappingRuleView{
		ID:       "mappingRule5",
		Name:     "foo",
		Filters:  newFilters,
		Policies: p,
	}

	err = mutableClone.UpdateMappingRule(view, helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)

	r, err := rs.getMappingRuleByID(mr.uuid)
	require.NoError(t, err)

	mrs, err = mutableClone.MappingRules()
	require.NoError(t, err)
	require.Contains(t, mrs, r.uuid)
	updated := mrs[mr.uuid][0]
	require.NoError(t, err)
	require.Equal(t, updated.Name, "foo")
	require.Equal(t, updated.ID, mr.uuid)
	require.Equal(t, updated.Filters, newFilters)
	require.Equal(t, updated.LastUpdatedAtNanos, now)
	require.Equal(t, updated.LastUpdatedBy, testUser)
}

func TestDeleteMappingRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	mrs, err := rs.MappingRules()
	require.NoError(t, err)
	require.Contains(t, mrs, "mappingRule5")

	m, err := rs.getMappingRuleByID("mappingRule5")
	require.NoError(t, err)
	require.NotNil(t, m)

	err = mutable.DeleteMappingRule("mappingRule5", helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)

	m, err = rs.getMappingRuleByID("mappingRule5")
	require.NoError(t, err)
	require.True(t, m.Tombstoned())
	require.Nil(t, m.snapshots[len(m.snapshots)-1].policies)

	mrs, err = rs.MappingRules()
	require.NoError(t, err)
	require.Contains(t, mrs, "mappingRule5")
}

func TestAddRollupRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	_, err = rs.getRollupRuleByID("foo")
	require.Error(t, err)

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}

	newTargets := []RollupTargetView{
		RollupTargetView{
			Name:     "blah",
			Tags:     []string{"a"},
			Policies: p,
		},
	}
	view := RollupRuleView{
		Name:    "foo",
		Filters: newFilters,
		Targets: newTargets,
	}

	newID, err := mutable.AddRollupRule(view, helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)
	rrs, err := mutable.RollupRules()
	require.Contains(t, rrs, newID)
	require.NoError(t, err)

	rr, err := rs.getRollupRuleByName("foo")
	require.NoError(t, err)
	require.Contains(t, rrs, rr.uuid)

	updated := rrs[rr.uuid][0]
	require.Equal(t, updated.Name, view.Name)
	require.Equal(t, updated.ID, rr.uuid)
	require.Equal(t, updated.Targets, view.Targets)
	require.Equal(t, updated.Filters, view.Filters)
	require.Equal(t, updated.LastUpdatedAtNanos, now)
	require.Equal(t, updated.LastUpdatedBy, testUser)
}

func TestAddRollupRuleDup(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	r, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)
	require.NotNil(t, r)

	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}

	newTargets := []RollupTargetView{
		RollupTargetView{
			Name:     "blah",
			Tags:     []string{"a"},
			Policies: p,
		},
	}
	newFilters := map[string]string{"test": "bar"}
	view := RollupRuleView{
		Name:    "rollupRule5.snapshot1",
		Filters: newFilters,
		Targets: newTargets,
	}
	uuid, err := mutable.AddRollupRule(view, helper.NewUpdateMetadata(now, testUser))
	require.Empty(t, uuid)
	require.Error(t, err)
}

func TestReviveRollupRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	rr, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)

	err = mutable.DeleteRollupRule(rr.uuid, helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)

	rr, err = rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)
	require.True(t, rr.Tombstoned())

	snapshot := rr.snapshots[len(rr.snapshots)-1]

	view := RollupRuleView{
		ID:      rr.uuid,
		Name:    "rollupRule5.snapshot1",
		Filters: snapshot.rawFilters,
		Targets: []RollupTargetView{},
	}

	uuid, err := mutable.AddRollupRule(view, helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)
	require.NotEmpty(t, uuid)
	hist, err := mutable.RollupRules()
	require.NoError(t, err)
	require.Contains(t, hist, uuid)

	rrs, err := rs.RollupRules()
	require.NoError(t, err)
	require.Contains(t, rrs, rr.uuid)

	updated := rrs[rr.uuid][0]

	require.NoError(t, err)
	require.Equal(t, updated.Name, view.Name)
	require.Equal(t, updated.ID, rr.uuid)
	require.Equal(t, updated.Targets, view.Targets)
	require.Equal(t, updated.Filters, view.Filters)
	require.Equal(t, updated.LastUpdatedAtNanos, now)
	require.Equal(t, updated.LastUpdatedBy, testUser)
}

func TestUpdateRollupRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	rr, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)

	newFilters := map[string]string{"tag1": "value", "tag2": "value"}
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), policy.DefaultAggregationID)}
	newTargets := []RollupTargetView{
		RollupTargetView{
			Name:     "blah",
			Tags:     []string{"a"},
			Policies: p,
		},
	}

	view := RollupRuleView{
		ID:      rr.uuid,
		Name:    "foo",
		Filters: newFilters,
		Targets: newTargets,
	}

	err = mutable.UpdateRollupRule(view, helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)

	r, err := rs.getRollupRuleByID(rr.uuid)
	require.NoError(t, err)

	rrs, err := rs.RollupRules()
	require.Contains(t, rrs, r.uuid)

	updated := rrs[r.uuid][0]
	require.NoError(t, err)
	require.Equal(t, updated.Name, "foo")
	require.Equal(t, updated.ID, rr.uuid)
	require.Equal(t, updated.Targets, newTargets)
	require.Equal(t, updated.Filters, newFilters)
	require.Equal(t, updated.LastUpdatedAtNanos, now)
	require.Equal(t, updated.LastUpdatedBy, testUser)

	require.NoError(t, err)
}

func TestDeleteRollupRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	rrs, err := rs.RollupRules()
	require.NoError(t, err)
	require.Contains(t, rrs, "rollupRule5")

	rr, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)

	err = mutable.DeleteRollupRule(rr.uuid, helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)

	rr, err = rs.getRollupRuleByName("rollupRule5.snapshot1")
	require.NoError(t, err)
	require.True(t, rr.Tombstoned())
	require.Nil(t, rr.snapshots[len(rr.snapshots)-1].targets)

	rrs, err = rs.RollupRules()
	require.NoError(t, err)
	require.Contains(t, rrs, "rollupRule5")
}

func TestDeleteRuleset(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	err = mutable.Delete(helper.NewUpdateMetadata(now, testUser))
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

	err = mutable.Delete(helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)

	err = mutable.Revive(helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)

	require.False(t, rs.Tombstoned())
	for _, m := range rs.mappingRules {
		require.True(t, m.Tombstoned())
	}

	for _, r := range rs.rollupRules {
		require.True(t, r.Tombstoned())
	}
}

func TestRuleSetClone(t *testing.T) {
	mutable, rs, _, _ := initMutableTest()
	rsClone := mutable.Clone().(*ruleSet)
	require.Equal(t, rsClone, rs)
	for i, m := range rs.mappingRules {
		require.False(t, m == rsClone.mappingRules[i])
	}
	for i, r := range rs.rollupRules {
		require.False(t, r == rsClone.rollupRules[i])
	}

	rsClone.mappingRules = []*mappingRule{}
	rsClone.rollupRules = []*rollupRule{}
	require.NotEqual(t, rs.mappingRules, rsClone.mappingRules)
	require.NotEqual(t, rs.rollupRules, rsClone.rollupRules)
}

type testMappingsData struct {
	id              string
	matchFrom       int64
	matchTo         int64
	expireAtNanos   int64
	result          policy.PoliciesList
	metricType      metric.Type
	aggregationType policy.AggregationType
}

type testRollupResultsData struct {
	id            string
	matchFrom     int64
	matchTo       int64
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
