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
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/errors"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/models"
	xerrors "github.com/m3db/m3x/errors"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	compressor      = aggregation.NewIDCompressor()
	compressedMax   = compressor.MustCompress(aggregation.Types{aggregation.Max})
	compressedCount = compressor.MustCompress(aggregation.Types{aggregation.Count})
	compressedMin   = compressor.MustCompress(aggregation.Types{aggregation.Min})
	compressedMean  = compressor.MustCompress(aggregation.Types{aggregation.Mean})
	compressedP999  = compressor.MustCompress(aggregation.Types{aggregation.P999})

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
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
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
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
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
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
					},
				),
				policy.NewStagedPolicies(
					22000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
					},
				),
				policy.NewStagedPolicies(
					30000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
					},
				),
				policy.NewStagedPolicies(
					34000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
					},
				),
				policy.NewStagedPolicies(
					35000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
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
		aggregation.NewTypesOptions(),
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
			aggregationType: aggregation.Sum,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					22000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
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
			aggregationType: aggregation.Sum,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					35000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
			aggregationType: aggregation.Sum,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					24000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
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
			aggregationType: aggregation.Sum,
			result:          policy.DefaultPoliciesList,
		},
		{
			id:              "mtagName1=mtagValue3",
			matchFrom:       25000,
			matchTo:         25001,
			expireAtNanos:   30000,
			metricType:      metric.CounterType,
			aggregationType: aggregation.Min,
			result:          nil,
		},
		{
			id:              "mtagName1=mtagValue1",
			matchFrom:       10000,
			matchTo:         12000,
			expireAtNanos:   15000,
			metricType:      metric.CounterType,
			aggregationType: aggregation.Sum,
			result:          nil,
		},
		{
			id:              "mtagName1=mtagValue1",
			matchFrom:       10000,
			matchTo:         21000,
			expireAtNanos:   22000,
			metricType:      metric.CounterType,
			aggregationType: aggregation.Sum,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					20000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
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
			aggregationType: aggregation.Count,
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
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
					},
				),
				policy.NewStagedPolicies(
					22000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
					},
				),
				policy.NewStagedPolicies(
					30000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
					},
				),
				policy.NewStagedPolicies(
					34000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
					},
				),
				policy.NewStagedPolicies(
					35000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
			aggregationType: aggregation.Sum,
			result: policy.PoliciesList{
				policy.DefaultStagedPolicies,
				policy.NewStagedPolicies(
					24000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
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
		aggregation.NewTypesOptions(),
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
			aggregationType: aggregation.Sum,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					24000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), aggregation.DefaultID),
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
			aggregationType: aggregation.Min,
			result:          nil,
		},
		{
			id:              "rName4|rtagName1=rtagValue2",
			matchFrom:       25000,
			matchTo:         25001,
			expireAtNanos:   30000,
			metricType:      metric.UnknownType,
			aggregationType: aggregation.UnknownType,
			result:          nil,
		},
		{
			id:              "rName4|rtagName1=rtagValue2",
			matchFrom:       25000,
			matchTo:         25001,
			expireAtNanos:   30000,
			metricType:      metric.TimerType,
			aggregationType: aggregation.P99,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					24000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), aggregation.DefaultID),
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
			aggregationType: aggregation.Sum,
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
			aggregationType: aggregation.P99,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					120000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), aggregation.MustCompressTypes(aggregation.Sum, aggregation.P90, aggregation.P99)),
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour), aggregation.MustCompressTypes(aggregation.Sum, aggregation.P99)),
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
			aggregationType: aggregation.P90,
			result: policy.PoliciesList{
				policy.NewStagedPolicies(
					120000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), aggregation.MustCompressTypes(aggregation.Sum, aggregation.P90, aggregation.P99)),
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
			aggregationType: aggregation.Last,
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
		aggregation.NewTypesOptions(),
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
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
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
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
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
								policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), aggregation.DefaultID),
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
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
							},
						),
						policy.NewStagedPolicies(
							15000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
							},
						),
						policy.NewStagedPolicies(
							20000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
							},
						),
						policy.NewStagedPolicies(
							22000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
							},
						),
						policy.NewStagedPolicies(
							30000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
							},
						),
						policy.NewStagedPolicies(
							34000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
							},
						),
						policy.NewStagedPolicies(
							35000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(45*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
							},
						),
						policy.NewStagedPolicies(
							38000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(45*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
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
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
							},
						),
						policy.NewStagedPolicies(
							30000,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
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
		aggregation.NewTypesOptions(),
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
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), compressedMin),
								policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
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
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
										policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
										policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
										policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
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
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
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
										policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), aggregation.DefaultID),
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
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
										policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
										policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
										policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), aggregation.DefaultID),
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
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
										policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
										policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
										policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
										policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
										policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), aggregation.DefaultID),
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

func TestRuleSetLatest(t *testing.T) {
	schema := &schema.RuleSet{
		Namespace:    "testNamespace",
		CutoverNanos: 998234,
		MappingRules: testMappingRulesConfig(),
		RollupRules:  testRollupRulesConfig(),
	}
	rs, err := NewRuleSetFromSchema(123, schema, testRuleSetOptions())
	require.NoError(t, err)
	latest, err := rs.Latest()
	require.NoError(t, err)

	expected := &models.RuleSetSnapshotView{
		Namespace:    "testNamespace",
		Version:      123,
		CutoverNanos: 998234,
		MappingRules: map[string]*models.MappingRuleView{
			"mappingRule1": &models.MappingRuleView{
				ID:           "mappingRule1",
				Name:         "mappingRule1.snapshot3",
				Tombstoned:   false,
				CutoverNanos: 30000,
				Filter:       "mtagName1:mtagValue1",
				Policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
				},
			},
			"mappingRule3": &models.MappingRuleView{
				ID:           "mappingRule3",
				Name:         "mappingRule3.snapshot2",
				Tombstoned:   false,
				CutoverNanos: 34000,
				Filter:       "mtagName1:mtagValue1",
				Policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
					policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
				},
			},
			"mappingRule4": &models.MappingRuleView{
				ID:           "mappingRule4",
				Name:         "mappingRule4.snapshot1",
				Tombstoned:   false,
				CutoverNanos: 24000,
				Filter:       "mtagName1:mtagValue2",
				Policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.MustCompressTypes(aggregation.P999)),
				},
			},
			"mappingRule5": &models.MappingRuleView{
				ID:                 "mappingRule5",
				Name:               "mappingRule5.snapshot1",
				Tombstoned:         false,
				CutoverNanos:       100000,
				LastUpdatedAtNanos: 123456,
				LastUpdatedBy:      "test",
				Filter:             "mtagName1:mtagValue1",
				Policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
				},
			},
		},
		RollupRules: map[string]*models.RollupRuleView{
			"rollupRule1": &models.RollupRuleView{
				ID:           "rollupRule1",
				Name:         "rollupRule1.snapshot3",
				Tombstoned:   false,
				CutoverNanos: 30000,
				Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
				Targets: []models.RollupTargetView{
					{
						Name: "rName1",
						Tags: []string{"rtagName1", "rtagName2"},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						},
					},
				},
			},
			"rollupRule3": &models.RollupRuleView{
				ID:           "rollupRule3",
				Name:         "rollupRule3.snapshot2",
				Tombstoned:   false,
				CutoverNanos: 34000,
				Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
				Targets: []models.RollupTargetView{
					{
						Name: "rName1",
						Tags: []string{"rtagName1", "rtagName2"},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
						},
					},
				},
			},
			"rollupRule4": &models.RollupRuleView{
				ID:           "rollupRule4",
				Name:         "rollupRule4.snapshot1",
				Tombstoned:   false,
				CutoverNanos: 24000,
				Filter:       "rtagName1:rtagValue2",
				Targets: []models.RollupTargetView{
					{
						Name: "rName3",
						Tags: []string{"rtagName1", "rtagName2"},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
						},
					},
				},
			},
			"rollupRule5": &models.RollupRuleView{
				ID:           "rollupRule5",
				Name:         "rollupRule5.snapshot1",
				Tombstoned:   false,
				CutoverNanos: 24000,
				Filter:       "rtagName1:rtagValue2",
				Targets: []models.RollupTargetView{
					{
						Name: "rName4",
						Tags: []string{"rtagName1"},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), aggregation.DefaultID),
						},
					},
				},
			},
			"rollupRule6": &models.RollupRuleView{
				ID:           "rollupRule6",
				Name:         "rollupRule6.snapshot1",
				Tombstoned:   false,
				CutoverNanos: 100000,
				Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
				Targets: []models.RollupTargetView{
					{
						Name: "rName3",
						Tags: []string{"rtagName1", "rtagName2"},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
						},
					},
				},
			},
		},
	}
	require.Equal(t, expected, latest)
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
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
					policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
				},
			},
			&mappingRuleSnapshot{
				name:         "mappingRule1.snapshot3",
				tombstoned:   false,
				cutoverNanos: 30000,
				filter:       filter1,
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
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
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
					policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
				},
			},
			&mappingRuleSnapshot{
				name:         "mappingRule2.snapshot3",
				tombstoned:   true,
				cutoverNanos: 35000,
				filter:       filter1,
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
					policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
					policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
					policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
				},
			},
			&mappingRuleSnapshot{
				name:         "mappingRule3.snapshot2",
				tombstoned:   false,
				cutoverNanos: 34000,
				filter:       filter1,
				policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
					policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
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
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
				},
			},
		},
	}

	return []*mappingRule{mappingRule1, mappingRule2, mappingRule3, mappingRule4, mappingRule5}
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
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
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
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
							policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
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
							policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
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
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
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
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
							policy.NewPolicy(policy.NewStoragePolicy(45*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
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
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
							policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
						},
					},
					{
						Name: b("rName2"),
						Tags: [][]byte{b("rtagName1")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
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
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
						},
					},
				},
			},
			&rollupRuleSnapshot{
				name:         "rollupRule3.snapshot3",
				tombstoned:   true,
				cutoverNanos: 38000,
				filter:       filter1,
				targets: []rollupTarget{
					{
						Name: b("rName1"),
						Tags: [][]byte{b("rtagName1"), b("rtagName2")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), aggregation.DefaultID),
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
				targets: []rollupTarget{
					{
						Name: b("rName3"),
						Tags: [][]byte{b("rtagName1"), b("rtagName2")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
				targets: []rollupTarget{
					{
						Name: b("rName4"),
						Tags: [][]byte{b("rtagName1")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute), aggregation.DefaultID),
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
				targets: []rollupTarget{
					{
						Name: b("rName3"),
						Tags: [][]byte{b("rtagName1"), b("rtagName2")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID),
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
				targets: []rollupTarget{
					{
						Name: b("rName5"),
						Tags: [][]byte{b("rtagName1")},
						Policies: []policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), aggregation.MustCompressTypes(aggregation.Sum, aggregation.P90, aggregation.P99)),
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour), aggregation.MustCompressTypes(aggregation.Sum, aggregation.P99)),
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
					Filter:       "mtagName1:mtagValue1",
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
					Filter:       "mtagName1:mtagValue1",
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
					Filter:       "mtagName1:mtagValue1",
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
					Filter:       "mtagName1:mtagValue1",
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
					Filter:       "mtagName1:mtagValue1",
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
					Filter:       "mtagName1:mtagValue1",
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
					Filter:       "mtagName1:mtagValue1",
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
					Filter:       "mtagName1:mtagValue1",
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
					Filter:       "mtagName1:mtagValue2",
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
					Filter:             "mtagName1:mtagValue1",
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
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
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
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
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
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
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
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
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
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
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
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
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
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
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
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
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
					Filter:       "rtagName1:rtagValue2",
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
					Filter:       "rtagName1:rtagValue2",
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
					Filter:       "rtagName1:rtagValue1 rtagName2:rtagValue2",
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

// nolint: unparam
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
	require.Equal(t, err, errRuleNotFound)

	newFilter := "tag1:value tag2:value"
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID)}
	view := models.MappingRuleView{
		Name:     "foo",
		Filter:   newFilter,
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
	require.Equal(t, updated.Filter, view.Filter)
	require.Equal(t, updated.LastUpdatedAtNanos, now)
	require.Equal(t, updated.LastUpdatedBy, testUser)
}

func TestAddMappingRuleInvalidFilter(t *testing.T) {
	mutable, _, helper, err := initMutableTest()
	require.NoError(t, err)

	view := models.MappingRuleView{
		Name:     "testInvalidFilter",
		Filter:   "tag1:value1 tag2:abc[def",
		Policies: []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID)},
	}
	newID, err := mutable.AddMappingRule(view, helper.NewUpdateMetadata(time.Now().UnixNano(), testUser))
	require.Empty(t, newID)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "cannot add rule testInvalidFilter:"))
	_, ok := xerrors.InnerError(err).(errors.ValidationError)
	require.True(t, ok)
}

func TestAddMappingRuleDup(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	m, err := rs.getMappingRuleByName("mappingRule5.snapshot1")
	require.NoError(t, err)
	require.NotNil(t, m)

	newFilter := "tag1:value tag2:value"
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID)}
	view := models.MappingRuleView{
		Name:     "mappingRule5.snapshot1",
		Filter:   newFilter,
		Policies: p,
	}

	newID, err := mutable.AddMappingRule(view, helper.NewUpdateMetadata(time.Now().UnixNano(), testUser))
	require.Empty(t, newID)
	require.Error(t, err)
	containedErr, ok := err.(xerrors.ContainedError)
	require.True(t, ok)
	err = containedErr.InnerError()
	_, ok = err.(errors.RuleConflictError)
	require.True(t, ok)
}

func TestAddMappingRuleRevive(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	m, err := rs.getMappingRuleByName("mappingRule5.snapshot1")
	require.NoError(t, err)
	require.NotNil(t, m)

	err = mutable.DeleteMappingRule("mappingRule5", helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)

	newFilter := "test:bar"
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID)}
	view := models.MappingRuleView{
		Name:     "mappingRule5.snapshot1",
		Filter:   newFilter,
		Policies: p,
	}
	newID, err := mutable.AddMappingRule(view, helper.NewUpdateMetadata(now, testUser))
	require.NoError(t, err)

	mrs, err := mutable.MappingRules()
	require.Contains(t, mrs, newID)
	require.NoError(t, err)

	mr, err := rs.getMappingRuleByID("mappingRule5")
	require.NoError(t, err)
	require.Equal(t, mr.snapshots[len(mr.snapshots)-1].rawFilter, newFilter)

	updated := mrs[mr.uuid][0]
	require.NoError(t, err)
	require.Equal(t, updated.Name, "mappingRule5.snapshot1")
	require.Equal(t, updated.ID, mr.uuid)
	require.Equal(t, updated.Filter, newFilter)
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

	newFilter := "tag1:value tag2:value"
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID)}
	view := models.MappingRuleView{
		ID:       "mappingRule5",
		Name:     "foo",
		Filter:   newFilter,
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
	require.Equal(t, updated.Filter, newFilter)
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
	require.Equal(t, err, errRuleNotFound)

	newFilter := "tag1:value tag2:value"
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID)}

	newTargets := []models.RollupTargetView{
		models.RollupTargetView{
			Name:     "blah",
			Tags:     []string{"a"},
			Policies: p,
		},
	}
	view := models.RollupRuleView{
		Name:    "foo",
		Filter:  newFilter,
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
	require.Equal(t, updated.Filter, view.Filter)
	require.Equal(t, updated.LastUpdatedAtNanos, now)
	require.Equal(t, updated.LastUpdatedBy, testUser)
}

func TestAddRollupRuleDup(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	r, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)
	require.NotNil(t, r)

	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID)}

	newTargets := []models.RollupTargetView{
		models.RollupTargetView{
			Name:     "blah",
			Tags:     []string{"a"},
			Policies: p,
		},
	}
	newFilter := "test:bar"
	view := models.RollupRuleView{
		Name:    "rollupRule5.snapshot1",
		Filter:  newFilter,
		Targets: newTargets,
	}
	uuid, err := mutable.AddRollupRule(view, helper.NewUpdateMetadata(now, testUser))
	require.Empty(t, uuid)
	require.Error(t, err)
	containedErr, ok := err.(xerrors.ContainedError)
	require.True(t, ok)
	err = containedErr.InnerError()
	_, ok = err.(errors.RuleConflictError)
	require.True(t, ok)
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

	view := models.RollupRuleView{
		ID:      rr.uuid,
		Name:    "rollupRule5.snapshot1",
		Filter:  snapshot.rawFilter,
		Targets: []models.RollupTargetView{},
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
	require.Equal(t, updated.Filter, view.Filter)
	require.Equal(t, updated.LastUpdatedAtNanos, now)
	require.Equal(t, updated.LastUpdatedBy, testUser)
}

func TestUpdateRollupRule(t *testing.T) {
	mutable, rs, helper, err := initMutableTest()
	require.NoError(t, err)

	rr, err := rs.getRollupRuleByID("rollupRule5")
	require.NoError(t, err)

	newFilter := "tag1:value tag2:value"
	p := []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), aggregation.DefaultID)}
	newTargets := []models.RollupTargetView{
		models.RollupTargetView{
			Name:     "blah",
			Tags:     []string{"a"},
			Policies: p,
		},
	}

	view := models.RollupRuleView{
		ID:      rr.uuid,
		Name:    "foo",
		Filter:  newFilter,
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
	require.Equal(t, updated.Filter, newFilter)
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
	aggregationType aggregation.Type
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
