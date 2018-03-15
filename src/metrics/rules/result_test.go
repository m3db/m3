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

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestMatchResultHasExpired(t *testing.T) {
	r := NewMatchResult(0, 1000, nil, nil)
	require.False(t, r.HasExpired(0))
	require.True(t, r.HasExpired(1000))
}

func TestMatchResult(t *testing.T) {
	var (
		testExpireAtNanos  = int64(67890)
		testResultMappings = policy.PoliciesList{
			policy.NewStagedPolicies(
				12345,
				false,
				[]policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
					policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
					policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
				},
			),
			policy.NewStagedPolicies(
				23456,
				true,
				[]policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 10*time.Hour), aggregation.DefaultID),
					policy.NewPolicy(policy.NewStoragePolicy(2*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
				},
			),
		}
		testResultRollups = []RollupResult{
			{
				ID: b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
				PoliciesList: policy.PoliciesList{
					policy.NewStagedPolicies(
						12345,
						false,
						[]policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), aggregation.DefaultID),
							policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
							policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
						},
					),
					policy.NewStagedPolicies(
						23456,
						false,
						[]policy.Policy{
							policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 10*time.Hour), aggregation.DefaultID),
							policy.NewPolicy(policy.NewStoragePolicy(2*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
						},
					),
				},
			},
			{
				ID:           b("rName2|rtagName1=rtagValue1"),
				PoliciesList: policy.PoliciesList{policy.NewStagedPolicies(12345, false, nil)},
			},
			{
				ID: b("rName3|rtagName1=rtagValue2"),
				PoliciesList: policy.PoliciesList{
					policy.NewStagedPolicies(12345, false, nil),
					policy.NewStagedPolicies(27000, true, nil),
				},
			},
		}
	)

	inputs := []struct {
		matchAtNanos       int64
		expectedMappings   policy.PoliciesList
		expectedRollups    []RollupResult
		expectedTombstoned []bool
	}{
		{
			matchAtNanos:       0,
			expectedMappings:   testResultMappings,
			expectedRollups:    testResultRollups,
			expectedTombstoned: []bool{false, false, false},
		},
		{
			matchAtNanos:       20000,
			expectedMappings:   testResultMappings,
			expectedRollups:    testResultRollups,
			expectedTombstoned: []bool{false, false, false},
		},
		{
			matchAtNanos: 30000,
			expectedMappings: policy.PoliciesList{
				policy.NewStagedPolicies(
					23456,
					true,
					[]policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 10*time.Hour), aggregation.DefaultID),
						policy.NewPolicy(policy.NewStoragePolicy(2*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
					},
				),
			},
			expectedRollups: []RollupResult{
				{
					ID: b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
					PoliciesList: policy.PoliciesList{
						policy.NewStagedPolicies(
							23456,
							false,
							[]policy.Policy{
								policy.NewPolicy(policy.NewStoragePolicy(30*time.Second, xtime.Second, 10*time.Hour), aggregation.DefaultID),
								policy.NewPolicy(policy.NewStoragePolicy(2*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
							},
						),
					},
				},
				{
					ID:           b("rName2|rtagName1=rtagValue1"),
					PoliciesList: policy.PoliciesList{policy.NewStagedPolicies(12345, false, nil)},
				},
				emptyRollupResult,
			},
			expectedTombstoned: []bool{false, false, true},
		},
	}

	res := NewMatchResult(0, testExpireAtNanos, testResultMappings, testResultRollups)
	for _, input := range inputs {
		require.Equal(t, input.expectedMappings, res.MappingsAt(input.matchAtNanos))
		require.Equal(t, len(input.expectedRollups), res.NumRollups())
		for i := 0; i < len(input.expectedRollups); i++ {
			rollupRes, tombstoned := res.RollupsAt(i, input.matchAtNanos)
			require.Equal(t, input.expectedRollups[i], rollupRes)
			require.Equal(t, input.expectedTombstoned[i], tombstoned)
		}
	}
}
