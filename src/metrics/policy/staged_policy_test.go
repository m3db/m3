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

package policy

import (
	"testing"
	"time"

	"github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

func TestStagedPoliciesHasDefaultPolicies(t *testing.T) {
	sp := NewStagedPolicies(testNowNanos, true, nil)
	require.Equal(t, testNowNanos, sp.CutoverNanos)
	_, isDefault := sp.Policies()
	require.True(t, isDefault)
}

func TestStagedPoliciesHasCustomPolicies(t *testing.T) {
	policies := []Policy{
		NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), DefaultAggregationID),
		NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), DefaultAggregationID),
	}
	sp := NewStagedPolicies(testNowNanos, false, policies)
	require.Equal(t, testNowNanos, sp.CutoverNanos)
	actual, isDefault := sp.Policies()
	require.False(t, isDefault)
	require.Equal(t, policies, actual)
}

func TestStagedPoliciesSamePoliciesDefaultPolicies(t *testing.T) {
	inputs := []struct {
		sp       [2]StagedPolicies
		expected bool
	}{
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, nil),
				NewStagedPolicies(0, true, []Policy{}),
			},
			expected: true,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), mustCompress(Lower, Upper)),
				}),
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), mustCompress(Upper, Lower)),
				}),
			},
			expected: true,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), mustCompress(Upper)),
				}),
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), mustCompress(Last)),
				}),
			},
			expected: false,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), mustCompress(Upper)),
				}),
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), mustCompress(Upper, Lower)),
				}),
			},
			expected: false,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, nil),
				NewStagedPolicies(0, true, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), DefaultAggregationID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), DefaultAggregationID),
				}),
			},
			expected: false,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(1000, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), DefaultAggregationID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), DefaultAggregationID),
				}),
				NewStagedPolicies(0, true, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), DefaultAggregationID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), DefaultAggregationID),
				}),
			},
			expected: true,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(1000, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), DefaultAggregationID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), DefaultAggregationID),
				}),
				NewStagedPolicies(0, true, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), DefaultAggregationID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), DefaultAggregationID),
					NewPolicy(NewStoragePolicy(10*time.Minute, xtime.Minute, 24*time.Hour), DefaultAggregationID),
				}),
			},
			expected: false,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, true, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), DefaultAggregationID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), DefaultAggregationID),
					NewPolicy(NewStoragePolicy(10*time.Minute, xtime.Minute, 24*time.Hour), DefaultAggregationID),
				}),
				NewStagedPolicies(1000, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), DefaultAggregationID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), DefaultAggregationID),
				}),
			},
			expected: false,
		},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, input.sp[0].SamePolicies(input.sp[1]))
	}
}

func TestStagedPoliciesIsEmpty(t *testing.T) {
	inputs := []struct {
		sp       StagedPolicies
		expected bool
	}{
		{
			sp:       NewStagedPolicies(0, false, nil),
			expected: true,
		},
		{
			sp:       NewStagedPolicies(0, false, []Policy{}),
			expected: true,
		},
		{
			sp:       NewStagedPolicies(100, false, nil),
			expected: false,
		},
		{
			sp:       NewStagedPolicies(0, true, nil),
			expected: false,
		},
		{
			sp: NewStagedPolicies(0, true, []Policy{
				NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), DefaultAggregationID),
				NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), DefaultAggregationID),
			}),
			expected: false,
		},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, input.sp.IsDefault())
	}
}

func TestPoliciesListIsDefault(t *testing.T) {
	inputs := []struct {
		pl       PoliciesList
		expected bool
	}{
		{
			pl:       DefaultPoliciesList,
			expected: true,
		},
		{
			pl:       []StagedPolicies{},
			expected: false,
		},
		{
			pl: []StagedPolicies{NewStagedPolicies(0, true, []Policy{
				NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), DefaultAggregationID),
				NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), DefaultAggregationID),
			})},
			expected: false,
		},
		{
			pl:       []StagedPolicies{DefaultStagedPolicies, DefaultStagedPolicies},
			expected: false,
		},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, input.pl.IsDefault())
	}
}
