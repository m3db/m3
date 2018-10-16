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
	"encoding/json"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	xtime "github.com/m3db/m3x/time"

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
		NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
		NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
	}
	sp := NewStagedPolicies(testNowNanos, false, policies)
	require.Equal(t, testNowNanos, sp.CutoverNanos)
	actual, isDefault := sp.Policies()
	require.False(t, isDefault)
	require.Equal(t, policies, actual)
}

func TestStagedPoliciesEquals(t *testing.T) {
	inputs := []struct {
		sp       [2]StagedPolicies
		expected bool
	}{
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, nil),
				NewStagedPolicies(0, false, []Policy{}),
			},
			expected: true,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.MustCompressTypes(aggregation.Min, aggregation.Max)),
				}),
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.MustCompressTypes(aggregation.Max, aggregation.Min)),
				}),
			},
			expected: true,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), aggregation.DefaultID),
				}),
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), aggregation.DefaultID),
				}),
			},
			expected: true,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, nil),
				NewStagedPolicies(1, false, nil),
			},
			expected: false,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, nil),
				NewStagedPolicies(0, true, nil),
			},
			expected: false,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.MustCompressTypes(aggregation.Max)),
				}),
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.MustCompressTypes(aggregation.Last)),
				}),
			},
			expected: false,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.MustCompressTypes(aggregation.Max)),
				}),
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.MustCompressTypes(aggregation.Max, aggregation.Min)),
				}),
			},
			expected: false,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, nil),
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), aggregation.DefaultID),
				}),
			},
			expected: false,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(1000, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), aggregation.DefaultID),
				}),
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), aggregation.DefaultID),
					NewPolicy(NewStoragePolicy(10*time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
				}),
			},
			expected: false,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), aggregation.DefaultID),
					NewPolicy(NewStoragePolicy(10*time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
				}),
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), aggregation.DefaultID),
				}),
			},
			expected: false,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), aggregation.DefaultID),
				}),
				NewStagedPolicies(0, false, []Policy{
					NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
					NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
				}),
			},
			expected: false,
		},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, input.sp[0].Equals(input.sp[1]))
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
				NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
				NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), aggregation.DefaultID),
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
				NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
				NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), aggregation.DefaultID),
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

func TestPoliciesListJSONMarshaling(t *testing.T) {
	inputs := []struct {
		pl       PoliciesList
		expected string
	}{
		{
			pl:       PoliciesList{},
			expected: "[]",
		},
		{
			pl:       DefaultPoliciesList,
			expected: `[{"cutoverNanos":0,"tombstoned":false,"policies":null}]`,
		},
		{
			pl: PoliciesList{
				NewStagedPolicies(
					0,
					false,
					[]Policy{
						NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.DefaultID),
						NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.MustCompressTypes(aggregation.Count, aggregation.Mean)),
					},
				),
				NewStagedPolicies(
					100,
					true,
					[]Policy{
						NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.MustCompressTypes(aggregation.Sum)),
					},
				),
				NewStagedPolicies(
					200,
					false,
					[]Policy{},
				),
			},
			expected: `[{"cutoverNanos":0,"tombstoned":false,"policies":["10s:6h","1m:1d|Mean,Count"]},` +
				`{"cutoverNanos":100,"tombstoned":true,"policies":["10s:6h|Sum"]},` +
				`{"cutoverNanos":200,"tombstoned":false,"policies":[]}]`,
		},
	}
	for _, input := range inputs {
		b, err := json.Marshal(input.pl)
		require.NoError(t, err)
		require.Equal(t, input.expected, string(b))
		var unmarshalled PoliciesList
		err = json.Unmarshal(b, &unmarshalled)
		require.NoError(t, err)
		require.Equal(t, input.pl, unmarshalled)
	}
}
