// Copyright (c) 2016 Uber Technologies, Inc.
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
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestPolicyString(t *testing.T) {
	inputs := []struct {
		p        Policy
		expected string
	}{
		{p: NewPolicy(10*time.Second, xtime.Second, time.Hour), expected: "10s@1s:1h0m0s"},
		{p: NewPolicy(time.Minute, xtime.Minute, 12*time.Hour), expected: "1m0s@1m:12h0m0s"},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, input.p.String())
	}
}

func TestParsePolicy(t *testing.T) {
	inputs := []struct {
		str      string
		expected Policy
	}{
		{
			str:      "1s:1h",
			expected: NewPolicy(time.Second, xtime.Second, time.Hour),
		},
		{
			str:      "10s:1d",
			expected: NewPolicy(10*time.Second, xtime.Second, 24*time.Hour),
		},
		{
			str:      "60s:24h",
			expected: NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
		{
			str:      "1m:1d",
			expected: NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
		{
			str:      "1s@1s:1h",
			expected: NewPolicy(time.Second, xtime.Second, time.Hour),
		},
		{
			str:      "10s@1s:1d",
			expected: NewPolicy(10*time.Second, xtime.Second, 24*time.Hour),
		},
		{
			str:      "60s@1s:24h",
			expected: NewPolicy(time.Minute, xtime.Second, 24*time.Hour),
		},
		{
			str:      "1m@1m:1d",
			expected: NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
	}
	for _, input := range inputs {
		res, err := ParsePolicy(input.str)
		require.NoError(t, err)
		require.Equal(t, input.expected, res)
	}
}

func TestParsePolicyErrors(t *testing.T) {
	inputs := []string{
		"1s:1s:1s",
		"0s:1d",
		"10seconds:1s",
		"10seconds@1s:1d",
		"10s@2s:1d",
		"0.1s@1s:1d",
		"10s@2minutes:2d",
	}
	for _, input := range inputs {
		_, err := ParsePolicy(input)
		require.Error(t, err)
	}
}

func TestPolicyUnmarshalYAML(t *testing.T) {
	inputs := []struct {
		str      string
		expected Policy
	}{
		{
			str:      "1s:1h",
			expected: NewPolicy(time.Second, xtime.Second, time.Hour),
		},
		{
			str:      "10s:1d",
			expected: NewPolicy(10*time.Second, xtime.Second, 24*time.Hour),
		},
		{
			str:      "60s:24h",
			expected: NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
		{
			str:      "1m:1d",
			expected: NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
		{
			str:      "1s@1s:1h",
			expected: NewPolicy(time.Second, xtime.Second, time.Hour),
		},
		{
			str:      "10s@1s:1d",
			expected: NewPolicy(10*time.Second, xtime.Second, 24*time.Hour),
		},
		{
			str:      "60s@1s:24h",
			expected: NewPolicy(time.Minute, xtime.Second, 24*time.Hour),
		},
		{
			str:      "1m@1m:1d",
			expected: NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
	}
	for _, input := range inputs {
		var p Policy
		require.NoError(t, yaml.Unmarshal([]byte(input.str), &p))
		require.Equal(t, input.expected, p)
	}
}

func TestPolicyUnmarshalYAMLErrors(t *testing.T) {
	inputs := []string{
		"1s:1s:1s",
		"0s:1d",
		"10seconds:1s",
		"10seconds@1s:1d",
		"10s@2s:1d",
		"0.1s@1s:1d",
		"10s@2minutes:2d",
	}
	for _, input := range inputs {
		var p Policy
		require.Error(t, yaml.Unmarshal([]byte(input), &p))
	}
}

func TestPoliciesByResolutionAsc(t *testing.T) {
	inputs := []Policy{
		NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
		NewPolicy(10*time.Second, xtime.Second, 2*time.Hour),
		NewPolicy(10*time.Second, xtime.Second, 12*time.Hour),
		NewPolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		NewPolicy(time.Minute, xtime.Minute, time.Hour),
		NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
		NewPolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
	}
	expected := []Policy{inputs[2], inputs[0], inputs[1], inputs[5], inputs[4], inputs[3], inputs[6]}
	sort.Sort(ByResolutionAsc(inputs))
	require.Equal(t, expected, inputs)
}

func TestStagedPoliciesHasDefaultPolicies(t *testing.T) {
	sp := NewStagedPolicies(testNowNanos, true, nil)
	require.Equal(t, testNowNanos, sp.CutoverNanos)
	_, isDefault := sp.Policies()
	require.True(t, isDefault)
}

func TestStagedPoliciesHasCustomPolicies(t *testing.T) {
	policies := []Policy{
		NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
		NewPolicy(10*time.Second, xtime.Second, 2*time.Hour),
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
				NewStagedPolicies(0, false, nil),
				NewStagedPolicies(0, true, []Policy{
					NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
					NewPolicy(time.Minute, xtime.Minute, 12*time.Hour),
				}),
			},
			expected: false,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(1000, false, []Policy{
					NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
					NewPolicy(time.Minute, xtime.Minute, 12*time.Hour),
				}),
				NewStagedPolicies(0, true, []Policy{
					NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
					NewPolicy(time.Minute, xtime.Minute, 12*time.Hour),
				}),
			},
			expected: true,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(1000, false, []Policy{
					NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
					NewPolicy(time.Minute, xtime.Minute, 12*time.Hour),
				}),
				NewStagedPolicies(0, true, []Policy{
					NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
					NewPolicy(time.Minute, xtime.Minute, 12*time.Hour),
					NewPolicy(10*time.Minute, xtime.Minute, 24*time.Hour),
				}),
			},
			expected: false,
		},
		{
			sp: [2]StagedPolicies{
				NewStagedPolicies(0, true, []Policy{
					NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
					NewPolicy(time.Minute, xtime.Minute, 12*time.Hour),
					NewPolicy(10*time.Minute, xtime.Minute, 24*time.Hour),
				}),
				NewStagedPolicies(1000, false, []Policy{
					NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
					NewPolicy(time.Minute, xtime.Minute, 12*time.Hour),
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
				NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
				NewPolicy(time.Minute, xtime.Minute, 12*time.Hour),
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
				NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
				NewPolicy(time.Minute, xtime.Minute, 12*time.Hour),
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
