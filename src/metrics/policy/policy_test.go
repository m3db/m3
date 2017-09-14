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
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3metrics/generated/proto/schema"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestPolicyString(t *testing.T) {
	inputs := []struct {
		p        Policy
		expected string
	}{
		{p: NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, time.Hour), DefaultAggregationID), expected: "10s@1s:1h0m0s"},
		{p: NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), mustCompress(Mean, P999)), expected: "1m0s@1m:12h0m0s|Mean,P999"},
		{p: NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), mustCompress(Mean)), expected: "1m0s@1m:12h0m0s|Mean"},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, input.p.String())
	}
}

func TestPolicyUnmarshalYAML(t *testing.T) {
	inputs := []struct {
		str      string
		expected Policy
	}{
		{
			str:      "1s:1h",
			expected: NewPolicy(NewStoragePolicy(time.Second, xtime.Second, time.Hour), DefaultAggregationID),
		},
		{
			str:      "10s:1d|Mean",
			expected: NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), mustCompress(Mean)),
		},
		{
			str:      "60s:24h|Mean,Count",
			expected: NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), mustCompress(Mean, Count)),
		},
		{
			str:      "1m:1d|Count,Mean",
			expected: NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), mustCompress(Mean, Count)),
		},
		{
			str:      "1s@1s:1h|P999,P9999",
			expected: NewPolicy(NewStoragePolicy(time.Second, xtime.Second, time.Hour), mustCompress(P999, P9999)),
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
		"|",
		"|Mean",
		"1s:1h|",
		"1s:1h||",
		"1s:1h|P99|",
		"1s:1h|P",
		"1s:1h|Meann",
		"1s:1h|Mean,",
	}
	for _, input := range inputs {
		var p Policy
		require.Error(t, yaml.Unmarshal([]byte(input), &p))
	}
}

func TestNewPoliciesFromSchema(t *testing.T) {
	input := []*schema.Policy{
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
				schema.AggregationType_MEAN,
				schema.AggregationType_P999,
			},
		},
		&schema.Policy{
			StoragePolicy: &schema.StoragePolicy{
				Resolution: &schema.Resolution{
					WindowSize: int64(time.Minute),
					Precision:  int64(time.Minute),
				},
				Retention: &schema.Retention{
					Period: int64(240 * time.Hour),
				},
			},
			AggregationTypes: []schema.AggregationType{
				schema.AggregationType_MEAN,
				schema.AggregationType_P9999,
			},
		},
	}

	res, err := NewPoliciesFromSchema(input)
	require.NoError(t, err)
	require.Equal(t, []Policy{
		NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), mustCompress(Mean, P999)),
		NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 240*time.Hour), mustCompress(Mean, P9999)),
	}, res)
}

func TestParsePolicyIntoSchema(t *testing.T) {
	inputs := []struct {
		str      string
		expected *schema.Policy
	}{
		{
			str: "1s:1h",
			expected: &schema.Policy{
				StoragePolicy: &schema.StoragePolicy{
					Resolution: &schema.Resolution{
						WindowSize: time.Second.Nanoseconds(),
						Precision:  time.Second.Nanoseconds(),
					},
					Retention: &schema.Retention{
						Period: time.Hour.Nanoseconds(),
					},
				},
			},
		},
		{
			str: "1s:1h|Mean",
			expected: &schema.Policy{
				StoragePolicy: &schema.StoragePolicy{
					Resolution: &schema.Resolution{
						WindowSize: time.Second.Nanoseconds(),
						Precision:  time.Second.Nanoseconds(),
					},
					Retention: &schema.Retention{
						Period: time.Hour.Nanoseconds(),
					},
				},
				AggregationTypes: []schema.AggregationType{schema.AggregationType_MEAN},
			},
		},
		{
			str: "60s:24h|Mean,Count",
			expected: &schema.Policy{
				StoragePolicy: &schema.StoragePolicy{
					Resolution: &schema.Resolution{
						WindowSize: time.Minute.Nanoseconds(),
						Precision:  time.Minute.Nanoseconds(),
					},
					Retention: &schema.Retention{
						Period: 24 * time.Hour.Nanoseconds(),
					},
				},
				AggregationTypes: []schema.AggregationType{schema.AggregationType_MEAN, schema.AggregationType_COUNT},
			},
		},
		{
			str: "1m:1d|Count,Mean",
			expected: &schema.Policy{
				StoragePolicy: &schema.StoragePolicy{
					Resolution: &schema.Resolution{
						WindowSize: time.Minute.Nanoseconds(),
						Precision:  time.Minute.Nanoseconds(),
					},
					Retention: &schema.Retention{
						Period: 24 * time.Hour.Nanoseconds(),
					},
				},
				AggregationTypes: []schema.AggregationType{schema.AggregationType_MEAN, schema.AggregationType_COUNT},
			},
		},
		{
			str: "1m@1s:1h|P999,P9999",
			expected: &schema.Policy{
				StoragePolicy: &schema.StoragePolicy{
					Resolution: &schema.Resolution{
						WindowSize: time.Minute.Nanoseconds(),
						Precision:  time.Second.Nanoseconds(),
					},
					Retention: &schema.Retention{
						Period: time.Hour.Nanoseconds(),
					},
				},
				AggregationTypes: []schema.AggregationType{schema.AggregationType_P999, schema.AggregationType_P9999},
			},
		},
	}

	for _, input := range inputs {
		p, err := ParsePolicy(input.str)
		require.NoError(t, err)

		sp, err := p.Schema()
		require.NoError(t, err)
		require.Equal(t, input.expected, sp, input.str)
	}
}

func TestPoliciesByResolutionAsc(t *testing.T) {
	inputs := []Policy{
		NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), DefaultAggregationID),
		NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour), DefaultAggregationID),
		NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour), DefaultAggregationID),
		NewPolicy(NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), DefaultAggregationID),
		NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, time.Hour), DefaultAggregationID),
		NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), DefaultAggregationID),
		NewPolicy(NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), AggregationID{100}),
		NewPolicy(NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), DefaultAggregationID),
		NewPolicy(NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour), AggregationID{100}),
	}
	expected := []Policy{inputs[2], inputs[0], inputs[1], inputs[5], inputs[4], inputs[3], inputs[7], inputs[6], inputs[8]}
	sort.Sort(ByResolutionAsc(inputs))
	require.Equal(t, expected, inputs)
}
