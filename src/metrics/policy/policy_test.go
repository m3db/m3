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

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/x/test/testmarshal"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestPolicyString(t *testing.T) {
	inputs := []struct {
		p        Policy
		expected string
	}{
		{p: NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, time.Hour), aggregation.DefaultID), expected: "10s:1h"},
		{p: NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), aggregation.MustCompressTypes(aggregation.Mean, aggregation.P999)), expected: "1m:12h|Mean,P999"},
		{p: NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), aggregation.MustCompressTypes(aggregation.Mean)), expected: "1m:12h|Mean"},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, input.p.String())
	}
}

func TestPolicyMarshalling(t *testing.T) {
	inputs := []struct {
		notCanonical bool
		str          string
		expected     Policy
	}{
		{
			str:      "1s:1h",
			expected: NewPolicy(NewStoragePolicy(time.Second, xtime.Second, time.Hour), aggregation.DefaultID),
		},
		{
			str:      "10s:1d|Mean",
			expected: NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.MustCompressTypes(aggregation.Mean)),
		},
		{
			notCanonical: true,
			str:          "60s:24h|Mean,Count",
			expected:     NewPolicy(NewStoragePolicy(60*time.Second, xtime.Minute, 24*time.Hour), aggregation.MustCompressTypes(aggregation.Mean, aggregation.Count)),
		},
		{
			notCanonical: true,
			str:          "1m:1d|Count,Mean",
			expected:     NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.MustCompressTypes(aggregation.Mean, aggregation.Count)),
		},
		{
			str:      "1m:1d|Mean,Count",
			expected: NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.MustCompressTypes(aggregation.Mean, aggregation.Count)),
		},
		{
			notCanonical: true,
			str:          "1s@1s:1h|P999,P9999",
			expected:     NewPolicy(NewStoragePolicy(time.Second, xtime.Second, time.Hour), aggregation.MustCompressTypes(aggregation.P999, aggregation.P9999)),
		},
	}

	t.Run("roundtrips", func(t *testing.T) {
		examples := make([]Policy, 0, len(inputs))
		for _, ex := range inputs {
			examples = append(examples, ex.expected)
		}

		testmarshal.TestMarshalersRoundtrip(t, examples, []testmarshal.Marshaler{testmarshal.TextMarshaler, testmarshal.JSONMarshaler, testmarshal.YAMLMarshaler})
	})

	t.Run("marshals/text", func(t *testing.T) {
		for _, input := range inputs {
			if input.notCanonical {
				continue
			}
			testmarshal.Require(t, testmarshal.AssertMarshals(t, testmarshal.TextMarshaler, input.expected, []byte(input.str)))
		}
	})

	t.Run("unmarshals/text", func(t *testing.T) {
		for _, input := range inputs {
			testmarshal.Require(t, testmarshal.AssertUnmarshals(t, testmarshal.TextMarshaler, input.expected, []byte(input.str)))
		}
	})

	t.Run("unmarshals/yaml", func(t *testing.T) {
		for _, input := range inputs {
			testmarshal.Require(t, testmarshal.AssertUnmarshals(t, testmarshal.YAMLMarshaler, input.expected, []byte(input.str)))
		}
	})
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

func TestNewPoliciesFromProto(t *testing.T) {
	input := []*policypb.Policy{
		&policypb.Policy{
			StoragePolicy: &policypb.StoragePolicy{
				Resolution: &policypb.Resolution{
					WindowSize: int64(10 * time.Second),
					Precision:  int64(time.Second),
				},
				Retention: &policypb.Retention{
					Period: int64(24 * time.Hour),
				},
			},
			AggregationTypes: []aggregationpb.AggregationType{
				aggregationpb.AggregationType_MEAN,
				aggregationpb.AggregationType_P999,
			},
		},
		&policypb.Policy{
			StoragePolicy: &policypb.StoragePolicy{
				Resolution: &policypb.Resolution{
					WindowSize: int64(time.Minute),
					Precision:  int64(time.Minute),
				},
				Retention: &policypb.Retention{
					Period: int64(240 * time.Hour),
				},
			},
			AggregationTypes: []aggregationpb.AggregationType{
				aggregationpb.AggregationType_MEAN,
				aggregationpb.AggregationType_P9999,
			},
		},
	}

	res, err := NewPoliciesFromProto(input)
	require.NoError(t, err)
	require.Equal(t, []Policy{
		NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.MustCompressTypes(aggregation.Mean, aggregation.P999)),
		NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 240*time.Hour), aggregation.MustCompressTypes(aggregation.Mean, aggregation.P9999)),
	}, res)
}

func TestParsePolicyIntoProto(t *testing.T) {
	inputs := []struct {
		str      string
		expected *policypb.Policy
	}{
		{
			str: "1s:1h",
			expected: &policypb.Policy{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: &policypb.Resolution{
						WindowSize: time.Second.Nanoseconds(),
						Precision:  time.Second.Nanoseconds(),
					},
					Retention: &policypb.Retention{
						Period: time.Hour.Nanoseconds(),
					},
				},
			},
		},
		{
			str: "1s:1h|Mean",
			expected: &policypb.Policy{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: &policypb.Resolution{
						WindowSize: time.Second.Nanoseconds(),
						Precision:  time.Second.Nanoseconds(),
					},
					Retention: &policypb.Retention{
						Period: time.Hour.Nanoseconds(),
					},
				},
				AggregationTypes: []aggregationpb.AggregationType{aggregationpb.AggregationType_MEAN},
			},
		},
		{
			str: "60s:24h|Mean,Count",
			expected: &policypb.Policy{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: &policypb.Resolution{
						WindowSize: time.Minute.Nanoseconds(),
						Precision:  time.Minute.Nanoseconds(),
					},
					Retention: &policypb.Retention{
						Period: 24 * time.Hour.Nanoseconds(),
					},
				},
				AggregationTypes: []aggregationpb.AggregationType{aggregationpb.AggregationType_MEAN, aggregationpb.AggregationType_COUNT},
			},
		},
		{
			str: "1m:1d|Count,Mean",
			expected: &policypb.Policy{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: &policypb.Resolution{
						WindowSize: time.Minute.Nanoseconds(),
						Precision:  time.Minute.Nanoseconds(),
					},
					Retention: &policypb.Retention{
						Period: 24 * time.Hour.Nanoseconds(),
					},
				},
				AggregationTypes: []aggregationpb.AggregationType{aggregationpb.AggregationType_MEAN, aggregationpb.AggregationType_COUNT},
			},
		},
		{
			str: "1m@1s:1h|P999,P9999",
			expected: &policypb.Policy{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: &policypb.Resolution{
						WindowSize: time.Minute.Nanoseconds(),
						Precision:  time.Second.Nanoseconds(),
					},
					Retention: &policypb.Retention{
						Period: time.Hour.Nanoseconds(),
					},
				},
				AggregationTypes: []aggregationpb.AggregationType{aggregationpb.AggregationType_P999, aggregationpb.AggregationType_P9999},
			},
		},
	}

	for _, input := range inputs {
		p, err := ParsePolicy(input.str)
		require.NoError(t, err)

		sp, err := p.Proto()
		require.NoError(t, err)
		require.Equal(t, input.expected, sp, input.str)
	}
}
