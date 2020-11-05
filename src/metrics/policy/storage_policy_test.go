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
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/x/test/testmarshal"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

var (
	testStoragePolicy      = NewStoragePolicy(10*time.Second, xtime.Second, time.Hour)
	testBadStoragePolicy   = NewStoragePolicy(10*time.Second, xtime.Unit(100), time.Hour)
	testStoragePolicyProto = policypb.StoragePolicy{
		Resolution: policypb.Resolution{
			WindowSize: (10 * time.Second).Nanoseconds(),
			Precision:  time.Second.Nanoseconds(),
		},
		Retention: policypb.Retention{
			Period: time.Hour.Nanoseconds(),
		},
	}
	testStoragePolicyProtoNoResolution = policypb.StoragePolicy{
		Retention: policypb.Retention{
			Period: time.Hour.Nanoseconds(),
		},
	}
	testStoragePolicyProtoNoRetention = policypb.StoragePolicy{
		Resolution: policypb.Resolution{
			WindowSize: (10 * time.Second).Nanoseconds(),
			Precision:  time.Second.Nanoseconds(),
		},
	}
	testStoragePolicyProtoBadPrecision = policypb.StoragePolicy{
		Resolution: policypb.Resolution{
			WindowSize: (10 * time.Second).Nanoseconds(),
			Precision:  2,
		},
		Retention: policypb.Retention{
			Period: time.Hour.Nanoseconds(),
		},
	}
)

func TestStoragePolicyString(t *testing.T) {
	inputs := []struct {
		p        StoragePolicy
		expected string
	}{
		{p: NewStoragePolicy(10*time.Second, xtime.Second, time.Hour), expected: "10s:1h"},
		{p: NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), expected: "1m:12h"},
		{p: NewStoragePolicy(time.Minute, xtime.Second, 12*time.Hour), expected: "1m@1s:12h"},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, input.p.String())
	}
}

func TestParseStoragePolicy(t *testing.T) {
	inputs := []struct {
		str      string
		expected StoragePolicy
	}{
		{
			str:      "1s:1h",
			expected: NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		},
		{
			str:      "10s:1d",
			expected: NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		},
		{
			str:      "60s:24h",
			expected: NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
		{
			str:      "1m:1d",
			expected: NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
		{
			str:      "1s@1s:1h",
			expected: NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		},
		{
			str:      "10s@1s:1d",
			expected: NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		},
		{
			str:      "60s@1s:24h",
			expected: NewStoragePolicy(time.Minute, xtime.Second, 24*time.Hour),
		},
		{
			str:      "1m@1m:1d",
			expected: NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
		{
			str:      "1h0m0s@1h0m0s:24h0m0s",
			expected: NewStoragePolicy(time.Hour, xtime.Hour, 24*time.Hour),
		},
		{
			str:      "1h:24h",
			expected: NewStoragePolicy(time.Hour, xtime.Hour, 24*time.Hour),
		},
	}
	for _, input := range inputs {
		res, err := ParseStoragePolicy(input.str)
		require.NoError(t, err)
		require.Equal(t, input.expected, res)
	}
}

func TestStoragePolicyParseRoundTrip(t *testing.T) {
	inputs := []StoragePolicy{
		NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Second, 24*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
	}

	for _, input := range inputs {
		str := input.String()
		parsed, err := ParseStoragePolicy(str)
		require.NoError(t, err)
		require.Equal(t, input, parsed)
	}
}

func TestParseStoragePolicyErrors(t *testing.T) {
	inputs := []string{
		"1s:1s:1s",
		"10seconds:1s",
		"10seconds@1s:1d",
		"10s@2s:1d",
		"0.1s@1s:1d",
		"10s@2minutes:2d",
	}
	for _, input := range inputs {
		_, err := ParseStoragePolicy(input)
		require.Error(t, err)
	}
}

func TestStoragePolicyMarshalJSON(t *testing.T) {
	inputs := []struct {
		storagePolicy StoragePolicy
		expected      string
	}{
		{
			storagePolicy: NewStoragePolicy(time.Second, xtime.Second, time.Hour),
			expected:      "\"1s:1h\"",
		},
		{
			storagePolicy: NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
			expected:      "\"10s:1d\"",
		},
		{
			storagePolicy: NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
			expected:      "\"1m:1d\"",
		},
		{
			storagePolicy: NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
			expected:      "\"1m:1d\"",
		},
		{
			storagePolicy: NewStoragePolicy(time.Second, xtime.Second, time.Hour),
			expected:      "\"1s:1h\"",
		},
		{
			storagePolicy: NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
			expected:      "\"10s:1d\"",
		},
		{
			storagePolicy: NewStoragePolicy(time.Minute, xtime.Second, 24*time.Hour),
			expected:      "\"1m@1s:1d\"",
		},
		{
			storagePolicy: NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
			expected:      "\"1m:1d\"",
		},
	}

	for _, input := range inputs {
		res, err := json.Marshal(input.storagePolicy)
		require.NoError(t, err)
		require.Equal(t, input.expected, string(res))
	}
}

func TestStoragePolicyUnmarshalJSON(t *testing.T) {
	inputs := []struct {
		str      string
		expected StoragePolicy
	}{
		{
			str:      "\"1s:1h\"",
			expected: NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		},
		{
			str:      "\"10s:1d\"",
			expected: NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		},
		{
			str:      "\"60s:24h\"",
			expected: NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
		{
			str:      "\"1m:1d\"",
			expected: NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
		{
			str:      "\"1s@1s:1h\"",
			expected: NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		},
		{
			str:      "\"10s@1s:1d\"",
			expected: NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		},
		{
			str:      "\"60s@1s:24h\"",
			expected: NewStoragePolicy(time.Minute, xtime.Second, 24*time.Hour),
		},
		{
			str:      "\"1m@1m:1d\"",
			expected: NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
	}

	for _, input := range inputs {
		var p StoragePolicy
		require.NoError(t, json.Unmarshal([]byte(input.str), &p))
		require.Equal(t, input.expected, p)
	}
}

func TestStoragePolicyUnmarshalJSONError(t *testing.T) {
	inputs := []string{
		"1m:1d",
		"1m",
		"1d",
	}

	for _, input := range inputs {
		var p StoragePolicy
		require.Error(t, json.Unmarshal([]byte(input), &p))
	}
}

func TestStoragePolicyMarshalRoundtrip(t *testing.T) {
	inputs := StoragePolicies{
		NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Second, 24*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
	}

	testmarshal.TestMarshalersRoundtrip(t, inputs,
		[]testmarshal.Marshaler{
			testmarshal.YAMLMarshaler,
			testmarshal.TextMarshaler,
			testmarshal.JSONMarshaler})
}

func TestStoragePolicyYAMLMarshal(t *testing.T) {
	inputs := []struct {
		str          string
		expected     StoragePolicy
		notCanonical bool
	}{
		{
			str:      "1s:1h",
			expected: NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		},
		{
			str:      "10s:1d",
			expected: NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		},
		{
			str:          "60s:24h",
			notCanonical: true,
			expected:     NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
		{
			str:      "1m:1d",
			expected: NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
		{
			str:          "1s@1s:1h",
			notCanonical: true,
			expected:     NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		},
		{
			str:          "10s@1s:1d",
			notCanonical: true,
			expected:     NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		},
		{
			str:          "60s@1s:24h",
			notCanonical: true,
			expected:     NewStoragePolicy(time.Minute, xtime.Second, 24*time.Hour),
		},
		{
			str:          "1m@1m:1d",
			notCanonical: true,
			expected:     NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		},
		{
			str:      "2m@1ms:1d",
			expected: NewStoragePolicy(2*time.Minute, 1*xtime.Millisecond, 24*time.Hour),
		},
	}

	t.Run("marshals", func(t *testing.T) {
		for _, input := range inputs {
			canonical := !input.notCanonical
			if canonical {
				testmarshal.AssertMarshals(t, testmarshal.YAMLMarshaler, input.expected, []byte(input.str+"\n"))
			}
		}
	})

	t.Run("unmarshals", func(t *testing.T) {
		for _, input := range inputs {
			testmarshal.AssertUnmarshals(t, testmarshal.YAMLMarshaler, input.expected, []byte(input.str))
		}
	})

}

func TestMustParseStoragePolicy(t *testing.T) {
	inputs := []struct {
		str         string
		shouldPanic bool
		expected    StoragePolicy
	}{
		{
			str:         "1s:1h",
			shouldPanic: false,
			expected:    NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		},
		{
			str:         "10seconds:1d",
			shouldPanic: true,
		},
	}
	for _, input := range inputs {
		if input.shouldPanic {
			require.Panics(t, func() { MustParseStoragePolicy(input.str) })
		} else {
			require.Equal(t, input.expected, MustParseStoragePolicy(input.str))
		}
	}
}

func TestStoragePolicyUnmarshalYAMLErrors(t *testing.T) {
	inputs := []string{
		"1s:1s:1s",
		"10seconds:1s",
		"10seconds@1s:1d",
		"10s@2s:1d",
		"0.1s@1s:1d",
		"10s@2minutes:2d",
	}
	for _, input := range inputs {
		var p StoragePolicy
		require.Error(t, yaml.Unmarshal([]byte(input), &p))
	}
}

func TestNewStoragePolicyFromProto(t *testing.T) {
	inputs := []struct {
		s *policypb.StoragePolicy
		p StoragePolicy
	}{
		{
			s: &policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: int64(10 * time.Second),
					Precision:  int64(time.Second),
				},
				Retention: policypb.Retention{
					Period: int64(24 * time.Hour),
				},
			},
			p: NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		},
		{
			s: &policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: int64(time.Minute),
					Precision:  int64(time.Minute),
				},
				Retention: policypb.Retention{
					Period: int64(240 * time.Hour),
				},
			},
			p: NewStoragePolicy(time.Minute, xtime.Minute, 240*time.Hour),
		},
	}

	for _, input := range inputs {
		res, err := NewStoragePolicyFromProto(input.s)
		require.NoError(t, err)
		require.Equal(t, input.p, res)
	}
}

func TestStoragePolicyToProto(t *testing.T) {
	var pb policypb.StoragePolicy
	require.NoError(t, testStoragePolicy.ToProto(&pb))
	require.Equal(t, testStoragePolicyProto, pb)
}

func TestStoragePolicyToProtoBadStoragePolicy(t *testing.T) {
	var pb policypb.StoragePolicy
	require.Error(t, testBadStoragePolicy.ToProto(&pb))
}

func TestStoragePolicyFromProto(t *testing.T) {
	var res StoragePolicy
	require.NoError(t, res.FromProto(testStoragePolicyProto))
	require.Equal(t, testStoragePolicy, res)
}

func TestStoragePolicyFromProtoNilResolution(t *testing.T) {
	var res StoragePolicy
	require.Equal(t, errNilResolutionProto, res.FromProto(testStoragePolicyProtoNoResolution))
}

func TestStoragePolicyFromProtoNilRetention(t *testing.T) {
	var res StoragePolicy
	require.Equal(t, errNilRetentionProto, res.FromProto(testStoragePolicyProtoNoRetention))
}

func TestStoragePolicyFromProtoBadPrecision(t *testing.T) {
	var res StoragePolicy
	require.Error(t, res.FromProto(testStoragePolicyProtoBadPrecision))
}

func TestStoragePolicyProtoRoundTrip(t *testing.T) {
	var (
		pb  policypb.StoragePolicy
		res StoragePolicy
	)
	require.NoError(t, testStoragePolicy.ToProto(&pb))
	require.NoError(t, res.FromProto(pb))
	require.Equal(t, testStoragePolicy, res)
}

func TestStoragePoliciesJSONMarshal(t *testing.T) {
	input := StoragePolicies{
		NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
	}

	b, err := json.Marshal(input)
	require.NoError(t, err)
	expected := "[\"1s:1h\",\"10s:1d\",\"1m:1d\"]"
	require.Equal(t, expected, string(b))
}

func TestStoragePoliciesJSONUnMarshal(t *testing.T) {
	input := "[\"1s:1h\",\"10s:1d\",\"1m:1d\"]"
	var storagePolicies StoragePolicies
	require.NoError(t, json.Unmarshal([]byte(input), &storagePolicies))
	expected := StoragePolicies{
		NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
	}
	require.Equal(t, expected, storagePolicies)
}

func TestStoragePoliciesRoundtrip(t *testing.T) {
	input := StoragePolicies{
		NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
	}

	testmarshal.TestMarshalersRoundtrip(t, []StoragePolicies{input}, []testmarshal.Marshaler{testmarshal.JSONMarshaler, testmarshal.YAMLMarshaler})
}

func TestStoragePoliciesByResolutionAscRetentionDesc(t *testing.T) {
	inputs := StoragePolicies{
		NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
		NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
		NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
		NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
	}
	sort.Sort(ByResolutionAscRetentionDesc(inputs))

	expected := StoragePolicies{
		NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
		NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
		NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
		NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
	}
	require.Equal(t, expected, inputs)
}

func TestStoragePoliciesByRetentionAscResolutionAsc(t *testing.T) {
	inputs := StoragePolicies{
		NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
		NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
		NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Minute, 12*time.Hour),
		NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
		NewStoragePolicy(11*time.Second, xtime.Second, 2*time.Hour),
	}
	sort.Sort(ByRetentionAscResolutionAsc(inputs))

	expected := StoragePolicies{
		NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 2*time.Hour),
		NewStoragePolicy(11*time.Second, xtime.Second, 2*time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
		NewStoragePolicy(10*time.Second, xtime.Minute, 12*time.Hour),
		NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
		NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
		NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
		NewStoragePolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
	}
	require.Equal(t, expected, inputs)
}
