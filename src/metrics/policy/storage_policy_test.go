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
	"testing"
	"time"

	"github.com/m3db/m3metrics/generated/proto/schema"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestStoragePolicyString(t *testing.T) {
	inputs := []struct {
		p        StoragePolicy
		expected string
	}{
		{p: NewStoragePolicy(10*time.Second, xtime.Second, time.Hour), expected: "10s@1s:1h0m0s"},
		{p: NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour), expected: "1m0s@1m:12h0m0s"},
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
	}
	for _, input := range inputs {
		res, err := ParseStoragePolicy(input.str)
		require.NoError(t, err)
		require.Equal(t, input.expected, res)
	}
}

func TestParseStoragePolicyErrors(t *testing.T) {
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
		_, err := ParseStoragePolicy(input)
		require.Error(t, err)
	}
}

func TestStoragePolicyUnmarshalYAML(t *testing.T) {
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
	}
	for _, input := range inputs {
		var p StoragePolicy
		require.NoError(t, yaml.Unmarshal([]byte(input.str), &p))
		require.Equal(t, input.expected, p)
	}
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
		"0s:1d",
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

func TestNewStoragePolicyFromSchema(t *testing.T) {
	inputs := []struct {
		s *schema.StoragePolicy
		p StoragePolicy
	}{
		{
			s: &schema.StoragePolicy{
				Resolution: &schema.Resolution{
					WindowSize: int64(10 * time.Second),
					Precision:  int64(time.Second),
				},
				Retention: &schema.Retention{
					Period: int64(24 * time.Hour),
				},
			},
			p: NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		},
		{
			s: &schema.StoragePolicy{
				Resolution: &schema.Resolution{
					WindowSize: int64(time.Minute),
					Precision:  int64(time.Minute),
				},
				Retention: &schema.Retention{
					Period: int64(240 * time.Hour),
				},
			},
			p: NewStoragePolicy(time.Minute, xtime.Minute, 240*time.Hour),
		},
	}

	for _, input := range inputs {
		res, err := NewStoragePolicyFromSchema(input.s)
		require.NoError(t, err)
		require.Equal(t, input.p, res)
	}
}
