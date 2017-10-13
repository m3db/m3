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

	"github.com/stretchr/testify/require"
)

func TestValidRetentionValue(t *testing.T) {
	inputs := []RetentionValue{
		OneHour,
		SixHours,
		TwelveHours,
		OneDay,
		TwoDays,
		SevenDays,
		FourteenDays,
		ThirtyDays,
		FourtyFiveDays,
	}
	expected := []time.Duration{
		time.Hour,
		6 * time.Hour,
		12 * time.Hour,
		24 * time.Hour,
		2 * 24 * time.Hour,
		7 * 24 * time.Hour,
		14 * 24 * time.Hour,
		30 * 24 * time.Hour,
		45 * 24 * time.Hour,
	}
	for i, value := range inputs {
		retention, err := value.Retention()
		require.NoError(t, err)
		require.Equal(t, expected[i], retention.Duration())
		require.Equal(t, Retention(expected[i]), retention)
		require.True(t, value.IsValid())
	}
}

func TestInvalidRetentionValue(t *testing.T) {
	inputs := []RetentionValue{
		UnknownRetentionValue,
		RetentionValue(100),
	}
	for _, value := range inputs {
		_, err := value.Retention()
		require.Equal(t, errUnknownRetentionValue, err)
		require.False(t, value.IsValid())
	}
}

func TestValidRetention(t *testing.T) {
	inputs := []Retention{
		Retention(time.Hour),
		Retention(6 * time.Hour),
		Retention(12 * time.Hour),
		Retention(24 * time.Hour),
		Retention(2 * 24 * time.Hour),
		Retention(7 * 24 * time.Hour),
		Retention(14 * 24 * time.Hour),
		Retention(30 * 24 * time.Hour),
		Retention(45 * 24 * time.Hour),
	}
	expected := []RetentionValue{
		OneHour,
		SixHours,
		TwelveHours,
		OneDay,
		TwoDays,
		SevenDays,
		FourteenDays,
		ThirtyDays,
		FourtyFiveDays,
	}
	for i, retention := range inputs {
		value, err := ValueFromRetention(retention)
		require.NoError(t, err)
		require.Equal(t, expected[i], value)
	}
}

func TestInvalidRetention(t *testing.T) {
	inputs := []Retention{
		Retention(time.Nanosecond),
		Retention(37 * time.Hour),
	}
	for _, retention := range inputs {
		_, err := ValueFromRetention(retention)
		require.Equal(t, errUnknownRetention, err)
	}
}

func TestParseRetention(t *testing.T) {
	inputs := []struct {
		str      string
		expected Retention
	}{
		{
			str:      "1s",
			expected: Retention(time.Second),
		},
		{
			str:      "10s",
			expected: Retention(10 * time.Second),
		},
		{
			str:      "1m",
			expected: Retention(time.Minute),
		},
		{
			str:      "10m",
			expected: Retention(10 * time.Minute),
		},
		{
			str:      "1h",
			expected: Retention(time.Hour),
		},
		{
			str:      "1d",
			expected: Retention(24 * time.Hour),
		},
		{
			str:      "1w",
			expected: Retention(24 * 7 * time.Hour),
		},
		{
			str:      "1mon",
			expected: Retention(24 * 30 * time.Hour),
		},
		{
			str:      "6mon",
			expected: Retention(24 * 180 * time.Hour),
		},
		{
			str:      "1y",
			expected: Retention(24 * 365 * time.Hour),
		},
		{
			str:      "24h0m0s",
			expected: Retention(24 * time.Hour),
		},
	}

	for _, input := range inputs {
		res, err := ParseRetention(input.str)
		require.NoError(t, err)
		require.Equal(t, input.expected, res)
	}
}

func TestParseRetentionError(t *testing.T) {
	inputs := []string{
		"4",
		"",
		"4minutes",
		"4d3",
		",3490",
	}

	for _, input := range inputs {
		_, err := ParseRetention(input)
		require.Error(t, err)
	}
}

func TestRetentionString(t *testing.T) {
	inputs := []struct {
		retention Retention
		expected  string
	}{
		{
			retention: Retention(time.Second),
			expected:  "1s",
		},
		{
			retention: Retention(10 * time.Second),
			expected:  "10s",
		},
		{
			retention: Retention(time.Minute),
			expected:  "1m",
		},
		{
			retention: Retention(10 * time.Minute),
			expected:  "10m",
		},
		{
			retention: Retention(time.Hour),
			expected:  "1h",
		},
		{
			retention: Retention(24 * time.Hour),
			expected:  "1d",
		},
		{
			retention: Retention(24 * 7 * time.Hour),
			expected:  "1w",
		},
		{
			retention: Retention(24 * 30 * time.Hour),
			expected:  "1mon",
		},
		{
			retention: Retention(24 * 180 * time.Hour),
			expected:  "6mon",
		},
		{
			retention: Retention(24 * 365 * time.Hour),
			expected:  "1y",
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.retention.String())
	}
}

func TestRetentionRoundTrip(t *testing.T) {
	inputs := []Retention{
		Retention(time.Second),
		Retention(10 * time.Second),
		Retention(time.Minute),
		Retention(10 * time.Minute),
		Retention(time.Hour),
		Retention(24 * time.Hour),
		Retention(24 * 7 * time.Hour),
		Retention(24 * 30 * time.Hour),
		Retention(24 * 180 * time.Hour),
		Retention(24 * 365 * time.Hour),
	}

	for _, input := range inputs {
		str := input.String()
		res, err := ParseRetention(str)
		require.NoError(t, err)
		require.Equal(t, input, res)
	}
}
