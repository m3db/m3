// Copyright (c) 2019 Uber Technologies, Inc.
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

package time

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseExtendedDuration(t *testing.T) {
	const day = 24 * time.Hour
	tests := []struct {
		text string
		d    time.Duration
	}{
		{"6mon1w3d5min2s", day*180 + day*7 + day*3 + 5*time.Minute + 2*time.Second},
		{"334us", 334 * time.Microsecond},
		{"5m4ms", 5*time.Minute + 4*time.Millisecond},
		{"1y6mon", 365*day + 180*day},
		{"245ns", 245 * time.Nanosecond},
		{"-6mon1w3d5min2s", -(day*180 + day*7 + day*3 + 5*time.Minute + 2*time.Second)},
		{"-334us", -(334 * time.Microsecond)},
		{"-5m4ms", -(5*time.Minute + 4*time.Millisecond)},
		{"-1y6mon", -(365*day + 180*day)},
		{"-245ns", -(245 * time.Nanosecond)},
	}

	for _, test := range tests {
		d, err := ParseExtendedDuration(test.text)
		require.NoError(t, err, "encountered parse error for %s", test.text)
		assert.Equal(t, test.d, d, "incorrect duration for %s", test.text)
	}
}

func TestParseExtendedDurationErrors(t *testing.T) {
	tests := []struct {
		text string
		err  string
	}{
		{"4", "could not parse duration: duration='4', err='no unit'"},
		{"", "duration empty"},
		{"4minutes", "could not parse duration: duration='4minutes', err_unknown_unit='minutes'"},
		{"4d3", "could not parse duration: duration='4d3', err='no unit'"},
		{"--4d", "could not parse duration: duration='-4d', err='no value'"},
		{",3490", "could not parse duration: duration=',3490', err='no value'"},
	}

	for _, test := range tests {
		_, err := ParseExtendedDuration(test.text)
		require.Error(t, err, "expected error for '%s'", test.text)
		assert.Equal(t, test.err, err.Error(), "invalid error for %s", test.text)
	}
}

func TestToExtendedString(t *testing.T) {
	tests := []struct {
		d      time.Duration
		result string
	}{
		{0, "0s"},
		{time.Nanosecond, "1ns"},
		{30 * time.Nanosecond, "30ns"},
		{60 * time.Microsecond, "60us"},
		{999 * time.Millisecond, "999ms"},
		{20 * time.Second, "20s"},
		{70 * time.Second, "1m10s"},
		{120 * time.Second, "2m"},
		{10 * time.Minute, "10m"},
		{20 * time.Hour, "20h"},
		{24 * time.Hour, "1d"},
		{25 * time.Hour, "1d1h"},
		{24 * 7 * time.Hour, "7d"},
		{24 * 8 * time.Hour, "8d"},
		{24 * 30 * time.Hour, "30d"},
		{24 * 31 * time.Hour, "31d"},
		{24 * 300 * time.Hour, "300d"},
		{-time.Nanosecond, "-1ns"},
		{-30 * time.Nanosecond, "-30ns"},
		{-60 * time.Microsecond, "-60us"},
		{-999 * time.Millisecond, "-999ms"},
		{-20 * time.Second, "-20s"},
		{-70 * time.Second, "-1m10s"},
		{-120 * time.Second, "-2m"},
		{-10 * time.Minute, "-10m"},
		{-20 * time.Hour, "-20h"},
		{-24 * time.Hour, "-1d"},
		{-25 * time.Hour, "-1d1h"},
		{-24 * 7 * time.Hour, "-7d"},
		{-24 * 8 * time.Hour, "-8d"},
		{-24 * 30 * time.Hour, "-30d"},
		{-24 * 31 * time.Hour, "-31d"},
		{-24 * 300 * time.Hour, "-300d"},
	}

	for _, test := range tests {
		require.Equal(t, test.result, ToExtendedString(test.d))
	}
}

func TestExtendDurationRoundTrip(t *testing.T) {
	inputs := []string{
		"0s",
		"1ns",
		"30ns",
		"60us",
		"999ms",
		"20s",
		"1m10s",
		"2m",
		"10m",
		"20h",
		"1d",
		"1d1h",
		"7d",
		"8d",
		"30d",
		"31d",
		"300d",
		"-1ns",
		"-30ns",
		"-60us",
		"-999ms",
		"-20s",
		"-1m10s",
		"-2m",
		"-10m",
		"-20h",
		"-1d",
		"-1d1h",
		"-7d",
		"-8d",
		"-30d",
		"-31d",
		"-300d",
	}

	for _, input := range inputs {
		d, err := ParseExtendedDuration(input)
		require.NoError(t, err)
		res := ToExtendedString(d)
		require.Equal(t, input, res)
	}
}
