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
	expected := []Retention{
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
	for i, value := range inputs {
		retention, err := value.Retention()
		require.NoError(t, err)
		require.Equal(t, expected[i], retention)
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
