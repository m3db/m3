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

package xtime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitValue(t *testing.T) {
	inputs := []struct {
		u        Unit
		expected time.Duration
	}{
		{Second, time.Second},
		{Millisecond, time.Millisecond},
		{Microsecond, time.Microsecond},
		{Nanosecond, time.Nanosecond},
		{Minute, time.Minute},
		{Hour, time.Hour},
	}
	for _, input := range inputs {
		v, err := input.u.Value()
		require.NoError(t, err)
		require.Equal(t, input.expected, v)
	}

	invalidUnit := Unit(10)
	_, err := invalidUnit.Value()
	require.Equal(t, errUnrecognizedTimeUnit, err)
}

func TestUnitCount(t *testing.T) {
	inputs := []struct {
		u        Unit
		d        time.Duration
		expected int
	}{
		{Second, time.Second, 1},
		{Millisecond, 10 * time.Millisecond, 10},
		{Microsecond, time.Nanosecond, 0},
		{Nanosecond, time.Microsecond, 1000},
	}
	for _, input := range inputs {
		c, err := input.u.Count(input.d)
		require.NoError(t, err)
		require.Equal(t, input.expected, c)
	}

	invalidUnit := Unit(10)
	_, err := invalidUnit.Count(time.Second)
	require.Error(t, err)

	var (
		u               = Second
		invalidDuration = -1 * time.Second
	)
	_, err = u.Count(invalidDuration)
	require.Error(t, err)
}

func TestUnitMustCount(t *testing.T) {
	inputs := []struct {
		u        Unit
		d        time.Duration
		expected int
	}{
		{Second, time.Second, 1},
		{Millisecond, 10 * time.Millisecond, 10},
		{Microsecond, time.Nanosecond, 0},
		{Nanosecond, time.Microsecond, 1000},
	}
	for _, input := range inputs {
		c := input.u.MustCount(input.d)
		require.Equal(t, input.expected, c)
	}

	invalidUnit := Unit(10)
	require.Panics(t, func() {
		invalidUnit.MustCount(time.Second)
	})

	var (
		u               = Second
		invalidDuration = -1 * time.Second
	)
	require.Panics(t, func() {
		u.MustCount(invalidDuration)
	})
}

func TestUnitIsValid(t *testing.T) {
	inputs := []struct {
		u        Unit
		expected bool
	}{
		{Second, true},
		{Millisecond, true},
		{Microsecond, true},
		{Nanosecond, true},
		{Minute, true},
		{Hour, true},
		{Unit(10), false},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, input.u.IsValid())
	}
}

func TestUnitFromDuration(t *testing.T) {
	inputs := []struct {
		d        time.Duration
		expected Unit
	}{
		{time.Second, Second},
		{time.Millisecond, Millisecond},
		{time.Microsecond, Microsecond},
		{time.Nanosecond, Nanosecond},
		{time.Minute, Minute},
		{time.Hour, Hour},
	}
	for _, input := range inputs {
		u, err := UnitFromDuration(input.d)
		require.NoError(t, err)
		require.Equal(t, input.expected, u)
	}
}

func TestUnitFromDurationError(t *testing.T) {
	_, err := UnitFromDuration(time.Hour * 30)
	require.Equal(t, errConvertDurationToUnit, err)
}

func TestMaxUnitForDuration(t *testing.T) {
	inputs := []struct {
		d                time.Duration
		expectedMultiple int64
		expectedUnit     Unit
	}{
		{20 * time.Second, 20, Second},
		{70 * time.Second, 70, Second},
		{120 * time.Second, 2, Minute},
		{30 * time.Nanosecond, 30, Nanosecond},
		{999 * time.Millisecond, 999, Millisecond},
		{60 * time.Microsecond, 60, Microsecond},
		{10 * time.Minute, 10, Minute},
		{20 * time.Hour, 20, Hour},
	}
	for _, input := range inputs {
		m, u, err := MaxUnitForDuration(input.d)
		require.NoError(t, err)
		require.Equal(t, input.expectedMultiple, m)
		require.Equal(t, input.expectedUnit, u)
	}
}

func TestMaxUnitForDurationError(t *testing.T) {
	_, _, err := MaxUnitForDuration(0)
	require.Equal(t, errMaxUnitForDuration, err)
}

func TestDurationFromUnit(t *testing.T) {
	inputs := []struct {
		u        Unit
		expected time.Duration
	}{
		{Second, time.Second},
		{Millisecond, time.Millisecond},
		{Microsecond, time.Microsecond},
		{Nanosecond, time.Nanosecond},
		{Minute, time.Minute},
		{Hour, time.Hour},
	}
	for _, input := range inputs {
		d, err := DurationFromUnit(input.u)
		require.NoError(t, err)
		require.Equal(t, input.expected, d)
	}
}

func TestDurationFromUnitError(t *testing.T) {
	_, err := DurationFromUnit(None)
	require.Equal(t, errConvertUnitToDuration, err)
}

func TestUnitString(t *testing.T) {
	tests := []struct {
		u Unit
		s string
	}{
		{Second, "s"},
		{Millisecond, "ms"},
		{Microsecond, "us"},
		{Nanosecond, "ns"},
		{Minute, "m"},
		{Hour, "h"},
		{None, "unknown"},
	}

	for _, test := range tests {
		assert.Equal(t, test.s, test.u.String(), "invalid String() for %v", test.u)
	}
}
