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
		{None, "unknown"},
	}

	for _, test := range tests {
		assert.Equal(t, test.s, test.u.String(), "invalid String() for %v", test.u)
	}
}
