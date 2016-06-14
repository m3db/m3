package time

import (
	"testing"
	"time"

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
	}
	for _, input := range inputs {
		u, err := UnitFromDuration(input.d)
		require.NoError(t, err)
		require.Equal(t, input.expected, u)
	}
}
