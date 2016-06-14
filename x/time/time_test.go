package time

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestToNormalizedTime(t *testing.T) {
	inputs := []struct {
		t        time.Time
		u        time.Duration
		expected int64
	}{
		{time.Unix(1, 0), time.Second, 1},
		{time.Unix(0, 150000000), time.Millisecond, 150},
		{time.Unix(0, 150000000), time.Microsecond, 150000},
		{time.Unix(0, 150000000), time.Nanosecond, 150000000},
		{time.Unix(0, 150000000), time.Second, 0},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, ToNormalizedTime(input.t, input.u))
	}
}

func TestFromNormalizedTime(t *testing.T) {
	inputs := []struct {
		nt       int64
		u        time.Duration
		expected time.Time
	}{
		{1, time.Second, time.Unix(1, 0)},
		{150, time.Millisecond, time.Unix(0, 150000000)},
		{150000, time.Microsecond, time.Unix(0, 150000000)},
		{150000000, time.Nanosecond, time.Unix(0, 150000000)},
		{60100, time.Millisecond, time.Unix(60, 100000000)},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, FromNormalizedTime(input.nt, input.u))
	}
}

func TestToNormalizedDuration(t *testing.T) {
	inputs := []struct {
		d        time.Duration
		u        time.Duration
		expected int64
	}{
		{60 * time.Second, time.Second, 60},
		{60 * time.Second, time.Millisecond, 60000},
		{60100 * time.Millisecond, time.Second, 60},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, ToNormalizedDuration(input.d, input.u))
	}
}

func TestFromNormalizedDuration(t *testing.T) {
	inputs := []struct {
		nd       int64
		u        time.Duration
		expected time.Duration
	}{
		{60, time.Second, time.Minute},
		{60100, time.Millisecond, 60100 * time.Millisecond},
		{1000000000, time.Nanosecond, time.Second},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, FromNormalizedDuration(input.nd, input.u))
	}
}

func TestToNanoseconds(t *testing.T) {
	input := time.Unix(1, 100000000)
	require.Equal(t, int64(1100000000), ToNanoseconds(input))
}

func TestFromNanoseconds(t *testing.T) {
	expected := time.Unix(1, 100000000)
	require.Equal(t, expected, FromNanoseconds(1100000000))
}

func TestCeil(t *testing.T) {
	var timeZero time.Time
	inputs := []struct {
		t        time.Time
		d        time.Duration
		expected time.Time
	}{
		{timeZero.Add(2 * time.Hour), time.Hour, timeZero.Add(2 * time.Hour)},
		{timeZero.Add(2 * time.Hour), 2 * time.Hour, timeZero.Add(2 * time.Hour)},
		{timeZero.Add(15 * time.Minute), 2 * time.Hour, timeZero.Add(2 * time.Hour)},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, Ceil(input.t, input.d))
	}
}

func TestMinTime(t *testing.T) {
	inputs := []struct {
		t1       time.Time
		t2       time.Time
		expected time.Time
	}{
		{time.Unix(10, 0), time.Unix(20, 0), time.Unix(10, 0)},
		{time.Unix(20, 0), time.Unix(10, 0), time.Unix(10, 0)},
		{time.Unix(10, 0), time.Unix(10, 0), time.Unix(10, 0)},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, MinTime(input.t1, input.t2))
	}
}

func TestMaxTime(t *testing.T) {
	inputs := []struct {
		t1       time.Time
		t2       time.Time
		expected time.Time
	}{
		{time.Unix(10, 0), time.Unix(20, 0), time.Unix(20, 0)},
		{time.Unix(20, 0), time.Unix(10, 0), time.Unix(20, 0)},
		{time.Unix(10, 0), time.Unix(10, 0), time.Unix(10, 0)},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, MaxTime(input.t1, input.t2))
	}
}
