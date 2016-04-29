package memtsdb

import (
	"time"
)

// ToNormalizedTime returns the normalized units of time given a time unit.
func ToNormalizedTime(t time.Time, u time.Duration) int64 {
	return t.UnixNano() / u.Nanoseconds()
}

// FromNormalizedTime returns the time given the normalized time units and the time unit.
func FromNormalizedTime(nt int64, u time.Duration) time.Time {
	return time.Unix(0, int64(u/time.Nanosecond)*nt)
}

// ToNormalizedDuration returns the normalized units of duration given a time unit.
func ToNormalizedDuration(d time.Duration, u time.Duration) int64 {
	return int64(d / u)
}
