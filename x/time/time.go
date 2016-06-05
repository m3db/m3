package time

import "time"

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

// FromNormalizedDuration returns the duration given the normalized time duration and a time unit.
func FromNormalizedDuration(nd int64, u time.Duration) time.Duration {
	return time.Duration(nd) * u
}

// ToNanoseconds converts a time to nanoseconds.
func ToNanoseconds(t time.Time) int64 {
	return t.UnixNano()
}

// FromNanoseconds converts nanoseconds to a time.
func FromNanoseconds(nsecs int64) time.Time {
	return time.Unix(0, nsecs)
}

// Ceil returns the result of rounding t up to a multiple of d since
// the zero time.
func Ceil(t time.Time, d time.Duration) time.Time {
	res := t.Truncate(d)
	if res.Before(t) {
		res = res.Add(d)
	}
	return res
}

// MinTime returns the earlier one of t1 and t2.
func MinTime(t1, t2 time.Time) time.Time {
	if t1.Before(t2) {
		return t1
	}
	return t2
}

// MaxTime returns the later one of t1 and t2.
func MaxTime(t1, t2 time.Time) time.Time {
	if t1.After(t2) {
		return t1
	}
	return t2
}
