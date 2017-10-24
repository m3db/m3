package time

import "time"

// UnixNano is used to indicate that an int64 stores a unix timestamp at
// nanosecond resolution
type UnixNano int64

// Time returns a time.Time from a UnixNano
func (u UnixNano) Time() time.Time {
	return time.Unix(0, int64(u))
}

// NewUnixNano returns a UnixNano from a time.Time
func NewUnixNano(t time.Time) UnixNano {
	return UnixNano(t.UnixNano())
}
