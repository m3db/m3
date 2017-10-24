package time

import "time"

// UnixNano is used to indicate that an int64 stores a unix timestamp at
// nanosecond resolution
type UnixNano int64

// ToTime returns a time.ToTime from a UnixNano
func (u UnixNano) ToTime() time.Time {
	return time.Unix(0, int64(u))
}

// ToUnixNano returns a UnixNano from a time.Time
func ToUnixNano(t time.Time) UnixNano {
	return UnixNano(t.UnixNano())
}
