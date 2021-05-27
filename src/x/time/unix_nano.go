// Copyright (c) 2017 Uber Technologies, Inc.
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

package time

import (
	"fmt"
	"runtime/debug"
	"time"
)

// UnixNano is used to indicate that an int64 stores a unix timestamp at
// nanosecond resolution.
type UnixNano int64

// ToTime returns a time.ToTime from a UnixNano.
func (u UnixNano) ToTime() time.Time {
	return time.Unix(0, int64(u))
}

// ToUnixNano returns a UnixNano from a time.Time.
func ToUnixNano(t time.Time) UnixNano {
	return UnixNano(t.UnixNano())
}

// Truncate returns the result of rounding u down to a multiple of d.
func (u UnixNano) Truncate(d time.Duration) UnixNano {
	return (u / UnixNano(d)) * UnixNano(d)
}

// Sub returns the duration u-o. If the result exceeds the maximum (or minimum)
// value that can be stored in a Duration, the maximum (or minimum) duration
// will be returned.
func (u UnixNano) Sub(o UnixNano) time.Duration {
	return time.Duration(u - o)
}

// Add returns the time u+d.
func (u UnixNano) Add(d time.Duration) UnixNano {
	return u + UnixNano(d)
}

// ToNormalizedTime returns the normalized units of time given a time unit.
func (u UnixNano) ToNormalizedTime(d time.Duration) int64 {
	if u < 16221403330 {
		fmt.Println("NORMALIZING", u, "BY", d.Nanoseconds(), "RES:", int64(u)/d.Nanoseconds())
		debug.PrintStack()
	}
	return int64(u) / d.Nanoseconds()
}

// FromNormalizedTime returns the time given the normalized time units and the time unit.
func (u UnixNano) FromNormalizedTime(d time.Duration) UnixNano {
	return u * UnixNano(d/time.Nanosecond)
}

// Before reports whether the time instant u is before t.
func (u UnixNano) Before(t UnixNano) bool {
	return u < t
}

// After reports whether the time instant u is after t.
func (u UnixNano) After(t UnixNano) bool {
	return u > t
}

// Equal reports whether the time instant u is equal to t.
func (u UnixNano) Equal(t UnixNano) bool {
	return u == t
}

// IsZero reports whether the time instant u is 0.
func (u UnixNano) IsZero() bool {
	return u == 0
}

// String returns the time formatted using the format string
//	"2006-01-02 15:04:05.999999999 -0700 MST"
func (u UnixNano) String() string {
	return u.ToTime().String()
}

// Format returns the string representation for the time with the given format.
func (u UnixNano) Format(blockTimeFormat string) string {
	return u.ToTime().Format(blockTimeFormat)
}

// Seconds returns the seconds for time u, as an int64.
func (u UnixNano) Seconds() int64 {
	return u.ToNormalizedTime(time.Second)
}
