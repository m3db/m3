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
	"errors"
	"time"
)

// Different time units that are supported.
const (
	// None is a place holder for time units, it doesn't represent an actual time unit.
	None Unit = iota
	Second
	Millisecond
	Microsecond
	Nanosecond
	Minute
)

var (
	errUnrecognizedTimeUnit  = errors.New("unrecognized time unit")
	errConvertDurationToUnit = errors.New("unable to convert from duration to time unit")
)

// Unit represents a time unit.
type Unit byte

// Value is the time duration of the time unit.
func (tu Unit) Value() (time.Duration, error) {
	if d, found := unitsToDuration[tu]; found {
		return d, nil
	}
	return 0, errUnrecognizedTimeUnit
}

// IsValid returns whether the given time unit is valid / supported.
func (tu Unit) IsValid() bool {
	_, valid := unitsToDuration[tu]
	return valid
}

// String returns the string representation for the time unit
func (tu Unit) String() string {
	if s, found := unitStrings[tu]; found {
		return s
	}

	return "unknown"
}

// UnitFromDuration creates a time unit from a time duration.
func UnitFromDuration(d time.Duration) (Unit, error) {
	if unit, found := durationsToUnit[d]; found {
		return unit, nil
	}

	return None, errConvertDurationToUnit
}

var (
	unitStrings = map[Unit]string{
		Second:      "s",
		Millisecond: "ms",
		Nanosecond:  "ns",
		Microsecond: "us",
		Minute:      "m",
	}

	durationsToUnit = make(map[time.Duration]Unit)
	unitsToDuration = map[Unit]time.Duration{
		Second:      time.Second,
		Millisecond: time.Millisecond,
		Nanosecond:  time.Nanosecond,
		Microsecond: time.Microsecond,
		Minute:      time.Minute,
	}
)

func init() {
	for u, d := range unitsToDuration {
		durationsToUnit[d] = u
	}
}
