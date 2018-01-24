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

package time

import (
	"errors"
	"sort"
	"time"
)

// Different time units that are supported.
const (
	// None is a place holder for time units, it doesn't represent an actual time unit. The ordering
	// here is used for comparisons betweens units and should not be changed.
	None Unit = iota
	Second
	Millisecond
	Microsecond
	Nanosecond
	Minute
	Hour
	Day
	Year
)

var (
	errUnrecognizedTimeUnit  = errors.New("unrecognized time unit")
	errConvertDurationToUnit = errors.New("unable to convert from duration to time unit")
	errConvertUnitToDuration = errors.New("unable to convert from time unit to duration")
	errNegativeDuraton       = errors.New("duration cannot be negative")
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

// Count returns the number of units contained within the duration.
func (tu Unit) Count(d time.Duration) (int, error) {
	if d < 0 {
		return 0, errNegativeDuraton
	}

	if dur, found := unitsToDuration[tu]; found {
		return int(d / dur), nil
	}

	// Invalid unit.
	return 0, errUnrecognizedTimeUnit
}

// MustCount is like Count but panics if d is negative or if tu is not
// a valid Unit.
func (tu Unit) MustCount(d time.Duration) int {
	c, err := tu.Count(d)
	if err != nil {
		panic(err)
	}

	return c
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

// DurationFromUnit creates a time duration from a time unit.
func DurationFromUnit(u Unit) (time.Duration, error) {
	if duration, found := unitsToDuration[u]; found {
		return duration, nil
	}

	return 0, errConvertUnitToDuration
}

// MaxUnitForDuration determines the maximum unit for which
// the input duration is a multiple of.
func MaxUnitForDuration(d time.Duration) (int64, Unit) {
	var (
		currMultiple int64
		currUnit     = Nanosecond
		dUnixNanos   = d.Nanoseconds()
		isNegative   bool
	)
	if dUnixNanos < 0 {
		dUnixNanos = -dUnixNanos
		isNegative = true
	}
	for _, u := range unitsByDurationDesc {
		// The unit is guaranteed to be valid so it's safe to ignore error here.
		duration, _ := u.Value()
		if dUnixNanos < duration.Nanoseconds() {
			continue
		}
		durationUnixNanos := int64(duration)
		quotient := dUnixNanos / durationUnixNanos
		remainder := dUnixNanos - quotient*durationUnixNanos
		if remainder != 0 {
			continue
		}
		currMultiple = quotient
		currUnit = u
		break
	}
	if isNegative {
		currMultiple = -currMultiple
	}
	return currMultiple, currUnit
}

var (
	unitStrings = map[Unit]string{
		Second:      "s",
		Millisecond: "ms",
		Nanosecond:  "ns",
		Microsecond: "us",
		Minute:      "m",
		Hour:        "h",
		Day:         "d",
		Year:        "y",
	}

	durationsToUnit = make(map[time.Duration]Unit)
	unitsToDuration = map[Unit]time.Duration{
		Second:      time.Second,
		Millisecond: time.Millisecond,
		Nanosecond:  time.Nanosecond,
		Microsecond: time.Microsecond,
		Minute:      time.Minute,
		Hour:        time.Hour,
		Day:         time.Hour * 24,
		Year:        time.Hour * 24 * 365,
	}
	unitsByDurationDesc []Unit
)

// byDurationDesc sorts time units by their durations in descending order.
// The order is undefined if the units are invalid.
type byDurationDesc []Unit

func (b byDurationDesc) Len() int      { return len(b) }
func (b byDurationDesc) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

func (b byDurationDesc) Less(i, j int) bool {
	vi, _ := b[i].Value()
	vj, _ := b[j].Value()
	return vi > vj
}

func init() {
	unitsByDurationDesc = make([]Unit, 0, len(unitsToDuration))
	for u, d := range unitsToDuration {
		durationsToUnit[d] = u
		unitsByDurationDesc = append(unitsByDurationDesc, u)
	}
	sort.Sort(byDurationDesc(unitsByDurationDesc))
}
