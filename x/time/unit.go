package time

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
)

var (
	errUnrecognizedTimeUnit  = errors.New("unrecognized time unit")
	errConvertDurationToUnit = errors.New("unable to convert from duration to time unit")
)

// Unit represents a time unit.
type Unit byte

// Value is the time duration of the time unit.
func (tu Unit) Value() (time.Duration, error) {
	switch tu {
	case Second:
		return time.Second, nil
	case Millisecond:
		return time.Millisecond, nil
	case Microsecond:
		return time.Microsecond, nil
	case Nanosecond:
		return time.Nanosecond, nil
	}
	return 0, errUnrecognizedTimeUnit
}

// IsValid returns whether the given time unit is valid / supported.
func (tu Unit) IsValid() bool {
	switch tu {
	case Second, Millisecond, Microsecond, Nanosecond:
		return true
	default:
		return false
	}
}

// UnitFromDuration creates a time unit from a time duration.
func UnitFromDuration(d time.Duration) (Unit, error) {
	switch d {
	case time.Second:
		return Second, nil
	case time.Millisecond:
		return Millisecond, nil
	case time.Microsecond:
		return Microsecond, nil
	case time.Nanosecond:
		return Nanosecond, nil
	}
	return None, errConvertDurationToUnit
}
