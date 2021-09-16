// Package ptr contains functions to create references from constant values,
// typically for representing optional values.
package ptr

import (
	"time"
)

// String returns a string pointer.
func String(s string) *string { return &s }

// StringOrDefault returns the value of p, or if it is nil then the default value v.
func StringOrDefault(p *string, v string) string {
	if p == nil {
		return v
	}

	return *p
}

// ToString is a convenience method for dereferencing a string pointer to a
// string. "" is returned if the pointer is nil.
func ToString(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

// StringEqual compares two string pointers for equality.
func StringEqual(p1, p2 *string) bool {
	return p1 == p2 || (p1 != nil && p2 != nil && *p1 == *p2)
}

// Int returns an int pointer.
func Int(n int) *int { return &n }

// IntOrDefault returns the value of p, or if it is nil then the default value v.
func IntOrDefault(p *int, v int) int {
	if p == nil {
		return v
	}

	return *p
}

// IntEqual compares two int pointers for equality.
func IntEqual(p1, p2 *int) bool {
	return p1 == p2 || (p1 != nil && p2 != nil && *p1 == *p2)
}

// UInt returns a uint pointer.
func UInt(n uint) *uint { return &n }

// ToUInt dereferences a uint pointer to a uint. If the pointer is nil, then returns 0.
func ToUInt(n *uint) uint {
	if n == nil {
		return 0
	}
	return *n
}

// Int32 returns an int32 pointer
func Int32(n int32) *int32 { return &n }

// Int32OrDefault returns the value of p, or if it is nil then the default value v.
func Int32OrDefault(p *int32, v int32) int32 {
	if p == nil {
		return v
	}

	return *p
}

// Int32Equal compares two int32 pointers for equality.
func Int32Equal(p1, p2 *int32) bool {
	return p1 == p2 || (p1 != nil && p2 != nil && *p1 == *p2)
}

// Int64 returns an int64 pointer
func Int64(n int64) *int64 { return &n }

// Int64OrDefault returns the value of p, or if it is nil then the default value v.
func Int64OrDefault(p *int64, v int64) int64 {
	if p == nil {
		return v
	}

	return *p
}

// Int64Equal compares two int64 pointers for equality.
func Int64Equal(p1, p2 *int64) bool {
	return p1 == p2 || (p1 != nil && p2 != nil && *p1 == *p2)
}

// Bool returns a bool pointer.
func Bool(b bool) *bool { return &b }

// BoolOrDefault returns the value of p, or if it is nil then the default value v.
func BoolOrDefault(p *bool, v bool) bool {
	if p == nil {
		return v
	}

	return *p
}

// BoolEqual compares two bool pointers for equality.
func BoolEqual(p1, p2 *bool) bool {
	return p1 == p2 || (p1 != nil && p2 != nil && *p1 == *p2)
}

// Float64 returns a float64 pointer.
func Float64(n float64) *float64 { return &n }

// Float64OrDefault returns the value of p, or if it is nil then the default value v.
func Float64OrDefault(p *float64, v float64) float64 {
	if p == nil {
		return v
	}

	return *p
}

// Float64Equal compares two float64 pointers for equality.
func Float64Equal(p1, p2 *float64) bool {
	return p1 == p2 || (p1 != nil && p2 != nil && *p1 == *p2)
}

// Time returns a time pointer.
func Time(n time.Time) *time.Time { return &n }

// TimeOrDefault returns the value of p, or if it is nil then the default value v.
func TimeOrDefault(p *time.Time, v time.Time) time.Time {
	if p == nil {
		return v
	}

	return *p
}

// TimeEqual compares two Time pointers for equality
func TimeEqual(p1, p2 *time.Time) bool {
	return p1 == p2 || (p1 != nil && p2 != nil && *p1 == *p2)
}

// Duration returns a duration pointer.
func Duration(n time.Duration) *time.Duration { return &n }

// DurationOrDefault returns the value of p, or if it is nil then the default value v.
func DurationOrDefault(p *time.Duration, v time.Duration) time.Duration {
	if p == nil {
		return v
	}

	return *p
}

// DurationEqual compares two Duration pointers for equality
func DurationEqual(p1, p2 *time.Duration) bool {
	return p1 == p2 || (p1 != nil && p2 != nil && *p1 == *p2)
}
