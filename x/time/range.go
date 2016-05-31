package time

import "time"

// Range represents [start, end)
type Range struct {
	Start time.Time
	End   time.Time
}

// IsEmpty returns whether the time range is empty.
func (r Range) IsEmpty() bool {
	return r.Start == r.End
}

// Before determines whether r is before other.
func (r Range) Before(other Range) bool {
	return !r.End.After(other.Start)
}

// After determines whether r is after other.
func (r Range) After(other Range) bool {
	return other.Before(r)
}

// Contains determines whether r contains other.
func (r Range) Contains(other Range) bool {
	return !r.Start.After(other.Start) && !r.End.Before(other.End)
}

// Overlaps determines whether r overlaps with other.
func (r Range) Overlaps(other Range) bool {
	return r.End.After(other.Start) && r.Start.Before(other.End)
}

// Since returns the time range since a given point in time.
func (r Range) Since(t time.Time) Range {
	if t.Before(r.Start) {
		return r
	}
	if t.After(r.End) {
		return Range{}
	}
	return Range{Start: t, End: r.End}
}

// Merge merges the two ranges if they overlap. Otherwise,
// the gap between them is included.
func (r Range) Merge(other Range) Range {
	start := MinTime(r.Start, other.Start)
	end := MaxTime(r.End, other.End)
	return Range{Start: start, End: end}
}

// Subtract removes the intersection between r and other
// from r, possibly splitting r into two smaller ranges.
func (r Range) Subtract(other Range) []Range {
	if !r.Overlaps(other) {
		return []Range{r}
	}
	if other.Contains(r) {
		return nil
	}
	var res []Range
	left := Range{r.Start, other.Start}
	right := Range{other.End, r.End}
	if r.Contains(other) {
		if !left.IsEmpty() {
			res = append(res, left)
		}
		if !right.IsEmpty() {
			res = append(res, right)
		}
		return res
	}
	if !r.Start.After(other.Start) {
		if !left.IsEmpty() {
			res = append(res, left)
		}
		return res
	}
	if !right.IsEmpty() {
		res = append(res, right)
	}
	return res
}
