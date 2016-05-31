package time

import (
	"container/list"
)

// Ranges is a collection of time ranges.
type Ranges interface {

	// Len returns the number of ranges included.
	Len() int

	// IsEmpty returns true if the list of time ranges is empty.
	IsEmpty() bool

	// Contains checks if the range is fully contained.
	Contains(r Range) bool

	// AddRange adds the time range
	AddRange(r Range) Ranges

	// AddRanges adds the time ranges
	AddRanges(other Ranges) Ranges

	// Remove removes the time range
	RemoveRange(r Range) Ranges

	// RemoveRanges removes the given time ranges.
	RemoveRanges(other Ranges) Ranges

	// Iter returns an iterator that iterates forward over the time ranges included.
	Iter() RangeIter
}

type ranges struct {
	sortedRanges *list.List
}

// NewRanges creates a collection of time ranges.
func NewRanges() Ranges {
	return &ranges{sortedRanges: list.New()}
}

// Len returns the number of ranges included.
func (tr *ranges) Len() int {
	return tr.sortedRanges.Len()
}

// IsEmpty returns true if the list of time ranges is empty.
func (tr *ranges) IsEmpty() bool {
	return tr == nil || tr.Len() == 0
}

// Contains checks if the range is fully contained.
func (tr *ranges) Contains(r Range) bool {
	if r.IsEmpty() {
		return true
	}
	_, e := tr.findFirstNotBefore(r)
	if e == nil {
		return false
	}
	lr := e.Value.(Range)
	return lr.Contains(r)
}

// AddRange adds the time range to the collection of ranges.
func (tr *ranges) AddRange(r Range) Ranges {
	res := tr.clone()
	res.addRangeInPlace(r)
	return res
}

// addRangeInPlace adds r to tr in place without creating a new copy.
func (tr *ranges) addRangeInPlace(r Range) {
	if r.IsEmpty() {
		return
	}

	// if the previous range touches r, merge them
	pe, e := tr.findFirstNotBefore(r)
	if pe != nil {
		pr := pe.Value.(Range)
		if pr.End == r.Start {
			r.Start = pr.Start
			tr.sortedRanges.Remove(pe)
		}
	}

	for e != nil {
		lr := e.Value.(Range)
		ne := e.Next()
		if !lr.Overlaps(r) {
			// if r touches the next range, merge them
			if r.End == lr.Start {
				r.End = lr.End
				tr.sortedRanges.Remove(e)
				e = ne
			}
			break
		}
		r = r.Merge(lr)
		tr.sortedRanges.Remove(e)
		e = ne
	}
	if e == nil {
		tr.sortedRanges.PushBack(r)
		return
	}
	tr.sortedRanges.InsertBefore(r, e)
}

// AddRanges adds the time ranges.
func (tr *ranges) AddRanges(other Ranges) Ranges {
	res := tr.clone()
	if other == nil {
		return res
	}
	it := other.Iter()
	for it.Next() {
		res.addRangeInPlace(it.Value())
	}
	return res
}

// Remove removes the time range from the collection of ranges.
func (tr *ranges) RemoveRange(r Range) Ranges {
	cloned := tr.clone()
	cloned.removeRangeInPlace(r)
	return cloned
}

func (tr *ranges) removeRangeInPlace(r Range) {
	if r.IsEmpty() {
		return
	}
	_, e := tr.findFirstNotBefore(r)
	for e != nil {
		lr := e.Value.(Range)
		ne := e.Next()
		if !lr.Overlaps(r) {
			return
		}
		res := lr.Subtract(r)
		if res == nil {
			tr.sortedRanges.Remove(e)
		} else {
			e.Value = res[0]
			if len(res) == 2 {
				tr.sortedRanges.InsertAfter(res[1], e)
			}
		}
		e = ne
	}
}

// RemoveRanges removes the given time ranges from the current one.
func (tr *ranges) RemoveRanges(other Ranges) Ranges {
	cloned := tr.clone()
	if other == nil {
		return cloned
	}
	it := other.Iter()
	for it.Next() {
		cloned.removeRangeInPlace(it.Value())
	}
	return cloned
}

// Iter returns an iterator that iterates over the time ranges included.
func (tr *ranges) Iter() RangeIter {
	return newRangeIter(tr.sortedRanges)
}

// findFirstNotBefore finds the first interval that's not before r.
func (tr *ranges) findFirstNotBefore(r Range) (*list.Element, *list.Element) {
	var pe *list.Element
	for e := tr.sortedRanges.Front(); e != nil; e = e.Next() {
		if !e.Value.(Range).Before(r) {
			return pe, e
		}
		pe = e
	}
	return pe, nil
}

// clone returns a copy of the time ranges.
func (tr *ranges) clone() *ranges {
	if tr == nil {
		return nil
	}
	res := NewRanges().(*ranges)
	for e := tr.sortedRanges.Front(); e != nil; e = e.Next() {
		res.sortedRanges.PushBack(e.Value.(Range))
	}
	return res
}

// IsEmpty returns whether the time range collection is empty.
func IsEmpty(tr Ranges) bool {
	return tr == nil || tr.IsEmpty()
}
