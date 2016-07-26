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
	"bytes"
	"container/list"
)

// Ranges is a collection of time ranges.
type Ranges interface {

	// Len returns the number of ranges included.
	Len() int

	// IsEmpty returns true if the list of time ranges is empty.
	IsEmpty() bool

	// Overlaps checks if the range overlaps with any of the ranges in the collection.
	Overlaps(r Range) bool

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

	// String returns the string representation of the range.
	String() string
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

// Overlaps checks if the range overlaps with any of the ranges in the collection.
func (tr *ranges) Overlaps(r Range) bool {
	if r.IsEmpty() {
		return false
	}
	e := tr.findFirstNotBefore(r)
	if e == nil {
		return false
	}
	lr := e.Value.(Range)
	return lr.Overlaps(r)
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

	e := tr.findFirstNotBefore(r)
	for e != nil {
		lr := e.Value.(Range)
		ne := e.Next()
		if !lr.Overlaps(r) {
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
	e := tr.findFirstNotBefore(r)
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

// String returns the string representation of the range.
func (tr *ranges) String() string {
	var buf bytes.Buffer
	buf.WriteString("[")
	for e := tr.sortedRanges.Front(); e != nil; e = e.Next() {
		buf.WriteString(e.Value.(Range).String())
		if e.Next() != nil {
			buf.WriteString(",")
		}
	}
	buf.WriteString("]")
	return buf.String()
}

// findFirstNotBefore finds the first interval that's not before r.
func (tr *ranges) findFirstNotBefore(r Range) *list.Element {
	for e := tr.sortedRanges.Front(); e != nil; e = e.Next() {
		if !e.Value.(Range).Before(r) {
			return e
		}
	}
	return nil
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
