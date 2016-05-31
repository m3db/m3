package time

import "container/list"

// RangeIter iterates over a collection of time ranges.
type RangeIter interface {
	// Next moves to the next item.
	Next() bool

	// Value returns the current time range.
	Value() Range
}

type rangeIter struct {
	ranges *list.List
	cur    *list.Element
}

func newRangeIter(ranges *list.List) RangeIter {
	return &rangeIter{ranges: ranges}
}

// Next moves to the next item.
func (it *rangeIter) Next() bool {
	if it.ranges == nil {
		return false
	}
	if it.cur == nil {
		it.cur = it.ranges.Front()
	} else {
		it.cur = it.cur.Next()
	}
	return it.cur != nil
}

// Value returns the current time range.
func (it *rangeIter) Value() Range {
	return it.cur.Value.(Range)
}
