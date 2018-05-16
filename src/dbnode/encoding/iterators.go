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

package encoding

import (
	"time"

	"github.com/m3db/m3db/src/dbnode/ts"
	xtime "github.com/m3db/m3x/time"
)

var (
	// time is stored as an int64 plus an int32 nanosecond value, but if you
	// use max int64 for the seconds component only then integer overflow
	// will occur when performing comparisons like time.Before() and they
	// will not work correctly.
	timeMax = time.Unix(1<<63-62135596801, 999999999)
)

// iterators is a collection of iterators, and allows for reading in order values
// from the underlying iterators that are separately in order themselves.
type iterators struct {
	values      []Iterator
	earliest    Iterator
	earliestIdx int
	earliestAt  time.Time
	filtering   bool
	filterStart time.Time
	filterEnd   time.Time
}

func (i *iterators) len() int {
	return len(i.values)
}

func (i *iterators) current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	return i.earliest.Current()
}

func (i *iterators) at() time.Time {
	return i.earliestAt
}

func (i *iterators) push(iter Iterator) bool {
	if i.filtering && !i.moveIteratorToFilterNext(iter) {
		return false
	}
	i.values = append(i.values, iter)
	dp, _, _ := iter.Current()
	if dp.Timestamp.Before(i.earliestAt) {
		i.earliest = iter
		i.earliestIdx = len(i.values) - 1
		i.earliestAt = dp.Timestamp
	}
	return true
}

func (i *iterators) moveIteratorToFilterNext(iter Iterator) bool {
	next := true
	for next {
		dp, _, _ := iter.Current()
		if dp.Timestamp.Before(i.filterStart) {
			// Filter out any before start
			next = iter.Next()
			continue
		}
		if !dp.Timestamp.Before(i.filterEnd) {
			// Filter out completely if after end
			next = false
			break
		}
		// Within filter
		break
	}
	return next
}

func (i *iterators) moveToValidNext() (bool, error) {
	n := len(i.values)
	if n == 0 {
		return false, nil
	}

	var (
		prevAt = i.earliestAt
		next   = i.earliest.Next()
	)
	if i.filtering && next {
		// Filter out values if applying filters
		next = i.moveIteratorToFilterNext(i.earliest)
	}

	err := i.earliest.Err()
	if err != nil {
		i.reset()
		return false, err
	}

	if n == 1 {
		if next {
			dp, _, _ := i.earliest.Current()
			i.earliestAt = dp.Timestamp
		} else {
			i.reset()
		}
		return i.validateNext(next, prevAt)
	}

	if !next {
		// No next so swap tail in and shrink by one
		i.earliest.Close()
		i.values[i.earliestIdx] = i.values[n-1]
		i.values[n-1] = nil
		i.values = i.values[:n-1]
		n = n - 1
	}

	// Evaluate new earliest
	i.earliest = i.values[0]
	i.earliestIdx = 0
	dp, _, _ := i.earliest.Current()
	i.earliestAt = dp.Timestamp
	for idx := 1; idx < n; idx++ {
		dp, _, _ = i.values[idx].Current()
		if dp.Timestamp.Before(i.earliestAt) {
			i.earliest = i.values[idx]
			i.earliestIdx = idx
			i.earliestAt = dp.Timestamp
		}
	}

	// Apply filter to new earliest if necessary
	if i.filtering {
		inFilter := i.earliestAt.Before(i.filterEnd) &&
			!i.earliestAt.Before(i.filterStart)
		if !inFilter {
			return i.moveToValidNext()
		}
	}

	return i.validateNext(true, prevAt)
}

func (i *iterators) validateNext(next bool, prevAt time.Time) (bool, error) {
	if i.earliestAt.Before(prevAt) {
		// Out of order datapoint
		i.reset()
		return false, errOutOfOrderIterator
	}
	return next, nil
}

func (i *iterators) reset() {
	for idx := range i.values {
		i.values[idx].Close()
		i.values[idx] = nil
	}
	i.values = i.values[:0]
	i.earliest = nil
	i.earliestIdx = 0
	i.earliestAt = timeMax
}

func (i *iterators) setFilter(start, end time.Time) {
	i.filtering = true
	i.filterStart = start
	i.filterEnd = end
}
