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
	"sort"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
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
	values             []Iterator
	earliest           []Iterator
	earliestAt         time.Time
	filterStart        time.Time
	filterEnd          time.Time
	filtering          bool
	equalTimesStrategy IterateEqualTimestampStrategy

	// Used for caching reuse of value frequency lookup
	valueFrequencies map[float64]int
}

func (i *iterators) len() int {
	return len(i.values)
}

func (i *iterators) current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	numIters := len(i.earliest)

	switch i.equalTimesStrategy {
	case IterateHighestValue:
		sort.Slice(i.earliest, func(a, b int) bool {
			currA, _, _ := i.earliest[a].Current()
			currB, _, _ := i.earliest[b].Current()
			return currA.Value < currB.Value
		})

	case IterateLowestValue:
		sort.Slice(i.earliest, func(a, b int) bool {
			currA, _, _ := i.earliest[a].Current()
			currB, _, _ := i.earliest[b].Current()
			return currA.Value > currB.Value
		})

	case IterateHighestFrequencyValue:
		// Calculate frequencies
		if i.valueFrequencies == nil {
			i.valueFrequencies = make(map[float64]int)
		}
		for _, iter := range i.earliest {
			curr, _, _ := iter.Current()
			i.valueFrequencies[curr.Value]++
		}

		// Sort
		sort.Slice(i.earliest, func(a, b int) bool {
			currA, _, _ := i.earliest[a].Current()
			currB, _, _ := i.earliest[b].Current()
			freqA := i.valueFrequencies[currA.Value]
			freqB := i.valueFrequencies[currB.Value]
			return freqA < freqB
		})

		// Reset reuseable value frequencies
		for key := range i.valueFrequencies {
			delete(i.valueFrequencies, key)
		}

	default:
		// IterateLastPushed or unknown strategy code path, don't panic on unknown
		// as this is an internal data structure and this option is validated at a
		// layer above.
	}

	return i.earliest[numIters-1].Current()
}

func (i *iterators) at() time.Time {
	return i.earliestAt
}

func (i *iterators) push(iter Iterator) bool {
	if i.filtering && !i.moveIteratorToFilterNext(iter) {
		return false
	}
	i.values = append(i.values, iter)
	i.tryAddEarliest(iter)
	return true
}

func (i *iterators) tryAddEarliest(iter Iterator) {
	dp, _, _ := iter.Current()
	if dp.Timestamp.Equal(i.earliestAt) {
		// Push equal earliest
		i.earliest = append(i.earliest, iter)
	} else if dp.Timestamp.Before(i.earliestAt) {
		// Reset earliest and push new iter
		i.earliest = append(i.earliest[:0], iter)
		i.earliestAt = dp.Timestamp
	}
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
	var (
		prevAt = i.earliestAt
		n      = len(i.values)
	)
	for _, iter := range i.earliest {
		next := iter.Next()
		if next && i.filtering {
			// Filter out values if applying filters
			next = i.moveIteratorToFilterNext(iter)
		}

		err := iter.Err()
		if err != nil {
			i.reset()
			return false, err
		}

		if next {
			continue
		}

		// No next so swap tail in and shrink by one
		iter.Close()
		idx := -1
		for i, curr := range i.values {
			if curr == iter {
				idx = i
				break
			}
		}
		i.values[idx] = i.values[n-1]
		i.values[n-1] = nil
		i.values = i.values[:n-1]
		n = n - 1
	}

	// Reset earliest
	for idx := range i.earliest {
		i.earliest[idx] = nil
	}
	i.earliest = i.earliest[:0]

	// No iterators left
	if n == 0 {
		i.reset()
		return false, nil
	}

	// Force first to be new earliest, evaluate rest
	i.earliestAt = timeMax
	for _, iter := range i.values {
		i.tryAddEarliest(iter)
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
	for idx := range i.earliest {
		i.earliest[idx] = nil
	}
	i.earliest = i.earliest[:0]
	i.earliestAt = timeMax
}

func (i *iterators) setFilter(start, end time.Time) {
	i.filtering = true
	i.filterStart = start
	i.filterEnd = end
}
