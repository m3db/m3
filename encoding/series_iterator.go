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

package encoding

import (
	"container/heap"
	"errors"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	xtime "github.com/m3db/m3db/x/time"
)

var (
	timeZero              time.Time
	errOutOfOrderIterator = errors.New("series values are out of order from mixed reader")
)

type seriesIterator struct {
	id        string
	start     time.Time
	end       time.Time
	iters     []m3db.Iterator
	currIters IteratorHeap
	err       error
	firstNext bool
	closed    bool
}

// NewSeriesIterator creates a new series iterator
func NewSeriesIterator(
	id string,
	startInclusive, endExclusive time.Time,
	replicas []m3db.Iterator,
) m3db.SeriesIterator {
	it := &seriesIterator{}
	it.Reset(id, startInclusive, endExclusive, replicas)
	return it
}

func (it *seriesIterator) ID() string {
	return it.id
}

func (it *seriesIterator) Start() time.Time {
	return it.start
}

func (it *seriesIterator) End() time.Time {
	return it.end
}

func (it *seriesIterator) Next() bool {
	if it.firstNext {
		it.firstNext = false
		return it.hasNext()
	}
	if !it.hasNext() {
		return false
	}
	it.moveToNext(true)
	return it.hasNext()
}

func (it *seriesIterator) Current() (m3db.Datapoint, xtime.Unit, m3db.Annotation) {
	return it.currIters[0].Current()
}

func (it *seriesIterator) Err() error {
	return it.err
}

func (it *seriesIterator) Close() {
	if it.isClosed() {
		return
	}
	it.closed = true
	// TODO(r): when able to be pooled check if has pool and return to pool
}

func (it *seriesIterator) Reset(id string, startInclusive, endExclusive time.Time, replicas []m3db.Iterator) {
	it.id = id
	it.start = startInclusive
	it.end = endExclusive
	it.iters = it.iters[:0]
	it.currIters = it.currIters[:0]
	it.err = nil
	it.firstNext = true
	it.closed = false
	for _, replica := range replicas {
		it.iters = append(it.iters, replica)
	}
	heap.Init(&it.currIters)
	it.moveToNext(false)
}

func (it *seriesIterator) hasError() bool {
	return it.err != nil
}

func (it *seriesIterator) isClosed() bool {
	return it.closed
}

func (it *seriesIterator) hasMore() bool {
	return len(it.iters) > 0
}

func (it *seriesIterator) hasNext() bool {
	return !it.hasError() && !it.isClosed() && it.hasMore()
}

func (it *seriesIterator) moveIterToValidNext(iter m3db.Iterator) bool {
	dp, _, _ := iter.Current()
	prevT := dp.Timestamp
	for iter.Next() {
		dp, _, _ := iter.Current()
		t := dp.Timestamp
		if t.Before(prevT) {
			// Out of order datapoint
			it.err = errOutOfOrderIterator
			iter.Close()
			return false
		}
		if t.Before(it.start) {
			// Fast forward
			continue
		}
		if !t.Before(it.end) {
			// Close this iter
			break
		}
		return true
	}

	err := iter.Err()
	iter.Close()
	if err != nil {
		it.err = err
	}
	return false
}

func (it *seriesIterator) moveToNext(pop bool) {
	if pop {
		heap.Pop(&it.currIters)
	}

	for i, iter := range it.iters {
		for _, existing := range it.currIters {
			if existing == iter {
				// Already have this iterator in the currIters
				continue
			}
		}

		if !it.moveIterToValidNext(iter) {
			// Closed so remove it, two step, first nil out then delete
			it.iters[i] = nil
			continue
		}

		// Dedupe
		dp, _, _ := iter.Current()
		t := dp.Timestamp
		dupe := false
		for i := range it.currIters {
			dp, _, _ := it.currIters[i].Current()
			existsT := dp.Timestamp
			if t.Equal(existsT) {
				dupe = true
				break
			}
		}
		if !dupe {
			heap.Push(&it.currIters, iter)
		}
	}

	// Delete all nil iters
	itersLen := len(it.iters)
	for i := 0; i < itersLen; i++ {
		if it.iters[i] == nil {
			// Swap tail to here and progress
			it.iters[i] = it.iters[itersLen-1]
			it.iters = it.iters[:itersLen-1]
			itersLen--

			// Reconsider this record
			i--
		}
	}
}
