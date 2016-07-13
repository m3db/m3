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
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	xtime "github.com/m3db/m3db/x/time"
)

type seriesIterator struct {
	id        string
	start     time.Time
	end       time.Time
	iters     IteratorHeap
	err       error
	firstNext bool
	closed    bool
	pool      m3db.SeriesIteratorPool
}

// NewSeriesIterator creates a new series iterator
func NewSeriesIterator(
	id string,
	startInclusive, endExclusive time.Time,
	replicas []m3db.Iterator,
	pool m3db.SeriesIteratorPool,
) m3db.SeriesIterator {
	it := &seriesIterator{pool: pool}
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
	if !it.firstNext {
		if !it.hasNext() {
			return false
		}
		it.moveToNext()
	}
	it.firstNext = false
	return it.hasNext()
}

func (it *seriesIterator) Current() (m3db.Datapoint, xtime.Unit, m3db.Annotation) {
	return it.iters[0].Current()
}

func (it *seriesIterator) Err() error {
	return it.err
}

func (it *seriesIterator) Close() {
	if it.isClosed() {
		return
	}
	it.closed = true
	for _, iter := range it.iters {
		iter.Close()
	}
	it.iters = it.iters[:0]
	if it.pool != nil {
		it.pool.Put(it)
	}
}

func (it *seriesIterator) Reset(id string, startInclusive, endExclusive time.Time, replicas []m3db.Iterator) {
	it.id = id
	it.start = startInclusive
	it.end = endExclusive
	it.iters = it.iters[:0]
	it.err = nil
	it.firstNext = true
	it.closed = false
	heap.Init(&it.iters)
	for _, replica := range replicas {
		// Replica can be nil if replica failed to respond
		if replica == nil {
			continue
		}
		if !it.moveIteratorToValidNext(replica, true) {
			// No values within range
			continue
		}
		heap.Push(&it.iters, replica)
	}
}

func (it *seriesIterator) hasError() bool {
	return it.err != nil
}

func (it *seriesIterator) isClosed() bool {
	return it.closed
}

func (it *seriesIterator) hasMore() bool {
	return it.iters.Len() > 0
}

func (it *seriesIterator) hasNext() bool {
	return !it.hasError() && !it.isClosed() && it.hasMore()
}

func (it *seriesIterator) moveToNext() {
	iter := heap.Pop(&it.iters).(m3db.Iterator)
	prev, _, _ := iter.Current()

	if it.moveIteratorToValidNext(iter, false) {
		heap.Push(&it.iters, iter)
	}

	if it.iters.Len() == 0 {
		return
	}

	curr, _, _ := it.Current()
	if curr.Timestamp.Equal(prev.Timestamp) {
		// Dedupe
		it.moveToNext()
	}
}

func (it *seriesIterator) moveIteratorToValidNext(iter m3db.Iterator, first bool) bool {
	var prevT time.Time
	if !first {
		prev, _, _ := iter.Current()
		prevT = prev.Timestamp
	}
	for iter.Next() {
		curr, _, _ := iter.Current()
		t := curr.Timestamp
		if t.Before(prevT) {
			// Out of order datapoint
			if it.err == nil {
				it.err = errOutOfOrderIterator
			}
			iter.Close()
			return false
		}
		if t.Before(it.start) {
			// Continue past
			prevT = t
			continue
		}
		if !t.Before(it.end) {
			// Past end
			break
		}
		return true
	}

	err := iter.Err()
	iter.Close()
	if it.err == nil && err != nil {
		it.err = err
	}
	return false
}
