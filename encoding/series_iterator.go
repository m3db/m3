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

var (
	timeZero time.Time
)

type seriesIterator struct {
	id     string
	start  time.Time
	end    time.Time
	iters  IteratorHeap
	lastAt time.Time
	err    error
	closed bool
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
	if !it.hasNext() {
		return false
	}
	it.moveToNext()
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
	// TODO(r): when able to be pooled check if has pool and return to pool
}

func (it *seriesIterator) Reset(id string, startInclusive, endExclusive time.Time, replicas []m3db.Iterator) {
	it.id = id
	it.start = startInclusive
	it.end = endExclusive
	it.iters = it.iters[:0]
	it.lastAt = timeZero
	it.err = nil
	heap.Init(&it.iters)
	for _, replicaIter := range replicas {
		if replicaIter == nil {
			continue
		}
		if replicaIter.Next() {
			heap.Push(&it.iters, replicaIter)
		} else {
			err := replicaIter.Err()
			replicaIter.Close()
			if err != nil {
				it.err = err
				return
			}
		}
	}
}

func (it *seriesIterator) hasError() bool {
	return it.err != nil
}

func (it *seriesIterator) isClosed() bool {
	return it.closed
}

func (it *seriesIterator) hasMore() bool {
	return it.iters == nil || it.iters.Len() > 0
}

func (it *seriesIterator) hasNext() bool {
	return !it.hasError() && !it.isClosed() && it.hasMore()
}

func (it *seriesIterator) moveToNext() {
	earliest := heap.Pop(&it.iters).(m3db.Iterator)
	if earliest.Next() {
		heap.Push(&it.iters, earliest)

		// Now de-dupe and apply filters
		dp, _, _ := earliest.Current()
		if dp.Timestamp.Before(it.start) ||
			!dp.Timestamp.Before(it.end) ||
			!dp.Timestamp.After(it.lastAt) {
			it.moveToNext()
		} else {
			it.lastAt = dp.Timestamp
		}
	} else {
		err := earliest.Err()
		earliest.Close()
		if err != nil {
			it.err = err
		}
	}
}
