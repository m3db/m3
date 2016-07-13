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
	"io"

	"github.com/m3db/m3db/interfaces/m3db"
	xtime "github.com/m3db/m3db/x/time"
)

var (
	errOutOfOrderIterator = errors.New("series values are out of order from mixed reader")
)

// multiReaderIterator is an iterator that iterates in order over a list of sets of
// internally ordered but not collectively in order readers, it also deduplicates datapoints.
type multiReaderIterator struct {
	iters            IteratorHeap
	cachedIters      []m3db.ReaderIterator
	slicesIter       m3db.ReaderSliceOfSlicesIterator
	iteratorAlloc    m3db.ReaderIteratorAllocate
	singleSlicesIter singleSlicesOfSlicesIterator
	pool             m3db.MultiReaderIteratorPool
	err              error
	clean            bool
	closed           bool
}

// NewMultiReaderIterator creates a new multi-reader iterator.
func NewMultiReaderIterator(
	iteratorAlloc m3db.ReaderIteratorAllocate,
	pool m3db.MultiReaderIteratorPool,
) m3db.MultiReaderIterator {
	it := &multiReaderIterator{pool: pool, iteratorAlloc: iteratorAlloc}
	it.Reset(nil)
	return it
}

func (it *multiReaderIterator) Next() bool {
	if !it.clean {
		// When clean do not progress the first time
		if !it.hasNext() {
			return false
		}
		it.moveToNext()
	}
	it.clean = false
	return it.hasNext()
}

func (it *multiReaderIterator) Current() (m3db.Datapoint, xtime.Unit, m3db.Annotation) {
	return it.iters[0].Current()
}

func (it *multiReaderIterator) hasError() bool {
	return it.err != nil
}

func (it *multiReaderIterator) isClosed() bool {
	return it.closed
}

func (it *multiReaderIterator) hasMore() bool {
	return it.iters.Len() > 0 || it.slicesIter != nil
}

func (it *multiReaderIterator) hasNext() bool {
	return !it.hasError() && !it.isClosed() && it.hasMore()
}

func (it *multiReaderIterator) moveToNext() {
	if it.iters.Len() > 0 {
		it.moveIteratorsToNext()
	}
	if it.iters.Len() > 0 || it.hasError() {
		// Still have valid iters or has error
		return
	}

	// Move forward through slices of readers
	if !it.slicesIter.Next() {
		// No more readers, nil out so that hasMore reflects correctly
		it.slicesIter.Close()
		it.slicesIter = nil
		return
	}

	// Add all readers to current iterators heap
	currentLen := it.slicesIter.CurrentLen()
	for i := 0; i < currentLen; i++ {
		var (
			iter   m3db.ReaderIterator
			reader = it.slicesIter.CurrentAt(i)
		)
		if len(it.cachedIters) != 0 {
			iter = it.cachedIters[len(it.cachedIters)-1]
			iter.Reset(reader)
			it.cachedIters = it.cachedIters[:len(it.cachedIters)-1]
		} else {
			iter = it.iteratorAlloc(reader)
		}
		if iter.Next() {
			// Only insert it if it has values
			heap.Push(&it.iters, iter)
		} else {
			err := iter.Err()
			iter.Close()
			if err != nil {
				it.err = err
			}
		}
	}

	if it.iters.Len() == 0 && !it.hasError() {
		// No iterators were added, move to next
		it.moveToNext()
	}
}

func (it *multiReaderIterator) moveIteratorsToNext() {
	iter := heap.Pop(&it.iters).(m3db.ReaderIterator)
	prev, _, _ := iter.Current()

	if it.moveIteratorToValidNext(iter) {
		heap.Push(&it.iters, iter)
	} else {
		it.cachedIters = append(it.cachedIters, iter)
	}

	if it.iters.Len() == 0 {
		return
	}

	curr, _, _ := it.Current()
	if curr.Timestamp.Equal(prev.Timestamp) {
		// Dedupe
		it.moveIteratorsToNext()
	}
}

func (it *multiReaderIterator) moveIteratorToValidNext(iter m3db.Iterator) bool {
	prev, _, _ := iter.Current()
	prevT := prev.Timestamp
	if iter.Next() {
		curr, _, _ := iter.Current()
		t := curr.Timestamp
		if t.Before(prevT) {
			// Out of order datapoint
			it.err = errOutOfOrderIterator
			iter.Close()
			return false
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

func (it *multiReaderIterator) Err() error {
	return it.err
}

func (it *multiReaderIterator) Reset(readers []io.Reader) {
	it.singleSlicesIter.readers = readers
	it.singleSlicesIter.clean = true
	it.singleSlicesIter.closed = false
	it.ResetSliceOfSlices(&it.singleSlicesIter)
}

func (it *multiReaderIterator) ResetSliceOfSlices(slicesIter m3db.ReaderSliceOfSlicesIterator) {
	it.iters = it.iters[:0]
	it.slicesIter = slicesIter
	it.err = nil
	it.clean = true
	it.closed = false
	heap.Init(&it.iters)
	// Try moveToNext to load values for calls to Current before Next
	it.moveToNext()
}

func (it *multiReaderIterator) Close() {
	if it.isClosed() {
		return
	}
	it.closed = true
	for _, iter := range it.iters {
		iter.Close()
	}
	if it.slicesIter != nil {
		it.slicesIter.Close()
	}
	it.slicesIter = nil
	if it.pool != nil {
		it.pool.Put(it)
	}
}

type singleSlicesOfSlicesIterator struct {
	readers []io.Reader
	clean   bool
	closed  bool
}

func (it *singleSlicesOfSlicesIterator) Next() bool {
	if !it.clean || it.closed {
		return false
	}
	it.clean = false
	return true
}

func (it *singleSlicesOfSlicesIterator) CurrentLen() int {
	return len(it.readers)
}

func (it *singleSlicesOfSlicesIterator) CurrentAt(idx int) io.Reader {
	return it.readers[idx]
}

func (it *singleSlicesOfSlicesIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
}
