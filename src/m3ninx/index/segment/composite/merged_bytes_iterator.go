// Copyright (c) 2018 Uber Technologies, Inc.
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

package composite

import (
	"bytes"
	"container/heap"
	"errors"

	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	xerrors "github.com/m3db/m3x/errors"
)

var (
	errIterAlreadyClosed = errors.New("iterator has been closed")
	errUnsortedInput     = errors.New("received un-sorted iterators")
)

// mergeBytesIter iterates over a collection of OrderedBytesIterator(s)
// in order.
// NB: it returns each unique value it encounters exactly once, i.e.
// if two identical iterators returning {"a", "b"} are merged, the resulting
// output is {"a", "b"}, it is NOT {"a", "a", "b", "b"}.
// NB: it is not thread-safe
type mergeBytesIter struct {
	err    error
	closed bool

	initialized bool
	hasNext     bool
	next        []byte
	current     []byte

	// iters is a min-heap of iterators, further each iter in iters has
	// returned iter.Next() == true, i.e. iter.Current() is a valid value
	iters *orderedIters

	// allIters is required to call Close() on each iterator
	allIters []sgmt.OrderedBytesIterator
}

func newMergedOrderedBytesIterator(iters ...sgmt.OrderedBytesIterator) sgmt.OrderedBytesIterator {
	copiedIters := make(orderedIters, 0, len(iters))
	for _, it := range iters {
		if it.Next() {
			copiedIters = append(copiedIters, it)
			continue
		}
		if err := it.Err(); err != nil {
			return &mergeBytesIter{err: err}
		}
	}
	minHeap := &copiedIters
	heap.Init(minHeap)
	iter := &mergeBytesIter{
		iters:    minHeap,
		allIters: iters,
	}
	iter.hasNext = iter.findNext()
	iter.initialized = true
	return iter
}

var _ sgmt.OrderedBytesIterator = &mergeBytesIter{}

func (m *mergeBytesIter) Next() bool {
	if !m.hasNext {
		return false
	}
	// copy the value from next -> current
	m.current = m.copyTo(m.current, m.next)
	m.hasNext = m.findNext()
	return true
}

func (m *mergeBytesIter) findNext() bool {
	if m.err != nil {
		return false
	}

	for m.iters.Len() > 0 {
		nextIter := heap.Pop(m.iters).(sgmt.OrderedBytesIterator)
		nextValue := nextIter.Current()
		c := bytes.Compare(m.next, nextValue)

		// sanity test, should never happen
		if c > 0 {
			m.err = errUnsortedInput
			return false
		}

		if !m.initialized || c < 0 { // i.e. nextValue is the next value to return
			m.next = m.copyTo(m.next, nextValue)
			// update nextIter and push back into heap
			if nextIter.Next() {
				heap.Push(m.iters, nextIter)
			}

			// if nextIter is exhausted, we need to be sure it's not returning error
			if err := nextIter.Err(); err != nil {
				m.err = err
			}

			return true
		}

		// i.e. nextValue is the same as m.next, so need to update nextIter
		if nextIter.Next() { // i.e. nextIter is not exhausted, push back onto heap
			heap.Push(m.iters, nextIter)
		}

		// if nextIter is exhausted, we need to be sure it's not returning error
		if err := nextIter.Err(); err != nil {
			m.err = err
			return false
		}

		// i.e. the nextIter is exhausted, so we try the next one
	}
	return false
}

// need to take a copy of the underlying bytes as they are only guaranteed till the next Next() call
// copyTo returns the dst to propagate the new slice header
func (m *mergeBytesIter) copyTo(dst, src []byte) []byte {
	// preserve bytes slice to amortise allocations
	dst = dst[:0]
	dst = append(dst, src...)
	return dst
}

func (m *mergeBytesIter) Current() []byte {
	return m.current
}

func (m *mergeBytesIter) Err() error {
	if m.closed {
		return errIterAlreadyClosed
	}
	return m.err
}

func (m *mergeBytesIter) Close() error {
	if m.closed {
		return errIterAlreadyClosed
	}
	m.closed = true
	m.next = nil
	m.current = nil
	var multiErr xerrors.MultiError
	for _, i := range m.allIters {
		multiErr = multiErr.Add(i.Close())
	}
	m.allIters = nil
	m.iters = nil
	return multiErr.FinalError()
}

func (m *mergeBytesIter) Len() int {
	if m.closed {
		return 0
	}
	estimatedLen := 0
	for _, i := range *m.iters {
		estimatedLen += i.Len()
	}
	return estimatedLen
}

// implements heap.Interface
type orderedIters []sgmt.OrderedBytesIterator

var _ heap.Interface = &orderedIters{}

func (o orderedIters) Len() int {
	return len(o)
}

func (o orderedIters) Less(i int, j int) bool {
	return bytes.Compare(o[i].Current(), o[j].Current()) < 0
}

func (o orderedIters) Swap(i int, j int) {
	o[i], o[j] = o[j], o[i]
}

func (o *orderedIters) Push(x interface{}) {
	item := x.(sgmt.OrderedBytesIterator)
	*o = append(*o, item)
}

func (o *orderedIters) Pop() interface{} {
	old := *o
	n := len(old)
	iter := old[n-1]
	*o = old[0 : n-1]
	return iter
}
