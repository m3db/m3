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

package utils

import (
	"container/heap"
	"math"
	"sort"
)

// ValueIndexPair is a pair of float value and index at which it exists
type ValueIndexPair struct {
	Val   float64
	Index int
}

type lessFn func(ValueIndexPair, ValueIndexPair) bool

func maxHeapLess(i, j ValueIndexPair) bool {
	if equalWithNaNs(i.Val, j.Val) {
		return i.Index > j.Index
	}
	return i.Val < j.Val || lesserIfNaNs(i.Val, j.Val)
}

func minHeapLess(i, j ValueIndexPair) bool {
	if equalWithNaNs(i.Val, j.Val) {
		return i.Index > j.Index
	}
	return i.Val > j.Val || lesserIfNaNs(i.Val, j.Val)
}

// Compares two floats for equality with NaNs taken into account.
func equalWithNaNs(i, j float64) bool {
	return i == j || math.IsNaN(i) && math.IsNaN(j)
}

// Compares NaNs.
// Basically, we do not want to add NaNs to the heap when it has reached it's cap so this fn should be used to prevent this.
func lesserIfNaNs(i, j float64) bool {
	return math.IsNaN(i) && !math.IsNaN(j)
}

// Compares two float64 values which one is lesser with NaNs. NaNs are always sorted away.
func LesserWithNaNs(i, j float64) bool {
	return i < j || math.IsNaN(j) && !math.IsNaN(i)
}

// Compares two float64 values which one is greater with NaNs. NaNs are always sorted away.
func GreaterWithNaNs(i, j float64) bool {
	return i > j || math.IsNaN(j) && !math.IsNaN(i)
}

// FloatHeap is a heap that can be given a maximum size
type FloatHeap struct {
	isMaxHeap bool
	capacity  int
	floatHeap *floatHeap
}

// NewFloatHeap builds a new FloatHeap based on first parameter
// and a capacity given by second parameter. Zero and negative
// values for maxSize provide an unbounded FloatHeap
func NewFloatHeap(isMaxHeap bool, capacity int) FloatHeap {
	var less lessFn
	if isMaxHeap {
		less = maxHeapLess
	} else {
		less = minHeapLess
	}

	if capacity < 1 {
		capacity = 0
	}

	floatHeap := &floatHeap{
		heap: make([]ValueIndexPair, 0, capacity),
		less: less,
	}

	heap.Init(floatHeap)
	return FloatHeap{
		isMaxHeap: isMaxHeap,
		capacity:  capacity,
		floatHeap: floatHeap,
	}
}

// Push pushes a value and index pair to the heap
func (fh *FloatHeap) Push(value float64, index int) {
	h := fh.floatHeap
	// If capacity is zero or negative, allow infinite size heap
	if fh.capacity > 0 {
		// At max size, drop or replace incoming value.
		// Otherwise, continue as normal
		if len(h.heap) >= fh.capacity {
			peek := h.heap[0]
			// Compare incoming value with current top of heap.
			// Decide if to replace the current top, or to drop incoming
			// value as appropriate for this min/max heap
			//
			// If values are equal, do not add incoming value regardless
			//
			// NB(arnikola): unfortunately, can't just replace first
			// element as it may not respect internal order. Need to
			// run heap.Fix() to rectify this
			if (fh.isMaxHeap && GreaterWithNaNs(value, peek.Val)) ||
				(!fh.isMaxHeap && LesserWithNaNs(value, peek.Val)) {
				h.heap[0] = ValueIndexPair{
					Val:   value,
					Index: index,
				}

				heap.Fix(h, 0)
			}

			return
		}

		// Otherwise, fallthrough
	}

	heap.Push(h, ValueIndexPair{
		Val:   value,
		Index: index,
	})
}

// Len returns the current length of the heap
func (fh *FloatHeap) Len() int {
	return fh.floatHeap.Len()
}

// Cap returns the capacity of the heap
func (fh *FloatHeap) Cap() int {
	return fh.capacity
}

// Reset resets the heap
func (fh *FloatHeap) Reset() {
	fh.floatHeap.heap = fh.floatHeap.heap[:0]
}

// Flush flushes the float heap and resets it. Does not guarantee order.
func (fh *FloatHeap) Flush() []ValueIndexPair {
	values := fh.floatHeap.heap
	fh.Reset()
	return values
}

// OrderedFlush flushes the float heap and returns values in order.
func (fh *FloatHeap) OrderedFlush() []ValueIndexPair {
	flushed := fh.Flush()
	sort.Slice(flushed, func(i, j int) bool {
		return !fh.floatHeap.less(flushed[i], flushed[j]) //reverse sort
	})
	return flushed
}

// Peek reveals the top value of the heap without mutating the heap.
func (fh *FloatHeap) Peek() (ValueIndexPair, bool) {
	h := fh.floatHeap.heap
	if len(h) == 0 {
		return ValueIndexPair{}, false
	}
	return h[0], true
}

// floatHeap is a heap that can be given a maximum size
type floatHeap struct {
	less lessFn
	heap []ValueIndexPair
}

// Assert that floatHeap is a heap.Interface
var _ heap.Interface = (*floatHeap)(nil)

// Len gives the length of items in the heap
func (h *floatHeap) Len() int {
	return len(h.heap)
}

// Less is true if value of i is less than value of j
func (h *floatHeap) Less(i, j int) bool {
	return h.less(h.heap[i], h.heap[j])
}

// Swap swaps values at these indices
func (h *floatHeap) Swap(i, j int) {
	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
}

// Push pushes a ValueIndexPair to the FloatMaxHeap
func (h *floatHeap) Push(x interface{}) {
	pair := x.(ValueIndexPair)
	h.heap = append(h.heap, pair)
}

// Pop pops a ValueIndexPair from the FloatMaxHeap
func (h *floatHeap) Pop() interface{} {
	old := h.heap
	n := len(old)
	tail := old[n-1]
	h.heap = old[:n-1]
	return tail
}
