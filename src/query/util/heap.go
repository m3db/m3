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

package util

import (
	"container/heap"
)

// ValueIndexPair is a pair of float value and index at which it exists
type ValueIndexPair struct {
	val   float64
	index int
}

// FloatHeap is a heap that can be given a maximum size
type FloatHeap struct {
	isMaxHeap bool
	maxSize   int
	heap      []ValueIndexPair
}

// Assert that FloatHeap is a heap.interface
var _ heap.Interface = (*FloatHeap)(nil)

// NewFloatHeap builds a new FloatHeap based on first parameter
// and a maxSize given by second parameter. Zero and negative
// values for maxSize provide an unbounded FloatHeap
func NewFloatHeap(isMaxHeap bool, maxSize int) *FloatHeap {
	return &FloatHeap{
		isMaxHeap: isMaxHeap,
		heap:      make([]ValueIndexPair, 0, maxSize),
		maxSize:   maxSize,
	}
}

// Len gives the length of items in the heap
func (h *FloatHeap) Len() int {
	return len(h.heap)
}

// Less is true if value of i is less than value of j
func (h *FloatHeap) Less(i, j int) bool {
	heap := h.heap
	iVal, jVal := heap[i].val, heap[j].val
	if iVal == jVal {
		return heap[i].index > heap[j].index
	}
	less := iVal < jVal
	return h.isMaxHeap == less
}

// Swap swaps values at these indices
func (h *FloatHeap) Swap(i, j int) {
	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
}

// Push pushes a ValueIndexPair to the FloatMaxHeap
func (h *FloatHeap) Push(x interface{}) {
	pair := x.(ValueIndexPair)
	// If maxSize is zero or negative, allow infinite size heap
	if h.maxSize > 0 {
		// At max size, drop or replace incoming value.
		// Otherwise, continue as normal
		if len(h.heap) >= h.maxSize {
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
			if h.isMaxHeap && pair.val > peek.val ||
				(!h.isMaxHeap && pair.val < peek.val) {
				h.heap[0] = pair
				heap.Fix(h, 0)
			}

			return
		}
	}
	h.heap = append(h.heap, pair)
}

// Pop pops a ValueIndexPair from the FloatMaxHeap
func (h *FloatHeap) Pop() interface{} {
	old := h.heap
	n := len(old)
	tail := old[n-1]
	h.heap = old[:n-1]
	return tail
}
