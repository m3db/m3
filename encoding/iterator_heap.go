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

import "github.com/m3db/m3db/interfaces/m3db"

// An IteratorHeap is a min-heap of iterators. The top of the heap is the iterator
// whose current value is the earliest datapoint among all iterators in the heap.
type IteratorHeap []m3db.Iterator

func (h IteratorHeap) Len() int {
	return len(h)
}

func (h IteratorHeap) Less(i, j int) bool {
	di, _, _ := h[i].Current()
	dj, _, _ := h[j].Current()
	return di.Timestamp.Before(dj.Timestamp)
}

func (h IteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push an iterator
func (h *IteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(m3db.Iterator))
}

// Pop an iterator
func (h *IteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	if n == 0 {
		return nil
	}

	x := old[n-1]
	*h = old[:n-1]
	return x
}
