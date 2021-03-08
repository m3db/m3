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

package cm

// minHeap is a typed min heap for floating point numbers. Unlike the generic
// heap in the container/heap package, pushing data to or popping data off of
// the heap doesn't require conversion between floats and interface{} objects,
// therefore avoiding the memory and GC overhead due to the additional allocations.
type minHeap []float64

// Len returns the number of values in the heap.
func (h minHeap) Len() int { return len(h) }

// Min returns the minimum value from the heap.
func (h minHeap) Min() float64 { return h[0] }

// Push pushes a value onto the heap.
func (h *minHeap) Push(value float64) {
	if len(*h) == cap(*h) {
		h.ensureSize()
	}
	// append
	(*h) = append(*h, value)
	// then, shift up if necessary to fix heap structure. manually inlined.
	heap := *h
	n := len(heap)
	i := n - 1
	for i < n && i >= 0 {
		parent := (i - 1) / 2
		if parent == i || parent >= n || parent < 0 || heap[parent] <= heap[i] {
			break
		}
		heap[parent], heap[i] = heap[i], heap[parent]
		i = parent
	}
}

func (h *minHeap) Reset() {
	if heap := *h; cap(heap) >= _initialHeapBucketSize {
		sharedHeapPool.Put(heap)
	}
	(*h) = nil
}

// Pop pops the minimum value from the heap.
func (h *minHeap) Pop() float64 {
	var (
		old = *h
		n   = len(old) - 1
		val = old[0]
		i   int
	)
	old[0], old[n] = old[n], old[0]
	smallest := i
	for smallest >= 0 && smallest <= n { // bounds-check elimination hint
		left := smallest*2 + 1
		right := left + 1
		if left < n && left >= 0 && old[left] < old[smallest] {
			smallest = left
		}
		if right < n && right >= 0 && old[right] < old[smallest] {
			smallest = right
		}
		if smallest == i {
			break
		}
		old[i], old[smallest] = old[smallest], old[i]
		i = smallest
	}
	*h = old[0:n]
	return val
}

func (h minHeap) SortDesc() {
	heap := h
	// this is equivalent to Pop() in a loop (heapsort)
	// all the redundant-looking conditions are there to eliminate bounds-checks
	for n := len(heap) - 1; n > 0 && n < len(heap); n = len(heap) - 1 {
		var (
			i        int
			smallest int
		)
		heap[0], heap[n] = heap[n], heap[0]
		for smallest >= 0 && smallest <= n {
			var (
				left  = smallest*2 + 1
				right = left + 1
			)
			if left < n && left >= 0 && heap[left] < heap[smallest] {
				smallest = left
			}
			if right < n && right >= 0 && heap[right] < heap[smallest] {
				smallest = right
			}
			if smallest == i {
				break
			}
			heap[i], heap[smallest] = heap[smallest], heap[i]
			i = smallest
		}
		heap = heap[0:n]
	}
}

func (h *minHeap) ensureSize() {
	var (
		heap      = *h
		targetCap = cap(heap) * 2
		newHeap   = sharedHeapPool.Get(targetCap)
	)
	(*newHeap) = append(*newHeap, heap...)
	if cap(heap) >= _initialHeapBucketSize {
		sharedHeapPool.Put(heap)
	}
	(*h) = *newHeap
}
