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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

var heapTests = []struct {
	name        string
	size        int
	values      []float64
	expectedMax []ValueIndexPair
	expectedMin []ValueIndexPair
}{
	{
		"size 0",
		0,
		[]float64{1, 8, 2, 4, 2, 3, 0, -3, 3},
		[]ValueIndexPair{
			{-3, 7},
			{0, 6},
			{1, 0},
			{2, 4},
			{2, 2},
			{3, 8},
			{3, 5},
			{4, 3},
			{8, 1},
		},
		[]ValueIndexPair{
			{8, 1},
			{4, 3},
			{3, 8},
			{3, 5},
			{2, 4},
			{2, 2},
			{1, 0},
			{0, 6},
			{-3, 7},
		},
	},
	{
		"size 1",
		1,
		[]float64{1, 8, 2, 4, 2, 3, 0, -3, 3},
		[]ValueIndexPair{
			{8, 1},
		},
		[]ValueIndexPair{
			{-3, 7},
		},
	},
	{
		"size 3",
		3,
		[]float64{1, 8, 2, 4, 2, 3, 0, -3, 3},
		[]ValueIndexPair{
			// NB: since two values at 3, index is first one to come in
			{3, 5},
			{4, 3},
			{8, 1},
		},
		[]ValueIndexPair{
			{1, 0},
			{0, 6},
			{-3, 7},
		},
	},
	{
		"size 4",
		4,
		[]float64{1, 8, 2, 4, 2, 3, 0, -3, 3},
		[]ValueIndexPair{
			{3, 8},
			{3, 5},
			{4, 3},
			{8, 1},
		},
		[]ValueIndexPair{
			{2, 2},
			{1, 0},
			{0, 6},
			{-3, 7},
		},
	},
}

func TestMaxHeap(t *testing.T) {
	for _, tt := range heapTests {
		t.Run(tt.name, func(t *testing.T) {
			size := tt.size
			h := NewFloatHeap(true, size)
			heap.Init(h)
			for i, v := range tt.values {
				pair := ValueIndexPair{
					val:   v,
					index: i,
				}
				heap.Push(h, pair)
				if size < 1 {
					// No max size; length should be index + 1
					assert.Equal(t, i+1, h.Len(), "size <= 0, no max size")
				} else {
					assert.True(t, h.Len() <= size, "length is larger than max size")
				}
			}

			actual := make([]ValueIndexPair, 0, h.Len())
			for h.Len() > 0 {
				pair := heap.Pop(h).(ValueIndexPair)
				actual = append(actual, pair)
			}
			assert.Equal(t, tt.expectedMax, actual)
		})
	}
}

func TestMinHeap(t *testing.T) {
	for _, tt := range heapTests {
		t.Run(tt.name, func(t *testing.T) {
			size := tt.size
			h := NewFloatHeap(false, size)
			heap.Init(h)
			for i, v := range tt.values {
				pair := ValueIndexPair{
					val:   v,
					index: i,
				}
				heap.Push(h, pair)
				if size < 1 {
					// No max size; length should be index + 1
					assert.Equal(t, i+1, h.Len(), "size <= 0, no max size")
				} else {
					assert.True(t, h.Len() <= size, "length is larger than max size")
				}
			}

			actual := make([]ValueIndexPair, 0, h.Len())
			for h.Len() > 0 {
				pair := heap.Pop(h).(ValueIndexPair)
				actual = append(actual, pair)
			}
			assert.Equal(t, tt.expectedMin, actual)
		})
	}
}

func BenchmarkHeap(b *testing.B) {
	values := make([]float64, 100000)
	for i := range values {
		values[i] = rand.Float64()
	}
	h := NewFloatHeap(true, 0)
	heap.Init(h)
	for i := 0; i < b.N; i++ {
		for i, v := range values {
			// if len(h.heap) == 0 {
			// 	heap.Push(h, ValueIndexPair{
			// 		val:   v,
			// 		index: i,
			// 	})
			// 	continue
			// }
			// if v >= h.heap[0].val {
			// 	continue
			// }

			pair := ValueIndexPair{
				val:   v,
				index: i,
			}
			heap.Push(h, pair)
		}
	}
}

// BenchmarkHeap-8   	     500	   3395086 ns/op	 1601606 B/op	  100000 allocs/op

// with fix: BenchmarkHeap-8   	   10000	    107431 ns/op	      80 B/op	       0 allocs/op
// with manual: BenchmarkHeap-8   	   10000	    111572 ns/op	      80 B/op	       0 allocs/op

// limit(20)  500           3419644 ns/op         1601610 B/op     100000 allocs/op
// BenchmarkHeap-8   	   10000	    130780 ns/op	      80 B/op	       0 allocs/op
// BenchmarkHeap-8   	   10000	    126953 ns/op	      80 B/op	       0 allocs/op
// no pointer pre-checked BenchmarkHeap-8   	   10000	    111578 ns/op	      80 B/op	       0 allocs/op
// unlimited  200	          8568156 ns/op	         9687749 B/op	   100000 allocs/op
