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

package pool

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeHeapBasics(t *testing.T) {
	heap := NewNativeHeap([]Bucket{
		Bucket{Capacity: 128, Count: 4},
		Bucket{Capacity: 256, Count: 4},
	}, nil)

	heap.Init()

	b := heap.Get(192)[:192]
	b[128] = 'x'

	runtime.GC()

	require.Equal(t, byte('x'), b[128])

	heap.Put(b)
}

func TestNativeHeapDirect(t *testing.T) {
	heap := NewNativeHeap([]Bucket{
		Bucket{Capacity: 128, Count: 4},
		Bucket{Capacity: 256, Count: 4},
	}, nil)

	heap.Init()

	require.NotPanics(t, func() {
		heap.Put(heap.Get(4096))
	})
}

func TestNativeHeapOverflow(t *testing.T) {
	heap := NewNativeHeap([]Bucket{
		Bucket{Capacity: 128, Count: 2},
		Bucket{Capacity: 256, Count: 2},
	}, nil)

	heap.Init()

	for i := 0; i < 5; i++ {
		require.NotNil(t, heap.Get(42))
	}
}

func BenchmarkNativeHeap(b *testing.B) {
	heap := NewNativeHeap([]Bucket{
		Bucket{Capacity: 128, Count: 4},
		Bucket{Capacity: 256, Count: 4},
	}, nil)

	heap.Init()

	for n := 0; n <= b.N; n++ {
		heap.Put(heap.Get(256))
	}
}

func BenchmarkBytesHeap(b *testing.B) {
	heap := NewBytesPool([]Bucket{
		Bucket{Capacity: 128, Count: 4},
		Bucket{Capacity: 256, Count: 4},
	}, nil)

	heap.Init()

	for n := 0; n <= b.N; n++ {
		heap.Put(heap.Get(256))
	}
}
