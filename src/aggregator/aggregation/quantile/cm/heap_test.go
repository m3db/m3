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

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMinHeapPushInDecreasingOrder(t *testing.T) {
	h := &minHeap{}
	iter := 10
	for i := iter - 1; i >= 0; i-- {
		h.Push(float64(i))
		require.Equal(t, iter-i, h.Len())
	}
	validateSort(t, *h)
	for i := 0; i < iter; i++ {
		require.Equal(t, float64(i), h.Min())
		require.Equal(t, float64(i), h.Pop())
		validateInvariant(t, *h, 0)
	}
}

func TestMinHeapPushInIncreasingOrder(t *testing.T) {
	h := &minHeap{}
	iter := 10
	for i := 0; i < iter; i++ {
		h.Push(float64(i))
		require.Equal(t, i+1, h.Len())
	}
	validateSort(t, *h)
	for i := 0; i < iter; i++ {
		require.Equal(t, float64(i), h.Min())
		require.Equal(t, float64(i), h.Pop())
		validateInvariant(t, *h, 0)
	}
}

func TestMinHeapPushInRandomOrderAndSort(t *testing.T) {
	h := &minHeap{}
	iter := 42
	for i := 0; i < iter; i++ {
		h.Push(rand.ExpFloat64())
	}
	validateSort(t, *h)
}

func validateSort(t *testing.T, h minHeap) {
	t.Helper()
	// copy heap before applying reference sort and minheap-sort
	a := make([]float64, h.Len())
	b := make([]float64, h.Len())
	for i := 0; i < len(h); i++ {
		a[i], b[i] = h[i], h[i]
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(a)))
	heap := (*minHeap)(&b)
	heap.SortDesc()
	require.Equal(t, a, b)
}

func validateInvariant(t *testing.T, h minHeap, i int) {
	var (
		n     = h.Len()
		left  = 2*i + 1
		right = 2*i + 2
	)

	if left < n {
		require.True(t, h[i] <= h[left])
		validateInvariant(t, h, left)
	}

	if right < n {
		require.True(t, h[i] <= h[right])
		validateInvariant(t, h, right)
	}
}
