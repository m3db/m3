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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

type maxSlice []ValueIndexPair

func (s maxSlice) Len() int      { return len(s) }
func (s maxSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s maxSlice) Less(i, j int) bool {
	if s[i].val == s[j].val {
		return s[i].index > s[j].index
	}
	return s[i].val < s[j].val
}

type minSlice []ValueIndexPair

func (s minSlice) Len() int      { return len(s) }
func (s minSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s minSlice) Less(i, j int) bool {
	if s[i].val == s[j].val {
		return s[i].index > s[j].index
	}
	return s[i].val > s[j].val
}

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
			for i, v := range tt.values {
				h.Push(v, i)
			}

			// Flush and sort results (Flush does not care about order)
			actual := h.Flush()
			sort.Sort(maxSlice(actual))
			assert.Equal(t, tt.expectedMax, actual)
			// Assert Flush flushes the heap
			assert.Equal(t, 0, h.floatHeap.Len())

			// Refill heap
			for i, v := range tt.values {
				h.Push(v, i)
			}

			actual = h.FlushByPop()
			assert.Equal(t, tt.expectedMax, actual)
			// Assert FlushByPop flushes the heap
			assert.Equal(t, 0, h.floatHeap.Len())
		})
	}
}

func TestMinHeap(t *testing.T) {
	for _, tt := range heapTests {
		t.Run(tt.name, func(t *testing.T) {
			size := tt.size
			h := NewFloatHeap(false, size)
			for i, v := range tt.values {
				h.Push(v, i)
			}

			// Flush and sort results (Flush does not care about order)
			actual := h.Flush()
			sort.Sort(minSlice(actual))
			assert.Equal(t, tt.expectedMin, actual)
			// Assert Flush flushes the heap
			assert.Equal(t, 0, h.floatHeap.Len())

			// Refill heap
			for i, v := range tt.values {
				h.Push(v, i)
			}

			actual = h.FlushByPop()
			assert.Equal(t, tt.expectedMin, actual)
		})
	}
}
