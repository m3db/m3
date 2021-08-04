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
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/m3db/m3/src/query/test/compare"

	"github.com/stretchr/testify/assert"
)

type maxSlice []ValueIndexPair

func (s maxSlice) Len() int      { return len(s) }
func (s maxSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s maxSlice) Less(i, j int) bool {
	if s[i].Val == s[j].Val {
		return s[i].Index > s[j].Index
	}
	return s[i].Val < s[j].Val
}

type minSlice []ValueIndexPair

func (s minSlice) Len() int      { return len(s) }
func (s minSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s minSlice) Less(i, j int) bool {
	if s[i].Val == s[j].Val {
		return s[i].Index > s[j].Index
	}

	return s[i].Val > s[j].Val
}

var heapTests = []struct {
	name        string
	capacity    int
	values      []float64
	expectedMax []ValueIndexPair
	expectedMin []ValueIndexPair
}{
	{
		"capacity 0",
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
		"capacity 1",
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
		"capacity 3",
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
		"capacity 4",
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
			capacity := tt.capacity
			h := NewFloatHeap(true, capacity)
			assert.Equal(t, capacity, h.Cap())
			_, seen := h.Peek()
			assert.False(t, seen)

			for i, v := range tt.values {
				h.Push(v, i)
				if capacity < 1 {
					// No max size; length should be index + 1
					assert.Equal(t, i+1, h.Len(), "capacity <= 0, no max capacity")
				} else {
					assert.True(t, h.Len() <= capacity, "length is larger than capacity")
				}
			}

			peek, seen := h.Peek()
			assert.True(t, seen)
			assert.Equal(t, peek, tt.expectedMax[0])

			// Flush and sort results (Flush does not care about order)
			actual := h.Flush()
			sort.Sort(maxSlice(actual))
			assert.Equal(t, tt.expectedMax, actual)
			// Assert Flush flushes the heap
			assert.Equal(t, 0, h.floatHeap.Len())
			_, seen = h.Peek()
			assert.False(t, seen)
		})
	}
}

func TestMinHeap(t *testing.T) {
	for _, tt := range heapTests {
		t.Run(tt.name, func(t *testing.T) {
			capacity := tt.capacity
			h := NewFloatHeap(false, capacity)
			assert.Equal(t, capacity, h.Cap())
			_, seen := h.Peek()
			assert.False(t, seen)

			for i, v := range tt.values {
				h.Push(v, i)
			}

			peek, seen := h.Peek()
			assert.True(t, seen)
			assert.Equal(t, peek, tt.expectedMin[0])

			// Flush and sort results (Flush does not care about order)
			actual := h.Flush()
			sort.Sort(minSlice(actual))
			assert.Equal(t, tt.expectedMin, actual)
			// Assert Flush flushes the heap
			assert.Equal(t, 0, h.floatHeap.Len())
			_, seen = h.Peek()
			assert.False(t, seen)
		})
	}
}

func TestNegativeCapacityHeap(t *testing.T) {
	h := NewFloatHeap(false, -1)
	assert.Equal(t, 0, h.Cap())
	_, seen := h.Peek()
	assert.False(t, seen)

	length := 10000
	testArray := make([]float64, length)
	for i := range testArray {
		testArray[i] = rand.Float64()
		h.Push(testArray[i], i)
	}

	assert.Equal(t, length, h.Len())
	flushed := h.Flush()
	assert.Equal(t, length, len(flushed))
	assert.Equal(t, 0, h.Len())
	for _, pair := range flushed {
		assert.Equal(t, testArray[pair.Index], pair.Val)
	}
}

func equalPairs(t *testing.T, expected, actual []ValueIndexPair) {
	assert.Equal(t, len(expected), len(actual))
	for i, e := range expected {
		compare.EqualsWithNans(t, e.Val, actual[i].Val)
		assert.Equal(t, e.Index, actual[i].Index)
	}
}

func TestFlushOrdered(t *testing.T) {
	maxHeap := NewFloatHeap(true, 3)

	maxHeap.Push(0.1, 0)
	maxHeap.Push(1.1, 1)
	maxHeap.Push(2.1, 2)
	maxHeap.Push(3.1, 3)

	actualMax := maxHeap.OrderedFlush()

	assert.Equal(t, []ValueIndexPair{
		{Val: 3.1, Index: 3},
		{Val: 2.1, Index: 2},
		{Val: 1.1, Index: 1},
	}, actualMax)
	assert.Equal(t, 0, maxHeap.Len())

	minHeap := NewFloatHeap(false, 3)
	minHeap.Push(0.1, 0)
	minHeap.Push(1.1, 1)
	minHeap.Push(2.1, 2)
	minHeap.Push(3.1, 3)

	actualMin := minHeap.OrderedFlush()

	assert.Equal(t, []ValueIndexPair{
		{Val: 0.1, Index: 0},
		{Val: 1.1, Index: 1},
		{Val: 2.1, Index: 2},
	}, actualMin)
	assert.Equal(t, 0, minHeap.Len())
}

func TestFlushOrderedWhenRandomInsertionOrder(t *testing.T) {
	maxHeap := NewFloatHeap(true, 3)

	maxHeap.Push(math.NaN(), 4)
	maxHeap.Push(0.1, 0)
	maxHeap.Push(2.1, 2)
	maxHeap.Push(1.1, 1)
	maxHeap.Push(3.1, 3)
	maxHeap.Push(math.NaN(), 5)

	actualMax := maxHeap.OrderedFlush()

	assert.Equal(t, []ValueIndexPair{
		{Val: 3.1, Index: 3},
		{Val: 2.1, Index: 2},
		{Val: 1.1, Index: 1},
	}, actualMax)
	assert.Equal(t, 0, maxHeap.Len())

	minHeap := NewFloatHeap(false, 3)
	maxHeap.Push(math.NaN(), 4)
	minHeap.Push(0.1, 0)
	minHeap.Push(2.1, 2)
	minHeap.Push(1.1, 1)
	minHeap.Push(3.1, 3)
	maxHeap.Push(math.NaN(), 5)

	actualMin := minHeap.OrderedFlush()

	assert.Equal(t, []ValueIndexPair{
		{Val: 0.1, Index: 0},
		{Val: 1.1, Index: 1},
		{Val: 2.1, Index: 2},
	}, actualMin)
	assert.Equal(t, 0, minHeap.Len())
}

func TestFlushOrderedWhenRandomInsertionOrderAndTakeNaNs(t *testing.T) {
	maxHeap := NewFloatHeap(true, 3)
	maxHeap.Push(math.NaN(), 4)
	maxHeap.Push(1.1, 1)
	maxHeap.Push(3.1, 3)
	maxHeap.Push(math.NaN(), 5)

	actualMax := maxHeap.OrderedFlush()

	equalPairs(t, []ValueIndexPair{
		{Val: 3.1, Index: 3},
		{Val: 1.1, Index: 1},
		{Val: math.NaN(), Index: 4},
	}, actualMax)
	assert.Equal(t, 0, maxHeap.Len())

	minHeap := NewFloatHeap(false, 3)
	minHeap.Push(math.NaN(), 4)
	minHeap.Push(0.1, 0)
	minHeap.Push(2.1, 2)
	minHeap.Push(math.NaN(), 5)

	actualMin := minHeap.OrderedFlush()

	equalPairs(t, []ValueIndexPair{
		{Val: 0.1, Index: 0},
		{Val: 2.1, Index: 2},
		{Val: math.NaN(), Index: 4},
	}, actualMin)
	assert.Equal(t, 0, minHeap.Len())
}

func TestSortLesserWithNaNs(t *testing.T) {
	actual := []float64{ 5.0, 4.1, math.NaN(), 8.6, 0.1 }
	expected := []float64{ 0.1, 4.1, 5.0, 8.6, math.NaN() }

	sort.Slice(actual, func(i, j int) bool {
		return LesserWithNaNs(actual[i], actual[j])
	})

	compare.EqualsWithNans(t, expected, actual)
}

func TestSortGreaterWithNaNs(t *testing.T) {
	actual := []float64{ 5.0, 4.1, math.NaN(), 8.6, 0.1 }
	expected := []float64{ 8.6, 5.0, 4.1, 0.1, math.NaN() }

	sort.Slice(actual, func(i, j int) bool {
		return GreaterWithNaNs(actual[i], actual[j])
	})

	compare.EqualsWithNans(t, expected, actual)
}
