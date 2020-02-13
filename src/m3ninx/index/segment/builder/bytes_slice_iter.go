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

package builder

import (
	"bytes"
	"sort"

	"github.com/m3db/m3/src/m3ninx/index/segment"

	"github.com/twotwotwo/sorts"
)

// OrderedBytesSliceIter is a new ordered bytes slice iterator.
type OrderedBytesSliceIter struct {
	err  error
	done bool

	currentIdx    int
	current       []byte
	backingSlices *sortableSliceOfSliceOfByteSlices
}

var _ segment.FieldsIterator = &OrderedBytesSliceIter{}

// NewOrderedBytesSliceIter sorts a slice of bytes and then
// returns an iterator over them.
func NewOrderedBytesSliceIter(
	maybeUnorderedSlices [][][]byte,
) *OrderedBytesSliceIter {
	sortable := &sortableSliceOfSliceOfByteSlices{data: maybeUnorderedSlices}
	sorts.ByBytes(sortable)
	return &OrderedBytesSliceIter{
		currentIdx:    -1,
		backingSlices: sortable,
	}
}

// Next returns true if there is a next result.
func (b *OrderedBytesSliceIter) Next() bool {
	if b.done || b.err != nil {
		return false
	}
	b.currentIdx++
	if b.currentIdx >= b.backingSlices.Len() {
		b.done = true
		return false
	}
	iOuter, iInner := b.backingSlices.getIndices(b.currentIdx)
	b.current = b.backingSlices.data[iOuter][iInner]
	return true
}

// Current returns the current entry.
func (b *OrderedBytesSliceIter) Current() []byte {
	return b.current
}

// Err returns an error if an error occurred iterating.
func (b *OrderedBytesSliceIter) Err() error {
	return nil
}

// Len returns the length of the slice.
func (b *OrderedBytesSliceIter) Len() int {
	return b.backingSlices.Len()
}

// Close releases resources.
func (b *OrderedBytesSliceIter) Close() error {
	b.current = nil
	return nil
}

type sortableSliceOfSliceOfByteSlices struct {
	data   [][][]byte
	length *int
}

func (s *sortableSliceOfSliceOfByteSlices) Len() int {
	if s.length != nil {
		return *s.length
	}

	totalLen := 0
	for _, innerSlice := range s.data {
		totalLen += len(innerSlice)
	}
	s.length = &totalLen

	return *s.length
}

func (s *sortableSliceOfSliceOfByteSlices) Less(i, j int) bool {
	iOuter, iInner := s.getIndices(i)
	jOuter, jInner := s.getIndices(j)
	return bytes.Compare(s.data[iOuter][iInner], s.data[jOuter][jInner]) < 0
}

func (s *sortableSliceOfSliceOfByteSlices) Swap(i, j int) {
	iOuter, iInner := s.getIndices(i)
	jOuter, jInner := s.getIndices(j)
	s.data[iOuter][iInner], s.data[jOuter][jInner] = s.data[jOuter][jInner], s.data[iOuter][iInner]
}

func (s *sortableSliceOfSliceOfByteSlices) Key(i int) []byte {
	iOuter, iInner := s.getIndices(i)
	return s.data[iOuter][iInner]
}

func (s *sortableSliceOfSliceOfByteSlices) getIndices(idx int) (int, int) {
	currentSliceIdx := 0
	for idx >= len(s.data[currentSliceIdx]) {
		idx -= len(s.data[currentSliceIdx])
		currentSliceIdx++
	}
	return currentSliceIdx, idx
}

func sortSliceOfByteSlices(b [][]byte) {
	sort.Slice(b, func(i, j int) bool {
		return bytes.Compare(b[i], b[j]) < 0
	})
}
