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

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"

	"github.com/twotwotwo/sorts"
)

type uniqueField struct {
	field        []byte
	opts         indexJobEntryOptions
	postingsList postings.List
}

// orderedFieldsPostingsListIter is a new ordered fields/postings list iterator.
type orderedFieldsPostingsListIter struct {
	err                error
	done               bool
	currentFieldBuffer keyBuffer

	currentIdx    int
	current       uniqueField
	backingSlices *sortableSliceOfSliceOfUniqueFieldsAsc
}

var _ segment.FieldsPostingsListIterator = &orderedFieldsPostingsListIter{}

type newOrderedFieldsPostingsListIterOptions struct {
	maybeUnorderedFields        [][]uniqueField
	graphitePathIndexingEnabled bool
}

// newOrderedFieldsPostingsListIter sorts a slice of slices of unique fields and then
// returns an iterator over them.
func newOrderedFieldsPostingsListIter(
	opts newOrderedFieldsPostingsListIterOptions,
) *orderedFieldsPostingsListIter {
	parallelSort := true
	if opts.graphitePathIndexingEnabled {
	CheckCanParallelSortLoop:
		for _, slice := range opts.maybeUnorderedFields {
			for i := range slice {
				if slice[i].opts.graphitePathNode || slice[i].opts.graphitePathLeaf {
					// NB(rob): We can't do parallel sorting if there's graphite
					// path/nodes since they require a buffer to be used that
					// can't be cached if multiple goroutines are accessing the
					// sortable fields structure.
					parallelSort = false
					break CheckCanParallelSortLoop
				}
			}
		}
	}
	sortable := newSortableSliceOfSliceOfUniqueFieldsAsc(opts.maybeUnorderedFields, parallelSort)
	if parallelSort {
		// NB(rob): See SetSortConcurrency why this RLock is required.
		sortConcurrencyLock.RLock()
		sorts.ByBytes(sortable)
		sortConcurrencyLock.RUnlock()
	} else {
		sort.Sort(sortable)
	}
	return &orderedFieldsPostingsListIter{
		currentIdx:    -1,
		backingSlices: sortable,
	}
}

// Next returns true if there is a next result.
func (b *orderedFieldsPostingsListIter) Next() bool {
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

type keyBuffer struct {
	bytes []byte
}

func fieldBytes(elem uniqueField, buff *keyBuffer) []byte {
	field := elem.field
	switch {
	case elem.opts.graphitePathNode:
		buff.bytes = append(buff.bytes[:0], doc.GraphitePathNodePrefix...)
		buff.bytes = append(buff.bytes, field...)
		field = buff.bytes
	case elem.opts.graphitePathLeaf:
		buff.bytes = append(buff.bytes[:0], doc.GraphitePathLeafPrefix...)
		buff.bytes = append(buff.bytes, field...)
		field = buff.bytes
	}
	return field
}

// Current returns the current entry.
func (b *orderedFieldsPostingsListIter) Current() ([]byte, postings.List) {
	return fieldBytes(b.current, &b.currentFieldBuffer), b.current.postingsList
}

// Err returns an error if an error occurred iterating.
func (b *orderedFieldsPostingsListIter) Err() error {
	return nil
}

// Len returns the length of the slice.
func (b *orderedFieldsPostingsListIter) Len() int {
	return b.backingSlices.Len()
}

// Close releases resources.
func (b *orderedFieldsPostingsListIter) Close() error {
	b.current = uniqueField{}
	return nil
}

type sortableSliceOfSliceOfUniqueFieldsAsc struct {
	data                [][]uniqueField
	length              int
	parallelSort        bool
	currentFieldBuffer1 keyBuffer
	currentFieldBuffer2 keyBuffer
}

func newSortableSliceOfSliceOfUniqueFieldsAsc(
	data [][]uniqueField,
	parallelSort bool,
) *sortableSliceOfSliceOfUniqueFieldsAsc {
	length := 0
	for _, innerSlice := range data {
		length += len(innerSlice)
	}
	return &sortableSliceOfSliceOfUniqueFieldsAsc{
		data:         data,
		length:       length,
		parallelSort: parallelSort,
	}
}

func (s *sortableSliceOfSliceOfUniqueFieldsAsc) Len() int {
	return s.length
}

func (s *sortableSliceOfSliceOfUniqueFieldsAsc) Less(i, j int) bool {
	iField := s.elemAt(i)
	jField := s.elemAt(j)
	iFieldBytes := fieldBytes(iField, &s.currentFieldBuffer1)
	jFieldBytes := fieldBytes(jField, &s.currentFieldBuffer2)
	return bytes.Compare(iFieldBytes, jFieldBytes) < 0
}

func (s *sortableSliceOfSliceOfUniqueFieldsAsc) Swap(i, j int) {
	iOuter, iInner := s.getIndices(i)
	jOuter, jInner := s.getIndices(j)
	s.data[iOuter][iInner], s.data[jOuter][jInner] = s.data[jOuter][jInner], s.data[iOuter][iInner]
}

func (s *sortableSliceOfSliceOfUniqueFieldsAsc) elemAt(idx int) uniqueField {
	outer, inner := s.getIndices(idx)
	elem := s.data[outer][inner]
	if s.parallelSort && (elem.opts.graphitePathNode || elem.opts.graphitePathLeaf) {
		// NB(rob): This should never be reached if this data entry is a
		// graphite node or leaf since we fall back to single core sorting if
		// that is the case.
		panic("unexpected parallel sort with graphite fields present")
	}
	return elem
}

func (s *sortableSliceOfSliceOfUniqueFieldsAsc) Key(i int) []byte {
	// NB(rob): This method only called by parallel sort so safe that we do
	// not call "fieldBytes" which will correctly prefix the field with
	// the graphite node or leaf prefix. This is checked inside "elemAt".
	return s.elemAt(i).field
}

func (s *sortableSliceOfSliceOfUniqueFieldsAsc) getIndices(idx int) (int, int) {
	currentSliceIdx := 0
	for idx >= len(s.data[currentSliceIdx]) {
		idx -= len(s.data[currentSliceIdx])
		currentSliceIdx++
	}
	return currentSliceIdx, idx
}
