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

var (
	graphitePathNodePrefixBytes = []byte(doc.GraphitePathNodePrefix)
	graphitePathLeafPrefixBytes = []byte(doc.GraphitePathLeafPrefix)
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
	maybeUnorderedFields        []shardUniqueFields
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
			for i := range slice.uniqueFields {
				if slice.uniqueFields[i].opts.graphitePathNode || slice.uniqueFields[i].opts.graphitePathLeaf {
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
	b.current = b.backingSlices.data[iOuter].uniqueFields[iInner]
	return true
}

func (b *orderedFieldsPostingsListIter) Reset() {
	backingSlices := b.backingSlices
	*b = orderedFieldsPostingsListIter{}
	b.currentIdx = -1
	b.backingSlices = backingSlices
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
	data                []shardUniqueFields
	length              int
	parallelSort        bool
	currentFieldBuffer1 keyBuffer
	currentFieldBuffer2 keyBuffer
}

func newSortableSliceOfSliceOfUniqueFieldsAsc(
	data []shardUniqueFields,
	parallelSort bool,
) *sortableSliceOfSliceOfUniqueFieldsAsc {
	length := 0
	for _, elem := range data {
		length += len(elem.uniqueFields)
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
	eitherGraphitePathNodeOrLeaf := iField.opts.graphitePathNode ||
		iField.opts.graphitePathLeaf ||
		jField.opts.graphitePathNode ||
		jField.opts.graphitePathLeaf
	if !eitherGraphitePathNodeOrLeaf {
		// No complexity, just compare the bytes.
		return bytes.Compare(iField.field, jField.field) < 0
	}

	if iField.opts.graphitePathLeaf && jField.opts.graphitePathNode {
		// NB(rob): If we have a leaf and a node, the leaf appears first since
		// it's alphanumerically appears before the node prefix.
		// See doc.GraphitePathNodePrefix and doc.GraphitePathLeafPrefix for
		// reference.
		return true
	}

	if iField.opts.graphitePathNode && jField.opts.graphitePathLeaf {
		// NB(rob): If we have a leaf and a node, the leaf appears first since
		// it's alphanumerically appears before the node prefix.
		// See doc.GraphitePathNodePrefix and doc.GraphitePathLeafPrefix for
		// reference.
		return false
	}

	if (iField.opts.graphitePathLeaf && jField.opts.graphitePathLeaf) ||
		(iField.opts.graphitePathNode && jField.opts.graphitePathNode) {
		// NB(rob): If we have a leaf+leaf or node+node then can just compare the
		// values to determine order.
		return bytes.Compare(iField.field, jField.field) < 0
	}

	// At this point on one side is either a leaf or a node and on the other
	// side it must be NEITHER a leaf or a node, so we compare the value on the
	// side that is not a leaf or node with the corresponding prefix of the type
	// on the side that does have a leaf or node.
	iFieldBytes := iField.field
	if iField.opts.graphitePathNode {
		iFieldBytes = graphitePathNodePrefixBytes
	} else if iField.opts.graphitePathLeaf {
		iFieldBytes = graphitePathLeafPrefixBytes
	}

	jFieldBytes := jField.field
	if jField.opts.graphitePathNode {
		jFieldBytes = graphitePathNodePrefixBytes
	} else if jField.opts.graphitePathLeaf {
		jFieldBytes = graphitePathLeafPrefixBytes
	}

	return bytes.Compare(iFieldBytes, jFieldBytes) < 0
}

func (s *sortableSliceOfSliceOfUniqueFieldsAsc) Swap(i, j int) {
	iOuter, iInner := s.getIndices(i)
	jOuter, jInner := s.getIndices(j)
	s.data[iOuter].uniqueFields[iInner], s.data[jOuter].uniqueFields[jInner] = s.data[jOuter].uniqueFields[jInner], s.data[iOuter].uniqueFields[iInner]
}

func (s *sortableSliceOfSliceOfUniqueFieldsAsc) elemAt(idx int) uniqueField {
	outer, inner := s.getIndices(idx)
	elem := s.data[outer].uniqueFields[inner]
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
	for idx >= len(s.data[currentSliceIdx].uniqueFields) {
		idx -= len(s.data[currentSliceIdx].uniqueFields)
		currentSliceIdx++
	}
	return currentSliceIdx, idx
}
