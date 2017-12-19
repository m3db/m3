// Copyright (c) 2017 Uber Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package encoding

import (
	"bytes"
	"sort"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// IndexSummaryIDBytesMetadata contains metadata about the bytes of the ID in
// the underlying byte stream in which the index summary was decoded
type IndexSummaryIDBytesMetadata struct {
	// The offset into the underlying byte array at which the bytes for the ID begin
	IDBytesStartOffset uint32
	// The length of the bytes for the ID
	IDBytesLength uint32
}

// ID returns the ID that the metadata corresponds to
func (m IndexSummaryIDBytesMetadata) ID(buf []byte) []byte {
	idStart := m.IDBytesStartOffset
	idEnd := idStart + m.IDBytesLength
	return buf[idStart:idEnd]
}

// IndexOffset returns the offset in the index file for the series that the
// metadata corresponds to. The buf, stream, and decoder arguments are passed in
// so that the IndexSummaryIDBytesMetadata struct can be kept small, as well as
// so that the caller can have control over re-use and allocations.
func (m IndexSummaryIDBytesMetadata) IndexOffset(
	buf []byte, stream DecoderStream, msgpackDecoder *msgpack.Decoder) (int64, error) {
	idStart := m.IDBytesStartOffset
	idEnd := idStart + m.IDBytesLength
	indexOffsetStart := int(idEnd)

	stream.Reset(buf[indexOffsetStart:])
	msgpackDecoder.Reset(stream)
	indexOffset, err := msgpackDecoder.DecodeInt64()
	// Should never happen, either something is really wrong with the code or
	// the file on disk was corrupted
	if err != nil {
		return -1, err
	}
	return int64(indexOffset), nil
}

// IndexSummaryIDBytesMetadataSortableCollection is a sortable container of IndexSummaryIDBytesMetadata
type IndexSummaryIDBytesMetadataSortableCollection struct {
	slice []IndexSummaryIDBytesMetadata
	// Store the underlying buf that the metadata points into so that we can
	// perform comparisons in the Less() method
	buf []byte
}

// Len returns the number of elements in the collection
func (s *IndexSummaryIDBytesMetadataSortableCollection) Len() int {
	return len(s.slice)
}

// Less reports whether the element
func (s *IndexSummaryIDBytesMetadataSortableCollection) Less(i, j int) bool {
	iBytes := s.slice[i].ID(s.buf)
	jBytes := s.slice[j].ID(s.buf)
	return bytes.Compare(iBytes, jBytes) <= 0
}

// Swap swaps the elements with indexes i and j
func (s *IndexSummaryIDBytesMetadataSortableCollection) Swap(i, j int) {
	temp := s.slice[i]
	s.slice[i] = s.slice[j]
	s.slice[j] = temp
}

// Element returns the element at index i
func (s *IndexSummaryIDBytesMetadataSortableCollection) Element(i uint) IndexSummaryIDBytesMetadata {
	return s.slice[i]
}

// Sorted returns a sorted slice of IndexSummaryIDBytesMetadata
func (s *IndexSummaryIDBytesMetadataSortableCollection) Sorted() []IndexSummaryIDBytesMetadata {
	sort.Sort(s)
	return s.slice
}

// NewIndexSummaryIDBytesMetadataSortableCollection creates a new
// IndexSummaryIDBytesMetadataSortableCollection
func NewIndexSummaryIDBytesMetadataSortableCollection(
	slice []IndexSummaryIDBytesMetadata,
	buf []byte,
) *IndexSummaryIDBytesMetadataSortableCollection {
	return &IndexSummaryIDBytesMetadataSortableCollection{
		slice: slice,
		buf:   buf,
	}
}
