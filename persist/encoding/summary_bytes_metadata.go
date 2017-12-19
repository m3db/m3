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

// IndexSummaryToken can be used, along with the summaries file buffer, to
// quickly retrieve the ID or index file offset of an index summary entry.
// It's structured such that the ID and Index file offset can be retrieved
// quickly, while only requiring 64-bits of memory per entry.
type IndexSummaryToken struct {
	// The offset into the underlying byte array at which the bytes for the ID begin
	idStartOffset uint32
	// The length of the bytes for the ID
	idLength uint32
}

// ID returns the ID that the metadata corresponds to
func (m IndexSummaryToken) ID(buf []byte) []byte {
	idStart := m.idStartOffset
	idEnd := idStart + m.idLength
	return buf[idStart:idEnd]
}

// IndexOffset returns the offset in the index file for the series that the
// metadata corresponds to. The buf, stream, and decoder arguments are passed in
// so that the IndexSummaryIDBytesMetadata struct can be kept small, as well as
// so that the caller can have control over re-use and allocations.
func (m IndexSummaryToken) IndexOffset(
	buf []byte, stream DecoderStream, msgpackDecoder *msgpack.Decoder) (int64, error) {
	idStart := m.idStartOffset
	idEnd := idStart + m.idLength
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

// NewIndexSummaryToken returns a new IndexSummaryToken
func NewIndexSummaryToken(idStartOffset, idLength uint32) IndexSummaryToken {
	return IndexSummaryToken{
		idStartOffset: idStartOffset,
		idLength:      idLength,
	}
}

// IndexSummaryTokenSortableCollection is a sortable container of IndexSummaryToken
type IndexSummaryTokenSortableCollection struct {
	slice []IndexSummaryToken
	// Store the underlying buf that the token references so that we can perform
	// comparisons in the Less() method
	buf []byte
}

// Len returns the number of elements in the collection
func (s *IndexSummaryTokenSortableCollection) Len() int {
	return len(s.slice)
}

// Less reports whether the element
func (s *IndexSummaryTokenSortableCollection) Less(i, j int) bool {
	iBytes := s.slice[i].ID(s.buf)
	jBytes := s.slice[j].ID(s.buf)
	return bytes.Compare(iBytes, jBytes) <= 0
}

// Swap swaps the elements with indexes i and j
func (s *IndexSummaryTokenSortableCollection) Swap(i, j int) {
	temp := s.slice[i]
	s.slice[i] = s.slice[j]
	s.slice[j] = temp
}

// Sorted returns a sorted slice of IndexSummaryIDBytesMetadata
func (s *IndexSummaryTokenSortableCollection) Sorted() []IndexSummaryToken {
	sort.Sort(s)
	return s.slice
}

// NewIndexSummaryIDBytesMetadataSortableCollection creates a new
// IndexSummaryIDBytesMetadataSortableCollection
func NewIndexSummaryIDBytesMetadataSortableCollection(
	slice []IndexSummaryToken,
	buf []byte,
) *IndexSummaryTokenSortableCollection {
	return &IndexSummaryTokenSortableCollection{
		slice: slice,
		buf:   buf,
	}
}
