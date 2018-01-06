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

package msgpack

import (
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
// so that the IndexSummaryToken struct can be kept small, as well as
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
	return indexOffset, nil
}

// NewIndexSummaryToken returns a new IndexSummaryToken
func NewIndexSummaryToken(idStartOffset, idLength uint32) IndexSummaryToken {
	return IndexSummaryToken{
		idStartOffset: idStartOffset,
		idLength:      idLength,
	}
}
