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

package m3db

import (
	"io"
)

// ReaderSliceReader implements the io reader interface backed by a slice of readers.
type ReaderSliceReader interface {
	io.Reader

	Readers() []io.Reader
}

// SegmentReader implements the io reader interface backed by a segment.
type SegmentReader interface {
	io.Reader

	// Segment gets the segment read by this reader.
	Segment() Segment

	// Reset resets the reader to read a new segment.
	Reset(segment Segment)

	// Close closes the reader and if pooled will return to the pool.
	Close()
}

// Segment represents a binary blob consisting of two byte slices.
type Segment struct {
	// Head is the head of the segment.
	Head []byte

	// Tail is the tail of the segment.
	Tail []byte
}
