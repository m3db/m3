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

package encoding

import (
	"io"

	"github.com/m3db/m3db/interfaces/m3db"
)

type readerSliceOfSlicesIterator struct {
	segments [][]m3db.SegmentReader
	idx      int
	len      int
	closed   bool
}

// NewReaderSliceOfSlicesFromSegmentReadersIterator creates a new reader slice of slices iterator
func NewReaderSliceOfSlicesFromSegmentReadersIterator(
	segments [][]m3db.SegmentReader,
) m3db.ReaderSliceOfSlicesFromSegmentReadersIterator {
	it := &readerSliceOfSlicesIterator{}
	it.Reset(segments)
	return it
}

func (it *readerSliceOfSlicesIterator) Next() bool {
	if !(it.idx+1 < it.len) {
		return false
	}
	it.idx++
	return true
}

func (it *readerSliceOfSlicesIterator) CurrentLen() int {
	return len(it.segments[it.arrayIdx()])
}

func (it *readerSliceOfSlicesIterator) CurrentAt(idx int) io.Reader {
	return it.segments[it.arrayIdx()][idx]
}

func (it *readerSliceOfSlicesIterator) Reset(segments [][]m3db.SegmentReader) {
	it.segments = segments
	it.idx = -1
	it.len = len(segments)
	it.closed = false
}

func (it *readerSliceOfSlicesIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
}

func (it *readerSliceOfSlicesIterator) arrayIdx() int {
	idx := it.idx
	if idx == -1 {
		idx = 0
	}
	return idx
}
