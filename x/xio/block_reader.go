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

package xio

import (
	"time"

	"github.com/m3db/m3db/ts"
)

type blockReader struct {
	reader SegmentReader
	start  time.Time
	end    time.Time
}

// NewBlockReader creates a new segment reader along with a specified segment.
func NewBlockReader(reader SegmentReader, start, end time.Time) BlockReader {
	return &blockReader{reader: reader, start: start, end: end}
}

func (r *blockReader) Clone() Reader {
	reader := r.reader.Clone()
	if sr, ok := reader.(SegmentReader); ok {
		return NewBlockReader(sr, r.start, r.end)
	}
	panic("segmentreader cloned to invalid type")
}

func (r *blockReader) Start() time.Time {
	return r.start
}

func (r *blockReader) End() time.Time {
	return r.end
}

func (r *blockReader) Read(b []byte) (int, error) {
	return r.reader.Read(b)
}

func (r *blockReader) Segment() (ts.Segment, error) {
	return r.reader.Segment()
}

func (r *blockReader) Reset(segment ts.Segment) {
	r.reader.Reset(segment)
}

func (r *blockReader) ResetWindowed(segment ts.Segment, start, end time.Time) {
	r.reader.Reset(segment)
	r.start = start
	r.end = end
}

func (r *blockReader) Finalize() {
	r.reader.Finalize()
}

func (r *blockReader) SegmentReader() (SegmentReader, error) {
	return r.reader, nil
}
