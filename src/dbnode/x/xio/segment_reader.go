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
	"io"

	"github.com/m3db/m3/src/dbnode/ts"
)

type segmentReader struct {
	segment ts.Segment
	si      int
	pool    SegmentReaderPool
}

// NewSegmentReader creates a new segment reader along with a specified segment.
func NewSegmentReader(segment ts.Segment) SegmentReader {
	return &segmentReader{segment: segment}
}

func (sr *segmentReader) Clone() (SegmentReader, error) {
	return NewSegmentReader(sr.segment), nil
}

func (sr *segmentReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	var head, tail []byte
	if b := sr.segment.Head; b != nil {
		head = b.Bytes()
	}
	if b := sr.segment.Tail; b != nil {
		tail = b.Bytes()
	}
	nh, nt := len(head), len(tail)
	if sr.si >= nh+nt {
		return 0, io.EOF
	}
	n := 0
	if sr.si < nh {
		nRead := copy(b, head[sr.si:])
		sr.si += nRead
		n += nRead
		if n == len(b) {
			return n, nil
		}
	}
	if sr.si < nh+nt {
		nRead := copy(b[n:], tail[sr.si-nh:])
		sr.si += nRead
		n += nRead
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (sr *segmentReader) Segment() (ts.Segment, error) {
	return sr.segment, nil
}

func (sr *segmentReader) Reset(segment ts.Segment) {
	sr.segment = segment
	sr.si = 0
}

func (sr *segmentReader) Finalize() {
	// Finalize the segment
	sr.segment.Finalize()

	if pool := sr.pool; pool != nil {
		pool.Put(sr)
	}
}
