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
	"github.com/m3db/m3/src/x/pool"
)

type segmentReader struct {
	segment ts.Segment
	headSet bool
	tailSet bool
	pool    SegmentReaderPool
}

// NewSegmentReader creates a new segment reader along with a specified segment.
func NewSegmentReader(segment ts.Segment) SegmentReader {
	return &segmentReader{segment: segment}
}

func (sr *segmentReader) Clone(
	pool pool.CheckedBytesPool,
) (SegmentReader, error) {
	return NewSegmentReader(sr.segment.Clone(pool)), nil
}

func (sr *segmentReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	if sr.headSet && sr.tailSet {
		return 0, io.EOF
	}

	bytesRead := 0
	if !sr.headSet {
		sr.headSet = true
		if h := sr.segment.Head; h != nil {
			bytesRead = copy(b, h.Bytes())

			// Buffer full, therefore eagerly return.
			if bytesRead == len(b) {
				return bytesRead, nil
			}
		}
	}

	sr.tailSet = true
	if t := sr.segment.Tail; t != nil {
		// Copy into the buffer at the position left over from head if needed.
		bytesRead += copy(b[bytesRead:], t.Bytes())
		return bytesRead, nil
	}

	return 0, io.EOF
}

func (sr *segmentReader) Segment() (ts.Segment, error) {
	return sr.segment, nil
}

func (sr *segmentReader) Reset(segment ts.Segment) {
	sr.segment = segment
	sr.headSet = false
	sr.tailSet = false
}

func (sr *segmentReader) Finalize() {
	// Finalize the segment
	sr.segment.Finalize()
	sr.headSet = false
	sr.tailSet = false

	if pool := sr.pool; pool != nil {
		pool.Put(sr)
	}
}
