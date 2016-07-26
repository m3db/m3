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
	"errors"
	"io"

	"github.com/m3db/m3db/interfaces/m3db"
)

var (
	errUnexpectedReaderType = errors.New("unexpected reader type")
)

type sliceOfSliceReader struct {
	s  [][]byte // raw data
	si int      // current slice index
	bi int      // current byte index
}

// NewSliceOfSliceReader creates a new io reader that wraps a slice of slice inside itself.
func NewSliceOfSliceReader(rawBytes [][]byte) io.Reader {
	return &sliceOfSliceReader{s: rawBytes}
}

func (r *sliceOfSliceReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if r.si >= len(r.s) {
		return 0, io.EOF
	}
	n := 0
	for r.si < len(r.s) && n < len(b) {
		nRead := copy(b[n:], r.s[r.si][r.bi:])
		n += nRead
		r.bi += nRead
		if r.bi >= len(r.s[r.si]) {
			r.si++
			r.bi = 0
		}
	}
	return n, nil
}

type readerSliceReader struct {
	s  []io.Reader // reader list
	si int         // slice index
}

// NewReaderSliceReader creates a new ReaderSliceReader instance.
func NewReaderSliceReader(r []io.Reader) m3db.ReaderSliceReader {
	return &readerSliceReader{s: r}
}

func (r *readerSliceReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if r.si >= len(r.s) {
		return 0, io.EOF
	}
	n := 0
	for r.si < len(r.s) && n < len(b) {
		if r.s[r.si] == nil {
			r.si++
			continue
		}
		nRead, _ := r.s[r.si].Read(b[n:])
		n += nRead
		if n < len(b) {
			r.si++
		}
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (r *readerSliceReader) Readers() []io.Reader {
	return r.s
}

type segmentReader struct {
	segment m3db.Segment
	si      int
	pool    m3db.SegmentReaderPool
}

// NewSegmentReader creates a new segment reader along with a specified segment.
func NewSegmentReader(segment m3db.Segment) m3db.SegmentReader {
	return &segmentReader{segment: segment}
}

// NewPooledSegmentReader creates a new pooled segment reader.
func NewPooledSegmentReader(segment m3db.Segment, pool m3db.SegmentReaderPool) m3db.SegmentReader {
	return &segmentReader{segment: segment, pool: pool}
}

func (sr *segmentReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	head, tail := sr.segment.Head, sr.segment.Tail
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

func (sr *segmentReader) Segment() m3db.Segment {
	return sr.segment
}

func (sr *segmentReader) Reset(segment m3db.Segment) {
	sr.segment = segment
	sr.si = 0
}

func (sr *segmentReader) Close() {
	if sr.pool != nil {
		sr.pool.Put(sr)
	}
}
