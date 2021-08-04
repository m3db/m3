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
	"encoding/binary"
	"io"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/pool"
)

type segmentReader struct {
	segment  ts.Segment
	lazyHead []byte
	lazyTail []byte
	si       int
	pool     SegmentReaderPool
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

func (sr *segmentReader) Read64() (word uint64, n byte, err error) {
	sr.lazyInit()

	var (
		headLen = len(sr.lazyHead)
		res     uint64
		bytes   byte
	)

	if sr.si+8 <= headLen {
		// NB: this compiles to a single 64 bit load followed by
		// a BSWAPQ on amd64 gc 1.13 (https://godbolt.org/z/oTK1jx).
		res = binary.BigEndian.Uint64(sr.lazyHead[sr.si:])
		sr.si += 8
		return res, 8, nil
	}

	headTailLen := headLen + len(sr.lazyTail)

	if sr.si < headLen {
		for ; sr.si < headLen; sr.si++ {
			res = (res << 8) | uint64(sr.lazyHead[sr.si])
			bytes++
		}
		for ; sr.si < headTailLen && bytes < 8; sr.si++ {
			res = (res << 8) | uint64(sr.lazyTail[sr.si-headLen])
			bytes++
		}
		return res << (64 - 8*bytes), bytes, nil
	}

	if sr.si+8 <= headTailLen {
		// NB: this compiles to a single 64 bit load followed by
		// a BSWAPQ on amd64 gc 1.13 (https://godbolt.org/z/oTK1jx).
		res = binary.BigEndian.Uint64(sr.lazyTail[sr.si-headLen:])
		sr.si += 8
		return res, 8, nil
	}

	if sr.si >= headTailLen {
		return 0, 0, io.EOF
	}

	for ; sr.si < headTailLen; sr.si++ {
		res = (res << 8) | uint64(sr.lazyTail[sr.si-headLen])
		bytes++
	}
	return res << (64 - 8*bytes), bytes, nil
}

func (sr *segmentReader) Peek64() (word uint64, n byte, err error) {
	sr.lazyInit()

	var (
		headLen = len(sr.lazyHead)
		i       = sr.si
		res     uint64
		bytes   byte
	)

	if i+8 <= headLen {
		// NB: this compiles to a single 64 bit load followed by
		// a BSWAPQ on amd64 gc 1.13 (https://godbolt.org/z/oTK1jx).
		res = binary.BigEndian.Uint64(sr.lazyHead[i:])
		return res, 8, nil
	}

	headTailLen := headLen + len(sr.lazyTail)

	if i < headLen {
		for ; i < headLen; i++ {
			res = (res << 8) | uint64(sr.lazyHead[i])
			bytes++
		}
		for ; i < headTailLen && bytes < 8; i++ {
			res = (res << 8) | uint64(sr.lazyTail[i-headLen])
			bytes++
		}
		return res << (64 - 8*bytes), bytes, nil
	}

	if i+8 <= headTailLen {
		// NB: this compiles to a single 64 bit load followed by
		// a BSWAPQ on amd64 gc 1.13 (https://godbolt.org/z/oTK1jx).
		res = binary.BigEndian.Uint64(sr.lazyTail[i-headLen:])
		return res, 8, nil
	}

	if i >= headTailLen {
		return 0, 0, io.EOF
	}

	for ; i < headTailLen; i++ {
		res = (res << 8) | uint64(sr.lazyTail[i-headLen])
		bytes++
	}
	return res << (64 - 8*bytes), bytes, nil
}

func (sr *segmentReader) Segment() (ts.Segment, error) {
	return sr.segment, nil
}

func (sr *segmentReader) Reset(segment ts.Segment) {
	sr.segment = segment
	sr.si = 0
	sr.lazyHead = sr.lazyHead[:0]
	sr.lazyTail = sr.lazyTail[:0]
}

func (sr *segmentReader) Finalize() {
	sr.segment.Finalize()
	sr.lazyHead = nil
	sr.lazyTail = nil

	if pool := sr.pool; pool != nil {
		pool.Put(sr)
	}
}

func (sr *segmentReader) lazyInit() {
	if b := sr.segment.Head; b != nil && len(sr.lazyHead) == 0 {
		sr.lazyHead = b.Bytes()
	}
	if b := sr.segment.Tail; b != nil && len(sr.lazyTail) == 0 {
		sr.lazyTail = b.Bytes()
	}
}
