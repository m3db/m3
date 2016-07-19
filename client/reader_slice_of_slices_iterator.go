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

package client

import (
	"io"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
	xio "github.com/m3db/m3db/x/io"
)

type readerSliceOfSlicesIterator struct {
	segments       []*rpc.Segments
	segmentReaders []m3db.SegmentReader
	idx            int
	closed         bool
	pool           readerSliceOfSlicesIteratorPool
}

func newReaderSliceOfSlicesIterator(
	segments []*rpc.Segments,
	pool readerSliceOfSlicesIteratorPool,
) *readerSliceOfSlicesIterator {
	it := &readerSliceOfSlicesIterator{}
	it.Reset(segments)
	return it
}

func (it *readerSliceOfSlicesIterator) Next() bool {
	if !(it.idx+1 < len(it.segments)) {
		return false
	}
	it.idx++

	// Extend segment readers if not enough available
	currLen := it.CurrentLen()
	if len(it.segmentReaders) < currLen {
		diff := currLen - len(it.segmentReaders)
		for i := 0; i < diff; i++ {
			it.segmentReaders = append(it.segmentReaders, xio.NewSegmentReaderWithSegment(m3db.Segment{}))
		}
	}

	// Set the segment readers to reader from current segment pieces
	segment := it.segments[it.idx]
	if segment.Merged != nil {
		it.segmentReaders[0].Reset(m3db.Segment{
			Head: segment.Merged.Head,
			Tail: segment.Merged.Tail,
		})
	} else {
		for i := 0; i < currLen; i++ {
			it.segmentReaders[i].Reset(m3db.Segment{
				Head: segment.Unmerged[i].Head,
				Tail: segment.Unmerged[i].Tail,
			})
		}
	}

	return true
}

func (it *readerSliceOfSlicesIterator) CurrentLen() int {
	if it.segments[it.idx].Merged != nil {
		return 1
	}
	return len(it.segments[it.idx].Unmerged)
}

func (it *readerSliceOfSlicesIterator) CurrentAt(idx int) io.Reader {
	if idx >= it.CurrentLen() {
		return nil
	}
	return it.segmentReaders[idx]
}

func (it *readerSliceOfSlicesIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	if it.pool != nil {
		it.pool.Put(it)
	}
}

func (it *readerSliceOfSlicesIterator) Reset(segments []*rpc.Segments) {
	it.segments = segments
	it.idx = -1
	it.closed = false
}
