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

	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/checked"
)

type readerSliceOfSlicesIterator struct {
	segments       []*rpc.Segments
	segmentReaders []xio.SegmentReader
	idx            int
	closed         bool
	pool           *readerSliceOfSlicesIteratorPool
}

func newReaderSliceOfSlicesIterator(
	segments []*rpc.Segments,
	pool *readerSliceOfSlicesIteratorPool,
) *readerSliceOfSlicesIterator {
	it := &readerSliceOfSlicesIterator{pool: pool}
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
			seg := ts.NewSegment(nil, nil, ts.FinalizeNone)
			it.segmentReaders = append(it.segmentReaders, xio.NewSegmentReader(seg))
		}
	}

	// Set the segment readers to reader from current segment pieces
	segment := it.segments[it.idx]
	if segment.Merged != nil {
		it.resetReader(it.segmentReaders[0], segment.Merged)
	} else {
		for i := 0; i < currLen; i++ {
			it.resetReader(it.segmentReaders[i], segment.Unmerged[i])
		}
	}

	return true
}

func (it *readerSliceOfSlicesIterator) resetReader(
	r xio.SegmentReader,
	seg *rpc.Segment,
) {
	rseg, err := r.Segment()
	if err != nil {
		r.Reset(ts.Segment{})
		return
	}

	var (
		head = rseg.Head
		tail = rseg.Tail
	)
	if head == nil {
		head = checked.NewBytes(seg.Head, nil)
		head.IncRef()
	} else {
		head.Reset(seg.Head)
	}
	if tail == nil {
		tail = checked.NewBytes(seg.Tail, nil)
		tail.IncRef()
	} else {
		tail.Reset(seg.Tail)
	}
	r.Reset(ts.NewSegment(head, tail, ts.FinalizeNone))
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
	// Release any refs to segments
	it.segments = nil
	// Release any refs to segment byte slices
	for i := range it.segmentReaders {
		seg, err := it.segmentReaders[i].Segment()
		if err != nil {
			continue
		}
		if seg.Head != nil {
			seg.Head.Reset(nil)
		}
		if seg.Tail != nil {
			seg.Tail.Reset(nil)
		}
	}
	if pool := it.pool; pool != nil {
		pool.Put(it)
	}
}

func (it *readerSliceOfSlicesIterator) Reset(segments []*rpc.Segments) {
	it.segments = segments
	it.idx = -1
	it.closed = false
}
