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
	readers        []io.Reader
	readersIdx     int
	segmentReaders []m3db.SegmentReader
	idx            int
	len            int
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
	if !(it.idx+1 < it.len) {
		return false
	}
	it.idx++
	it.readers = it.readers[:0]
	it.readersIdx = 0
	return true
}

func (it *readerSliceOfSlicesIterator) Current() []io.Reader {
	if len(it.readers) != 0 {
		// Already computed
		return it.readers
	}

	group := it.segments[it.idx]
	if group.Merged != nil {
		it.setNextReader(group.Merged)
		return it.readers
	}

	for _, value := range group.Unmerged {
		it.setNextReader(value)
	}
	return it.readers
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

func (it *readerSliceOfSlicesIterator) setNextReader(value *rpc.Segment) {
	idx := it.readersIdx
	it.readersIdx++

	// Creating segment readers is pass by value so this is safe
	segment := m3db.Segment{Head: value.Head, Tail: value.Tail}
	if idx >= len(it.segmentReaders) {
		newReader := xio.NewSegmentReader(segment)
		it.segmentReaders = append(it.segmentReaders, newReader)
	} else {
		it.segmentReaders[idx].Reset(segment)
	}
	it.readers = append(it.readers, io.Reader(it.segmentReaders[idx]))
}

func (it *readerSliceOfSlicesIterator) Reset(segments []*rpc.Segments) {
	it.segments = segments
	it.readers = it.readers[:0]
	it.readersIdx = 0
	it.idx = -1
	it.len = len(segments)
	it.closed = false
}
