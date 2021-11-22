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
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"
)

type testValue struct {
	value      float64
	t          xtime.UnixNano
	unit       xtime.Unit
	annotation ts.Annotation
}

type testIterator struct {
	values  []testValue
	idx     int
	closed  bool
	err     error
	onNext  func(oldIdx, newIdx int)
	onReset func(r xio.Reader64, descr namespace.SchemaDescr)
}

func newTestIterator(values []testValue) ReaderIterator {
	return &testIterator{values: values, idx: -1}
}

func (it *testIterator) Next() bool {
	if it.onNext != nil {
		it.onNext(it.idx, it.idx+1)
	}
	if it.Err() != nil {
		return false
	}
	if it.idx+1 >= len(it.values) {
		return false
	}
	it.idx++
	return true
}

func (it *testIterator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	idx := it.idx
	if idx == -1 {
		idx = 0
	}
	v := it.values[idx]
	dp := ts.Datapoint{TimestampNanos: v.t, Value: v.value}
	return dp, v.unit, v.annotation
}

func (it *testIterator) Err() error {
	return it.err
}

func (it *testIterator) Close() {
	it.closed = true
}

func (it *testIterator) Reset(r xio.Reader64, descr namespace.SchemaDescr) {
	it.onReset(r, descr)
}

func (it *testIterator) ResetSliceOfSlices(readers xio.ReaderSliceOfSlicesIterator, descr namespace.SchemaDescr) {
	l, _, _ := readers.CurrentReaders()
	for i := 0; i < l; i++ {
		r := readers.CurrentReaderAt(i)
		it.onReset(r, descr)
	}
}

type testMultiIterator struct {
	values  []testValue
	idx     int
	closed  bool
	err     error
	onNext  func(oldIdx, newIdx int)
	onReset func(r xio.Reader64)
}

func newTestMultiIterator(values []testValue, err error) MultiReaderIterator {
	return &testMultiIterator{values: values, idx: -1, err: err}
}

func (it *testMultiIterator) Next() bool {
	if it.onNext != nil {
		it.onNext(it.idx, it.idx+1)
	}
	if it.Err() != nil {
		return false
	}
	if it.idx+1 >= len(it.values) {
		return false
	}
	it.idx++
	return true
}

func (it *testMultiIterator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	idx := it.idx
	if idx == -1 {
		idx = 0
	}
	v := it.values[idx]
	dp := ts.Datapoint{TimestampNanos: v.t, Value: v.value}
	return dp, v.unit, v.annotation
}

func (it *testMultiIterator) Err() error {
	return it.err
}

func (it *testMultiIterator) Close() {
	it.closed = true
}

func (it *testMultiIterator) Reset(r []xio.SegmentReader, _ xtime.UnixNano,
	_ time.Duration, _ namespace.SchemaDescr) {
	for _, reader := range r {
		it.onReset(reader)
	}
}

func (it *testMultiIterator) ResetSliceOfSlices(
	readers xio.ReaderSliceOfSlicesIterator, _ namespace.SchemaDescr) {
	l, _, _ := readers.CurrentReaders()
	for i := 0; i < l; i++ {
		r := readers.CurrentReaderAt(i)
		it.onReset(r)
	}
}

func (it *testMultiIterator) Schema() namespace.SchemaDescr {
	return nil
}

func (it *testMultiIterator) Readers() xio.ReaderSliceOfSlicesIterator {
	return nil
}

type testReaderSliceOfSlicesIterator struct {
	blocks [][]xio.BlockReader
	idx    int
	closed bool
}

func newTestReaderSliceOfSlicesIterator(
	blocks [][]xio.BlockReader,
) xio.ReaderSliceOfSlicesIterator {
	return &testReaderSliceOfSlicesIterator{blocks: blocks, idx: -1}
}

func (it *testReaderSliceOfSlicesIterator) Next() bool {
	if !(it.idx+1 < len(it.blocks)) {
		return false
	}
	it.idx++
	return true
}

func (it *testReaderSliceOfSlicesIterator) CurrentReaders() (int, xtime.UnixNano, time.Duration) {
	return len(it.blocks[it.arrayIdx()]), 0, 0
}

func (it *testReaderSliceOfSlicesIterator) CurrentReaderAt(idx int) xio.BlockReader {
	return it.blocks[it.arrayIdx()][idx]
}

func (it *testReaderSliceOfSlicesIterator) Close() {
	it.closed = true
}

func (it *testReaderSliceOfSlicesIterator) Size() (int, error) {
	return 0, nil
}

func (it *testReaderSliceOfSlicesIterator) RewindToIndex(idx int) {
	it.idx = idx
}

func (it *testReaderSliceOfSlicesIterator) Index() int {
	return it.idx
}

func (it *testReaderSliceOfSlicesIterator) arrayIdx() int {
	idx := it.idx
	if idx == -1 {
		idx = 0
	}
	return idx
}

type testNoopReader struct {
	n byte // return for "n", also required so that each struct construction has its address
}

func (r *testNoopReader) Read64() (word uint64, n byte, err error) { return 0, r.n, nil }
func (r *testNoopReader) Peek64() (word uint64, n byte, err error) { return 0, r.n, nil }
func (r *testNoopReader) Segment() (ts.Segment, error)             { return ts.Segment{}, nil }
func (r *testNoopReader) Reset(ts.Segment)                         {}
func (r *testNoopReader) Finalize()                                {}
func (r *testNoopReader) Clone(
	_ pool.CheckedBytesPool,
) (xio.SegmentReader, error) {
	return r, nil
}
