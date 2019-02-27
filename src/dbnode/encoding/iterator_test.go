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
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

type testValue struct {
	value      float64
	t          time.Time
	unit       xtime.Unit
	annotation []byte
}

type testIterator struct {
	values  []testValue
	idx     int
	closed  bool
	err     error
	onNext  func(oldIdx, newIdx int)
	onReset func(r io.Reader)
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
	dp := ts.Datapoint{Timestamp: v.t, Value: v.value}
	return dp, v.unit, ts.Annotation(v.annotation)
}

func (it *testIterator) Err() error {
	return it.err
}

func (it *testIterator) Close() {
	it.closed = true
}

func (it *testIterator) Reset(r io.Reader) {
	it.onReset(r)
}

func (it *testIterator) ResetSliceOfSlices(readers xio.ReaderSliceOfSlicesIterator) {
	l, _, _ := readers.CurrentReaders()
	for i := 0; i < l; i++ {
		r := readers.CurrentReaderAt(i)
		it.onReset(r)
	}
}

type testMultiIterator struct {
	values  []testValue
	idx     int
	closed  bool
	err     error
	onNext  func(oldIdx, newIdx int)
	onReset func(r io.Reader)
}

func newTestMultiIterator(values []testValue) MultiReaderIterator {
	return &testMultiIterator{values: values, idx: -1}
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
	dp := ts.Datapoint{Timestamp: v.t, Value: v.value}
	return dp, v.unit, ts.Annotation(v.annotation)
}

func (it *testMultiIterator) Err() error {
	return it.err
}

func (it *testMultiIterator) Close() {
	it.closed = true
}

func (it *testMultiIterator) Reset(r []xio.SegmentReader, _ time.Time, _ time.Duration) {
	for _, reader := range r {
		it.onReset(reader)
	}
}

func (it *testMultiIterator) ResetSliceOfSlices(readers xio.ReaderSliceOfSlicesIterator) {
	l, _, _ := readers.CurrentReaders()
	for i := 0; i < l; i++ {
		r := readers.CurrentReaderAt(i)
		it.onReset(r)
	}
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

func (it *testReaderSliceOfSlicesIterator) CurrentReaders() (int, time.Time, time.Duration) {
	return len(it.blocks[it.arrayIdx()]), time.Time{}, 0
}

func (it *testReaderSliceOfSlicesIterator) CurrentReaderAt(idx int) xio.BlockReader {
	return it.blocks[it.arrayIdx()][idx]
}

func (it *testReaderSliceOfSlicesIterator) Close() {
	it.closed = true
}

func (it *testReaderSliceOfSlicesIterator) arrayIdx() int {
	idx := it.idx
	if idx == -1 {
		idx = 0
	}
	return idx
}

type testNoopReader struct {
	n int // return for "n", also required so that each struct construction has its address
}

func (r *testNoopReader) Read(p []byte) (int, error)   { return r.n, nil }
func (r *testNoopReader) Segment() (ts.Segment, error) { return ts.Segment{}, nil }
func (r *testNoopReader) Reset(ts.Segment)             {}
func (r *testNoopReader) Finalize()                    {}
func (r *testNoopReader) Clone(
	_ pool.CheckedBytesPool,
) (xio.SegmentReader, error) {
	return r, nil
}
