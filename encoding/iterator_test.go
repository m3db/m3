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

	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/time"
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

type testReaderSliceOfSlicesIterator struct {
	readers [][]io.Reader
	idx     int
	len     int
	closed  bool
}

func newTestReaderSliceOfSlicesIterator(
	readers [][]io.Reader,
) xio.ReaderSliceOfSlicesIterator {
	return &testReaderSliceOfSlicesIterator{readers: readers, idx: -1}
}

func (it *testReaderSliceOfSlicesIterator) Next() bool {
	if !(it.idx+1 < len(it.readers)) {
		return false
	}
	it.idx++
	return true
}

func (it *testReaderSliceOfSlicesIterator) CurrentLen() int {
	return len(it.readers[it.arrayIdx()])
}

func (it *testReaderSliceOfSlicesIterator) CurrentAt(idx int) io.Reader {
	return it.readers[it.arrayIdx()][idx]
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

func (r *testNoopReader) Read(p []byte) (int, error) {
	return r.n, nil
}
