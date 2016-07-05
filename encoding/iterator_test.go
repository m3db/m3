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
	"testing"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	xtime "github.com/m3db/m3db/x/time"

	"github.com/stretchr/testify/assert"
)

type testValue struct {
	value      float64
	t          time.Time
	unit       xtime.Unit
	annotation []byte
}

type testIterator struct {
	values []testValue
	idx    int
	closed bool
	err    error
}

func newTestIterator(values []testValue) m3db.Iterator {
	return &testIterator{values: values, idx: -1}
}

func (it *testIterator) Next() bool {
	if it.idx+1 >= len(it.values) {
		return false
	}
	it.idx++
	return true
}

func (it *testIterator) Current() (m3db.Datapoint, xtime.Unit, m3db.Annotation) {
	idx := it.idx
	if idx == -1 {
		idx = 0
	}
	v := it.values[idx]
	return m3db.Datapoint{Timestamp: v.t, Value: v.value}, v.unit, m3db.Annotation(v.annotation)
}

func (it *testIterator) Err() error {
	return it.err
}

func (it *testIterator) Close() {
	it.closed = true
}

type testReaderValues struct {
	values []testValue
	reader io.Reader
}

type testSingleReaderIterator struct {
	testIterator
	values []testReaderValues
	idx    int
	t      *testing.T
}

func newTestSingleReaderIterator(
	t *testing.T,
	values []testReaderValues,
) m3db.SingleReaderIterator {
	return &testSingleReaderIterator{
		testIterator: *newTestIterator(nil).(*testIterator),
		values:       values,
		idx:          -1,
		t:            t,
	}
}

func (it *testSingleReaderIterator) Reset(reader io.Reader) {
	it.idx++
	assert.Equal(it.t, it.values[it.idx].reader, reader)
	it.testIterator = *newTestIterator(nil).(*testIterator)
	it.testIterator.values = it.values[it.idx].values
}

type testReadersValues struct {
	values  []testValue
	readers []io.Reader
}

type testMultiReaderIterator struct {
	testIterator
	values []testReadersValues
	idx    int
	t      *testing.T
}

func newTestMultiReaderIterator(
	t *testing.T,
	values []testReadersValues,
) m3db.MultiReaderIterator {
	return &testMultiReaderIterator{
		testIterator: *newTestIterator(nil).(*testIterator),
		values:       values,
		idx:          -1,
		t:            t,
	}
}

func (it *testMultiReaderIterator) Reset(readers []io.Reader) {
	it.idx++
	assert.Equal(it.t, it.values[it.idx].readers, readers)
	it.testIterator = *newTestIterator(nil).(*testIterator)
	it.testIterator.values = it.values[it.idx].values
}

type testReaderSliceOfSlicesIterator struct {
	readers [][]io.Reader
	idx     int
	len     int
	closed  bool
}

func newTestReaderSliceOfSlicesIterator(
	readers [][]io.Reader,
) m3db.ReaderSliceOfSlicesIterator {
	return &testReaderSliceOfSlicesIterator{readers: readers, idx: -1}
}

func (it *testReaderSliceOfSlicesIterator) Next() bool {
	if !(it.idx+1 < len(it.readers)) {
		return false
	}
	it.idx++
	return true
}

func (it *testReaderSliceOfSlicesIterator) Current() []io.Reader {
	idx := it.idx
	if idx == -1 {
		idx = 0
	}
	return it.readers[idx]
}

func (it *testReaderSliceOfSlicesIterator) Close() {
	it.closed = true
}

type testNoopReader struct {
}

func (*testNoopReader) Read(p []byte) (n int, err error) {
	return 0, nil
}
