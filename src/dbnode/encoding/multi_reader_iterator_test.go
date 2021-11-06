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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/x/xio"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testMultiReader struct {
	input       [][]testMultiReaderEntries
	expected    []testValue
	expectedErr *testMultiReaderError
}

type testMultiReaderEntries struct {
	values []testValue
	err    *testMultiReaderError
}

type testMultiReaderError struct {
	err   error
	atIdx int
}

func TestMultiReaderIteratorMergesMulti(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)

	values := [][]testValue{
		{
			{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
			{2.0, start.Add(2 * time.Second), xtime.Second, nil},
			{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		},
		{
			{4.0, start.Add(4 * time.Second), xtime.Second, []byte{4, 5, 6}},
			{5.0, start.Add(5 * time.Second), xtime.Second, nil},
			{6.0, start.Add(6 * time.Second), xtime.Second, nil},
		},
	}

	test := testMultiReader{
		input: [][]testMultiReaderEntries{
			{{values: values[0]}, {values: values[1]}},
		},
		expected: append(values[0], values[1]...),
	}

	assertTestMultiReaderIterator(t, test)
}

func TestMultiReaderIteratorMergesEmpty(t *testing.T) {
	values := [][]testValue{
		{},
		{},
	}

	test := testMultiReader{
		input: [][]testMultiReaderEntries{
			{{values: values[0]}},
			{{values: values[1]}},
		},
		expected: values[0],
	}

	assertTestMultiReaderIterator(t, test)

	test = testMultiReader{
		input: [][]testMultiReaderEntries{
			{{values: values[0]}, {values: values[1]}},
		},
		expected: values[0],
	}

	assertTestMultiReaderIterator(t, test)
}

func TestMultiReaderIteratorReadsSlicesInOrder(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)

	values := [][]testValue{
		{
			{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
			{2.0, start.Add(2 * time.Second), xtime.Second, nil},
			{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		},
		{
			{4.0, start.Add(4 * time.Second), xtime.Second, []byte{4, 5, 6}},
			{5.0, start.Add(5 * time.Second), xtime.Second, nil},
			{6.0, start.Add(6 * time.Second), xtime.Second, nil},
		},
	}

	test := testMultiReader{
		input: [][]testMultiReaderEntries{
			{{values: values[0]}},
			{{values: values[1]}},
		},
		expected: append(values[0], values[1]...),
	}

	assertTestMultiReaderIterator(t, test)
}

func TestMultiReaderIteratorReadsSlicesWithNoEntries(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)

	values := [][]testValue{
		{
			{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
			{2.0, start.Add(2 * time.Second), xtime.Second, nil},
			{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		},
		{
			{4.0, start.Add(4 * time.Second), xtime.Second, []byte{4, 5, 6}},
			{5.0, start.Add(5 * time.Second), xtime.Second, nil},
			{6.0, start.Add(6 * time.Second), xtime.Second, nil},
		},
	}

	test := testMultiReader{
		input: [][]testMultiReaderEntries{
			{{values: values[0]}},
			{},
			{{values: values[1]}},
		},
		expected: append(values[0], values[1]...),
	}

	assertTestMultiReaderIterator(t, test)
}

func TestMultiReaderIteratorReadsSlicesWithEmptyEntries(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)

	values := [][]testValue{
		{
			{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
			{2.0, start.Add(2 * time.Second), xtime.Second, nil},
			{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		},
		{},
		{
			{4.0, start.Add(4 * time.Second), xtime.Second, []byte{4, 5, 6}},
			{5.0, start.Add(5 * time.Second), xtime.Second, nil},
			{6.0, start.Add(6 * time.Second), xtime.Second, nil},
		},
	}

	test := testMultiReader{
		input: [][]testMultiReaderEntries{
			{{values: values[0]}},
			{{values: values[1]}},
			{{values: values[2]}},
		},
		expected: append(values[0], values[2]...),
	}

	assertTestMultiReaderIterator(t, test)
}

func TestMultiReaderIteratorDeduplicatesSingle(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)

	values := []testValue{
		{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
		{2.0, start.Add(2 * time.Second), xtime.Second, nil},
		{2.0, start.Add(2 * time.Second), xtime.Second, nil},
	}

	test := testMultiReader{
		input: [][]testMultiReaderEntries{
			{{values: values}},
		},
		expected: values[:2],
	}

	assertTestMultiReaderIterator(t, test)
}

func TestMultiReaderIteratorDeduplicatesMulti(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)

	values := []testValue{
		{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
		{2.0, start.Add(2 * time.Second), xtime.Second, nil},
		{3.0, start.Add(3 * time.Second), xtime.Second, nil},
	}

	test := testMultiReader{
		input: [][]testMultiReaderEntries{
			{
				{values: values},
				{values: values},
				{values: values},
						},
		},
		expected: values,
	}

	assertTestMultiReaderIterator(t, test)
}

func TestMultiReaderIteratorErrorOnOutOfOrder(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)

	values := []testValue{
		{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
		{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		{2.0, start.Add(2 * time.Second), xtime.Second, nil},
	}

	test := testMultiReader{
		input: [][]testMultiReaderEntries{
			{{values: values}},
		},
		expected: values,
		expectedErr: &testMultiReaderError{
			err:   errOutOfOrderIterator,
			atIdx: 2,
		},
	}

	assertTestMultiReaderIterator(t, test)
}

func TestMultiReaderIteratorErrorOnInnerIteratorError(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)

	values := []testValue{
		{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
		{2.0, start.Add(2 * time.Second), xtime.Second, nil},
		{3.0, start.Add(3 * time.Second), xtime.Second, nil},
	}

	err := fmt.Errorf("an error")

	test := testMultiReader{
		input: [][]testMultiReaderEntries{
			{{
				values: values,
				err: &testMultiReaderError{
					err:   err,
					atIdx: 2,
				},
						}},
		},
		expected: values,
		expectedErr: &testMultiReaderError{
			err:   err,
			atIdx: 2,
		},
	}

	assertTestMultiReaderIterator(t, test)
}

func assertTestMultiReaderIterator(
	t *testing.T,
	test testMultiReader,
) {
	type readerEntries struct {
		reader  xio.Reader64
		entries *testMultiReaderEntries
	}

	var (
		blocks          [][]xio.BlockReader
		entriesByReader []readerEntries
	)

	for i := range test.input {
		var blocksArray []xio.BlockReader
		for j := range test.input[i] {
			reader := &testNoopReader{}
			entries := &test.input[i][j]
			block := xio.BlockReader{
				SegmentReader: reader,
			}
			entriesByReader = append(entriesByReader, readerEntries{
				reader:  block,
				entries: entries,
			})
			blocksArray = append(blocksArray, block)
		}

		blocks = append(blocks, blocksArray)
	}

	var testIterators []*testIterator
	var iteratorAlloc func(xio.Reader64, namespace.SchemaDescr) ReaderIterator
	iteratorAlloc = func(reader xio.Reader64, _ namespace.SchemaDescr) ReaderIterator {
		for i := range entriesByReader {
			if reader != entriesByReader[i].reader {
				continue
			}
			entries := entriesByReader[i].entries
			it := newTestIterator(entries.values).(*testIterator)
			// Install error if set
			if entries.err != nil {
				it.onNext = func(oldIdx, newIdx int) {
					if newIdx == entries.err.atIdx {
						it.err = entries.err.err
					}
				}
			}
			it.onReset = func(r xio.Reader64, descr namespace.SchemaDescr) {
				newIt := iteratorAlloc(r, descr).(*testIterator)
				*it = *newIt
				// We close this here as we never actually use this iterator
				// and we test at the end of the test to ensure all iterators were closed
				// by the multi reader iterator
				newIt.closed = true
			}
			testIterators = append(testIterators, it)
			return it
		}
		assert.Fail(t, "iterator allocate called for unknown reader")
		return nil
	}

	iter := NewMultiReaderIterator(iteratorAlloc, nil)
	slicesIter := newTestReaderSliceOfSlicesIterator(blocks)
	iter.ResetSliceOfSlices(slicesIter, nil)

	for i := 0; i < len(test.expected); i++ {
		next := iter.Next()
		if test.expectedErr != nil && i == test.expectedErr.atIdx {
			assert.Equal(t, false, next)
			break
		}
		require.Equal(t, true, next, "expected next for idx %d", i)
		dp, unit, annotation := iter.Current()
		expected := test.expected[i]
		require.Equal(t, expected.value, dp.Value, fmt.Sprintf("mismatch for idx %d", i))
		require.Equal(t, expected.t, dp.TimestampNanos, fmt.Sprintf("mismatch for idx %d", i))
		require.Equal(t, expected.unit, unit, fmt.Sprintf("mismatch for idx %d", i))
		require.Equal(t, expected.annotation, annotation, fmt.Sprintf("mismatch for idx %d", i))
	}

	// Ensure further calls to next false
	for i := 0; i < 2; i++ {
		assert.Equal(t, false, iter.Next())
	}

	if test.expectedErr == nil {
		assert.NoError(t, iter.Err())
	} else {
		assert.Equal(t, test.expectedErr.err, iter.Err())
	}

	iter.Close()

	// Ensure all closed
	for _, iter := range testIterators {
		assert.Equal(t, true, iter.closed)
	}
	assert.Equal(t, true, slicesIter.(*testReaderSliceOfSlicesIterator).closed)
}
