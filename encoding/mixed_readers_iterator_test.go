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

	xtime "github.com/m3db/m3db/x/time"

	"github.com/stretchr/testify/assert"
)

type testMixedReadersEntry struct {
	single *testMixedReaderSingleEntry
	multi  *testMixedReaderMultiEntry
}

type testMixedReaderSingleEntry struct {
	reader io.Reader
	values []testValue
}

type testMixedReaderMultiEntry struct {
	readers []io.Reader
	values  []testValue
}

func TestMixedReadersIteratorSingle(t *testing.T) {
	var a io.Reader
	all := []*io.Reader{&a}
	for _, r := range all {
		*r = &testNoopReader{}
	}

	start := time.Now().Truncate(time.Minute)

	assertTestMixedReadersIterator(t, []testMixedReadersEntry{
		{single: &testMixedReaderSingleEntry{
			reader: a,
			values: []testValue{
				{1.0, start.Add(1 * time.Millisecond), xtime.Millisecond, []byte{1, 2, 3}},
				{2.0, start.Add(1 * time.Second), xtime.Second, nil},
				{3.0, start.Add(2 * time.Second), xtime.Second, nil},
			},
		}},
	})
}

func TestMixedReadersIteratorSingleMultiSingle(t *testing.T) {
	var a, b, c, d, e io.Reader
	all := []*io.Reader{&a, &b, &c, &d, &e}
	for _, r := range all {
		*r = &testNoopReader{}
	}

	start := time.Now().Truncate(time.Minute)

	assertTestMixedReadersIterator(t, []testMixedReadersEntry{
		{single: &testMixedReaderSingleEntry{
			reader: a,
			values: []testValue{
				{1.0, start.Add(1 * time.Millisecond), xtime.Millisecond, []byte{1, 2, 3}},
				{2.0, start.Add(1 * time.Second), xtime.Second, nil},
				{3.0, start.Add(2 * time.Second), xtime.Second, nil},
			},
		}},
		{multi: &testMixedReaderMultiEntry{
			readers: []io.Reader{b, c, d},
			values: []testValue{
				{4.0, start.Add(1 * time.Second), xtime.Second, []byte{4, 5, 6}},
				{5.0, start.Add(2 * time.Second), xtime.Second, nil},
				{6.0, start.Add(3 * time.Second), xtime.Second, nil},
			},
		}},
		{single: &testMixedReaderSingleEntry{
			reader: e,
			values: []testValue{
				{7.0, start.Add(1 * time.Minute), xtime.Second, []byte{7, 8, 9}},
				{8.0, start.Add(3 * time.Minute), xtime.Second, nil},
				{9.0, start.Add(2 * time.Minute), xtime.Second, nil},
			},
		}},
	})
}

func assertTestMixedReadersIterator(
	t *testing.T,
	entries []testMixedReadersEntry,
) {
	var (
		values           []testValue
		singleIterValues []testReaderValues
		multiIterValues  []testReadersValues
		slicesIterValues [][]io.Reader
	)

	for _, entry := range entries {
		if entry.single != nil {
			values = append(values, entry.single.values...)
			singleIterValues = append(singleIterValues, testReaderValues{
				reader: entry.single.reader,
				values: entry.single.values,
			})
			slicesIterValues = append(slicesIterValues, []io.Reader{entry.single.reader})
		} else if entry.multi != nil {
			values = append(values, entry.multi.values...)
			multiIterValues = append(multiIterValues, testReadersValues{
				readers: entry.multi.readers,
				values:  entry.multi.values,
			})
			slicesIterValues = append(slicesIterValues, entry.multi.readers)
		}
	}

	singleIter := newTestSingleReaderIterator(t, singleIterValues)
	multiIter := newTestMultiReaderIterator(t, multiIterValues)
	slicesIter := newTestReaderSliceOfSlicesIterator(slicesIterValues)

	iter := NewMixedReadersIterator(singleIter, multiIter, slicesIter, nil)
	defer iter.Close()

	for i := 0; i < len(values); i++ {
		next := iter.Next()
		assert.Equal(t, true, next)
		dp, unit, annotation := iter.Current()
		expected := values[i]
		assert.Equal(t, expected.value, dp.Value)
		assert.Equal(t, expected.t, dp.Timestamp)
		assert.Equal(t, expected.unit, unit)
		assert.Equal(t, expected.annotation, []byte(annotation))
	}
	assert.NoError(t, iter.Err())
}
