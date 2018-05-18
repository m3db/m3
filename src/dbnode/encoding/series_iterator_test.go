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
	"testing"
	"time"

	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testSeries struct {
	id          string
	nsID        string
	start       time.Time
	end         time.Time
	input       [][]testValue
	expected    []testValue
	expectedErr *testSeriesErr
}

type testSeriesErr struct {
	err   error
	atIdx int
}

func TestMultiReaderMergesReplicas(t *testing.T) {
	start := time.Now().Truncate(time.Minute)
	end := start.Add(time.Minute)

	values := [][]testValue{
		[]testValue{
			{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
			{2.0, start.Add(2 * time.Second), xtime.Second, nil},
			{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		},
		[]testValue{
			{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
			{2.0, start.Add(2 * time.Second), xtime.Second, nil},
			{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		},
		[]testValue{
			{3.0, start.Add(3 * time.Second), xtime.Second, nil},
			{4.0, start.Add(4 * time.Second), xtime.Second, nil},
			{5.0, start.Add(5 * time.Second), xtime.Second, nil},
		},
	}

	test := testSeries{
		id:       "foo",
		nsID:     "bar",
		start:    start,
		end:      end,
		input:    values,
		expected: append(values[0], values[2][1:]...),
	}

	assertTestSeriesIterator(t, test)
}

func TestMultiReaderFiltersToRange(t *testing.T) {
	start := time.Now().Truncate(time.Minute)
	end := start.Add(time.Minute)

	values := []testValue{
		{0.0, start.Add(-2 * time.Second), xtime.Second, []byte{1, 2, 3}},
		{1.0, start.Add(-1 * time.Second), xtime.Second, nil},
		{2.0, start, xtime.Second, nil},
		{3.0, start.Add(1 * time.Second), xtime.Second, nil},
		{4.0, start.Add(60 * time.Second), xtime.Second, nil},
		{5.0, start.Add(61 * time.Second), xtime.Second, nil},
	}

	test := testSeries{
		id:       "foo",
		nsID:     "bar",
		start:    start,
		end:      end,
		input:    [][]testValue{values, values, values},
		expected: values[2:4],
	}

	assertTestSeriesIterator(t, test)
}

func TestSeriesIteratorIgnoresEmptyReplicas(t *testing.T) {
	start := time.Now().Truncate(time.Minute)
	end := start.Add(time.Minute)

	values := []testValue{
		{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
		{2.0, start.Add(2 * time.Second), xtime.Second, nil},
		{3.0, start.Add(3 * time.Second), xtime.Second, nil},
	}

	test := testSeries{
		id:       "foo",
		nsID:     "bar",
		start:    start,
		end:      end,
		input:    [][]testValue{values, []testValue{}, values},
		expected: values,
	}

	assertTestSeriesIterator(t, test)
}

func TestSeriesIteratorErrorOnOutOfOrder(t *testing.T) {
	start := time.Now().Truncate(time.Minute)
	end := start.Add(time.Minute)

	values := []testValue{
		{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
		{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		{2.0, start.Add(2 * time.Second), xtime.Second, nil},
	}

	test := testSeries{
		id:       "foo",
		nsID:     "bar",
		start:    start,
		end:      end,
		input:    [][]testValue{values},
		expected: values[:2],
		expectedErr: &testSeriesErr{
			err:   errOutOfOrderIterator,
			atIdx: 2,
		},
	}

	assertTestSeriesIterator(t, test)
}

func assertTestSeriesIterator(
	t *testing.T,
	series testSeries,
) {
	var iters []MultiReaderIterator
	for i := range series.input {
		if series.input[i] == nil {
			iters = append(iters, nil)
		} else {
			iters = append(iters, newTestMultiIterator(series.input[i]))
		}
	}

	iter := NewSeriesIterator(ident.StringID(series.id), ident.StringID(series.nsID), ident.EmptyTagIterator, series.start, series.end, iters, nil)
	defer iter.Close()

	assert.Equal(t, series.id, iter.ID().String())
	assert.Equal(t, series.nsID, iter.Namespace().String())
	assert.Equal(t, series.start, iter.Start())
	assert.Equal(t, series.end, iter.End())
	for i := 0; i < len(series.expected); i++ {
		next := iter.Next()
		if series.expectedErr != nil && i == series.expectedErr.atIdx {
			assert.Equal(t, false, next)
			break
		}
		require.Equal(t, true, next)
		dp, unit, annotation := iter.Current()
		expected := series.expected[i]
		assert.Equal(t, expected.value, dp.Value)
		assert.Equal(t, expected.t, dp.Timestamp)
		assert.Equal(t, expected.unit, unit)
		assert.Equal(t, expected.annotation, []byte(annotation))
	}
	// Ensure further calls to next false
	for i := 0; i < 2; i++ {
		assert.Equal(t, false, iter.Next())
	}
	if series.expectedErr == nil {
		assert.NoError(t, iter.Err())
	} else {
		assert.Equal(t, series.expectedErr.err, iter.Err())
	}
	for _, iter := range iters {
		if iter != nil {
			assert.Equal(t, true, iter.(*testMultiIterator).closed)
		}
	}
}
