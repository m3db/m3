// Copyright (c) 2020 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testAccumulatorSeries struct {
	id          string
	nsID        string
	start       xtime.UnixNano
	end         xtime.UnixNano
	input       []accumulatorInput
	expected    []testValue
	expectedErr *testSeriesErr

	expectedFirstAnnotation ts.Annotation
}

type accumulatorInput struct {
	values []testValue
	id     string
	err    error
}

func TestSeriesIteratorAccumulator(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)
	end := start.Add(time.Minute)

	values := []accumulatorInput{
		{
			values: []testValue{
				{1.0, start.Add(-1 * time.Second), xtime.Second, nil},
				{2.0, start.Add(1 * time.Second), xtime.Second, nil},
			},
			id: "foo0",
		},
		{
			values: []testValue{
				{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
				{2.0, start.Add(2 * time.Second), xtime.Second, nil},
				{3.0, start.Add(6 * time.Second), xtime.Second, nil},
			},
			id: "foo1",
		},
		{
			values: []testValue{
				{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
				{2.0, start.Add(2 * time.Second), xtime.Second, nil},
				{3.0, start.Add(3 * time.Second), xtime.Second, nil},
			},
			id: "foo2",
		},
		{
			values: []testValue{
				{3.0, start.Add(1 * time.Millisecond), xtime.Second, []byte{5}},
				{4.0, start.Add(4 * time.Second), xtime.Second, nil},
				{5.0, start.Add(5 * time.Second), xtime.Second, nil},
			},
			id: "foo3",
		},
	}

	ex := []testValue{
		{3.0, start.Add(1 * time.Millisecond), xtime.Second, []byte{5}},
		{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
		{2.0, start.Add(2 * time.Second), xtime.Second, nil},
		{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		{4.0, start.Add(4 * time.Second), xtime.Second, nil},
		{5.0, start.Add(5 * time.Second), xtime.Second, nil},
		{3.0, start.Add(6 * time.Second), xtime.Second, nil},
	}

	test := testAccumulatorSeries{
		id:                      "foo0",
		nsID:                    "bar",
		start:                   start,
		end:                     end,
		input:                   values,
		expected:                ex,
		expectedFirstAnnotation: []byte{5},
	}

	assertTestSeriesAccumulatorIterator(t, test)
}

func TestSingleSeriesIteratorAccumulator(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)
	end := start.Add(time.Minute)

	values := []accumulatorInput{
		{
			values: []testValue{
				{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
				{2.0, start.Add(2 * time.Second), xtime.Second, nil},
				{3.0, start.Add(6 * time.Second), xtime.Second, nil},
			},
			id: "foobar",
		},
	}

	ex := []testValue{
		{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
		{2.0, start.Add(2 * time.Second), xtime.Second, nil},
		{3.0, start.Add(6 * time.Second), xtime.Second, nil},
	}

	test := testAccumulatorSeries{
		id:                      "foobar",
		nsID:                    "bar",
		start:                   start,
		end:                     end,
		input:                   values,
		expected:                ex,
		expectedFirstAnnotation: []byte{1, 2, 3},
	}

	assertTestSeriesAccumulatorIterator(t, test)
}

type newTestSeriesAccumulatorIteratorResult struct {
	iter        *seriesIteratorAccumulator
	seriesIters []SeriesIterator
}

func newTestSeriesAccumulatorIterator(
	t *testing.T,
	series testAccumulatorSeries,
) newTestSeriesAccumulatorIteratorResult {
	iters := make([]SeriesIterator, 0, len(series.input))
	var acc SeriesIteratorAccumulator
	for _, r := range series.input {
		multiIter := newTestMultiIterator(
			r.values,
			r.err,
		)

		iter := NewSeriesIterator(SeriesIteratorOptions{
			ID:        ident.StringID(r.id),
			Namespace: ident.StringID(series.nsID),
			Tags: ident.NewTagsIterator(ident.NewTags(
				ident.StringTag("foo", "bar"), ident.StringTag("qux", "quz"),
			)),
			StartInclusive: series.start,
			EndExclusive:   series.end,
			Replicas:       []MultiReaderIterator{multiIter},
		}, nil)

		iters = append(iters, iter)
		if acc == nil {
			a, err := NewSeriesIteratorAccumulator(iter)
			require.NoError(t, err)
			acc = a
		} else {
			err := acc.Add(iter)
			require.NoError(t, err)
		}
	}

	accumulator, ok := acc.(*seriesIteratorAccumulator)
	require.True(t, ok)
	return newTestSeriesAccumulatorIteratorResult{
		iter:        accumulator,
		seriesIters: iters,
	}
}

func assertTestSeriesAccumulatorIterator(
	t *testing.T,
	series testAccumulatorSeries,
) {
	newSeriesIter := newTestSeriesAccumulatorIterator(t, series)
	iter := newSeriesIter.iter

	checkTags := func() {
		tags := iter.Tags()
		require.NotNil(t, tags)
		require.True(t, tags.Next())
		assert.True(t, tags.Current().Equal(ident.StringTag("foo", "bar")))
		require.True(t, tags.Next())
		assert.True(t, tags.Current().Equal(ident.StringTag("qux", "quz")))
		assert.False(t, tags.Next())
		assert.NoError(t, tags.Err())
		tags.Rewind()
	}

	checkTags()
	assert.Equal(t, series.id, iter.ID().String())
	assert.Equal(t, series.nsID, iter.Namespace().String())
	assert.Equal(t, series.start, iter.Start())
	assert.Equal(t, series.end, iter.End())
	for i := 0; i < len(series.expected); i++ {
		next := iter.Next()
		if series.expectedErr != nil && i == series.expectedErr.atIdx {
			assert.False(t, next)
			break
		}
		require.True(t, next)
		dp, unit, annotation := iter.Current()
		expected := series.expected[i]
		assert.Equal(t, expected.value, dp.Value)
		assert.Equal(t, expected.t, dp.TimestampNanos)
		assert.Equal(t, expected.unit, unit)
		assert.Equal(t, expected.annotation, annotation)
		assert.Equal(t, series.expectedFirstAnnotation, iter.FirstAnnotation())
		checkTags()
	}
	// Ensure further calls to next false
	for i := 0; i < 2; i++ {
		assert.False(t, iter.Next())
	}
	if series.expectedErr == nil {
		assert.NoError(t, iter.Err())
	} else {
		assert.Equal(t, series.expectedErr.err, iter.Err())
	}

	assert.Equal(t, series.id, iter.id.String())
	assert.Equal(t, series.nsID, iter.nsID.String())
	iter.Close()

	// Check that the tag iterator was closed.
	tagIter := iter.Tags()
	require.NotNil(t, tagIter)
	assert.False(t, tagIter.Next())
}

func TestAccumulatorMocked(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	start := xtime.Now()
	annotation := ts.Annotation{5, 6, 7}
	base := NewMockSeriesIterator(ctrl)
	base.EXPECT().ID().Return(ident.StringID("base")).AnyTimes()
	base.EXPECT().Namespace().Return(ident.StringID("ns")).AnyTimes()
	base.EXPECT().Next().Return(true)
	dp := ts.Datapoint{TimestampNanos: start, Value: 88}
	base.EXPECT().Current().Return(dp, xtime.Second, annotation).AnyTimes()
	base.EXPECT().Next().Return(false)
	base.EXPECT().Start().Return(start)
	base.EXPECT().End().Return(start.Add(time.Hour))
	base.EXPECT().Err().Return(nil).AnyTimes()
	base.EXPECT().Close()

	it, err := NewSeriesIteratorAccumulator(base)
	require.NoError(t, err)

	i := 0
	for it.Next() {
		ac, timeUnit, annot := it.Current()
		assert.Equal(t, dp, ac)
		assert.Equal(t, xtime.Second, timeUnit)
		assert.Equal(t, annotation, annot)
		assert.Equal(t, annotation, it.FirstAnnotation())
		i++
	}

	assert.Equal(t, 1, i)
	it.Close()
}
