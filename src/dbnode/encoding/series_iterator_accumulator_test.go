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

	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeriesIteratorAccumulatorReplicas(t *testing.T) {
	testSeriesIteratorAccumulatorReplicas(t, false)
}

func TestSeriesIteratorAccumulatorReplicasRetainTag(t *testing.T) {
	testSeriesIteratorAccumulatorReplicas(t, true)
}

func testSeriesIteratorAccumulatorReplicas(t *testing.T, retain bool) {
	start := time.Now().Truncate(time.Minute)
	end := start.Add(time.Minute)

	values := []inputReplica{
		{
			values: []testValue{
				{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
				{2.0, start.Add(2 * time.Second), xtime.Second, nil},
				{3.0, start.Add(3 * time.Second), xtime.Second, nil},
			},
		},
		{
			values: []testValue{
				{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
				{2.0, start.Add(2 * time.Second), xtime.Second, nil},
				{3.0, start.Add(3 * time.Second), xtime.Second, nil},
			},
		},
		{
			values: []testValue{
				{3.0, start.Add(0 * time.Second), xtime.Second, nil},
				{4.0, start.Add(4 * time.Second), xtime.Second, nil},
				{5.0, start.Add(5 * time.Second), xtime.Second, nil},
			},
		},
	}

	ex := []testValue{
		{3.0, start.Add(0 * time.Second), xtime.Second, nil},
		{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
		{2.0, start.Add(2 * time.Second), xtime.Second, nil},
		{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		{4.0, start.Add(4 * time.Second), xtime.Second, nil},
		{5.0, start.Add(5 * time.Second), xtime.Second, nil},
	}

	test := testSeries{
		id:        "foo",
		nsID:      "bar",
		start:     start,
		end:       end,
		retainTag: retain,
		input:     values,
		expected:  ex,
	}

	assertTestSeriesAccumulatorIterator(t, test)
}

type newTestSeriesAccumulatorIteratorResult struct {
	iter        *seriesIteratorAccumulator
	seriesIters []SeriesIterator
}

func newTestSeriesAccumulatorIterator(
	t *testing.T,
	series testSeries,
) newTestSeriesAccumulatorIteratorResult {
	iters := make([]SeriesIterator, 0, len(series.input))
	var acc SeriesIteratorAccumulator
	for i := range series.input {
		multiIter := newTestMultiIterator(
			series.input[i].values,
			series.input[i].err,
		)

		iter := NewSeriesIterator(SeriesIteratorOptions{
			ID:        ident.StringID(series.id),
			Namespace: ident.StringID(series.nsID),
			Tags: ident.NewTagsIterator(ident.NewTags(
				ident.StringTag("foo", "bar"), ident.StringTag("qux", "quz"),
			)),
			StartInclusive: xtime.ToUnixNano(series.start),
			EndExclusive:   xtime.ToUnixNano(series.end),
			Replicas:       []MultiReaderIterator{multiIter},
		}, nil)

		iters = append(iters, iter)
		if acc == nil {
			a, err := NewSeriesIteratorAccumulator(iter, SeriesAccumulatorOptions{
				RetainTags: series.retainTag,
			})
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
	series testSeries,
) {
	newSeriesIter := newTestSeriesAccumulatorIterator(t, series)
	iter := newSeriesIter.iter

	checkTags := func() {
		tags := iter.Tags()
		if tags == nil {
			return
		}
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
		checkTags()
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

	var tagIter ident.TagIterator
	if series.retainTag {
		checkTags()
		tagIter = iter.Tags()
	} else {
		assert.Nil(t, iter.Tags())
	}

	iter.Close()
	if series.retainTag {
		// Check that the tag iterator was closed.
		assert.False(t, tagIter.Next())
	}
}
