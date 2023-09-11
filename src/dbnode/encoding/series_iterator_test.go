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
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testSeries struct {
	id          string
	nsID        string
	retainTag   bool
	start       xtime.UnixNano
	end         xtime.UnixNano
	input       []inputReplica
	expected    []testValue
	expectedErr *testSeriesErr

	expectedFirstAnnotation ts.Annotation
}

type inputReplica struct {
	values []testValue
	err    error
}

type testSeriesErr struct {
	err   error
	atIdx int
}

func TestMultiReaderMergesReplicas(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)
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
				{3.0, start.Add(3 * time.Second), xtime.Second, nil},
				{4.0, start.Add(4 * time.Second), xtime.Second, nil},
				{5.0, start.Add(5 * time.Second), xtime.Second, nil},
			},
		},
	}

	expected := []testValue{
		{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
		{2.0, start.Add(2 * time.Second), xtime.Second, nil},
		{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		{4.0, start.Add(4 * time.Second), xtime.Second, nil},
		{5.0, start.Add(5 * time.Second), xtime.Second, nil},
	}

	test := testSeries{
		id:                      "foo",
		nsID:                    "bar",
		start:                   start,
		end:                     end,
		input:                   values,
		expected:                expected,
		expectedFirstAnnotation: []byte{1, 2, 3},
	}

	assertTestSeriesIterator(t, test)
}

func TestMultiReaderFiltersToRange(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)
	end := start.Add(time.Minute)

	input := []inputReplica{
		{
			values: []testValue{
				{0.0, start.Add(-2 * time.Second), xtime.Second, []byte{1, 2, 3}},
				{1.0, start.Add(-1 * time.Second), xtime.Second, nil},
				{2.0, start, xtime.Second, nil},
				{3.0, start.Add(1 * time.Second), xtime.Second, nil},
				{4.0, start.Add(60 * time.Second), xtime.Second, nil},
				{5.0, start.Add(61 * time.Second), xtime.Second, nil},
			},
		},
	}

	test := testSeries{
		id:                      "foo",
		nsID:                    "bar",
		start:                   start,
		end:                     end,
		input:                   input,
		expected:                input[0].values[2:4],
		expectedFirstAnnotation: []byte{1, 2, 3},
	}

	assertTestSeriesIterator(t, test)
}

func TestSeriesIteratorIgnoresEmptyReplicas(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)
	end := start.Add(time.Minute)

	values := []testValue{
		{1.0, start.Add(1 * time.Second), xtime.Second, []byte{3, 2, 1}},
		{2.0, start.Add(2 * time.Second), xtime.Second, nil},
		{3.0, start.Add(3 * time.Second), xtime.Second, nil},
	}

	test := testSeries{
		id:    "foo",
		nsID:  "bar",
		start: start,
		end:   end,
		input: []inputReplica{
			{values: values},
			{values: []testValue{}},
			{values: values},
		},
		expected:                values,
		expectedFirstAnnotation: []byte{3, 2, 1},
	}

	assertTestSeriesIterator(t, test)
}

func TestSeriesIteratorDoesNotIgnoreReplicasWithErrors(t *testing.T) {
	var (
		start = xtime.Now().Truncate(time.Minute)
		end   = start.Add(time.Minute)
		err   = errors.New("some-iteration-error")
	)

	test := testSeries{
		id:    "foo",
		nsID:  "bar",
		start: start,
		end:   end,
		input: []inputReplica{
			{err: err},
		},
		expectedErr: &testSeriesErr{err: err},
	}

	assertTestSeriesIterator(t, test)
}

func TestSeriesIteratorErrorOnOutOfOrder(t *testing.T) {
	start := xtime.Now().Truncate(time.Minute)
	end := start.Add(time.Minute)

	values := []testValue{
		{1.0, start.Add(1 * time.Second), xtime.Second, []byte{1, 2, 3}},
		{3.0, start.Add(3 * time.Second), xtime.Second, nil},
		{2.0, start.Add(2 * time.Second), xtime.Second, nil},
	}

	test := testSeries{
		id:                      "foo",
		nsID:                    "bar",
		start:                   start,
		end:                     end,
		input:                   []inputReplica{{values: values}},
		expected:                values[:2],
		expectedFirstAnnotation: []byte{1, 2, 3},
		expectedErr: &testSeriesErr{
			err:   errOutOfOrderIterator,
			atIdx: 2,
		},
	}

	assertTestSeriesIterator(t, test)
}

func TestSeriesIteratorSetIterateEqualTimestampStrategy(t *testing.T) {
	test := testSeries{
		id:   "foo",
		nsID: "bar",
	}

	iter := newTestSeriesIterator(t, test).iter

	// Ensure default value if none set
	assert.Equal(t, iter.iters.equalTimesStrategy,
		DefaultIterateEqualTimestampStrategy)

	// Ensure value is propagated during a reset
	iter.Reset(SeriesIteratorOptions{
		ID:                            ident.StringID("baz"),
		IterateEqualTimestampStrategy: IterateHighestValue,
	})
	assert.Equal(t, iter.iters.equalTimesStrategy,
		IterateHighestValue)

	// Ensure falls back to default after a reset without specifying
	iter.Reset(SeriesIteratorOptions{
		ID: ident.StringID("baz"),
	})
	assert.Equal(t, iter.iters.equalTimesStrategy,
		DefaultIterateEqualTimestampStrategy)
}

type testSeriesConsolidator struct {
	iters []MultiReaderIterator
}

func (c *testSeriesConsolidator) ConsolidateReplicas(
	_ []MultiReaderIterator,
) ([]MultiReaderIterator, error) {
	return c.iters, nil
}

func TestSeriesIteratorSetSeriesIteratorConsolidator(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	test := testSeries{
		id:   "foo",
		nsID: "bar",
	}

	iter := newTestSeriesIterator(t, test).iter
	newIter := NewMockMultiReaderIterator(ctrl)
	newIter.EXPECT().Next().Return(true)
	newIter.EXPECT().Current().Return(ts.Datapoint{}, xtime.Second, nil).Times(3)

	iter.iters.setFilter(0, 1)
	consolidator := &testSeriesConsolidator{iters: []MultiReaderIterator{newIter}}
	oldIter := NewMockMultiReaderIterator(ctrl)
	oldIters := []MultiReaderIterator{oldIter}
	iter.multiReaderIters = oldIters
	assert.Equal(t, oldIter, iter.multiReaderIters[0])
	iter.Reset(SeriesIteratorOptions{
		Replicas:                   oldIters,
		SeriesIteratorConsolidator: consolidator,
	})
	assert.Equal(t, newIter, iter.multiReaderIters[0])
}

type newTestSeriesIteratorResult struct {
	iter                 *seriesIterator
	multiReaderIterators []MultiReaderIterator
}

func newTestSeriesIterator(
	t *testing.T,
	series testSeries,
) newTestSeriesIteratorResult {
	var iters []MultiReaderIterator
	for i := range series.input {
		multiIter := newTestMultiIterator(
			series.input[i].values,
			series.input[i].err,
		)
		iters = append(iters, multiIter)
	}

	iter := NewSeriesIterator(SeriesIteratorOptions{
		ID:             ident.StringID(series.id),
		Namespace:      ident.StringID(series.nsID),
		Tags:           ident.EmptyTagIterator,
		StartInclusive: series.start,
		EndExclusive:   series.end,
		Replicas:       iters,
	}, nil)

	seriesIter, ok := iter.(*seriesIterator)
	require.True(t, ok)

	return newTestSeriesIteratorResult{
		iter:                 seriesIter,
		multiReaderIterators: iters,
	}
}

func assertTestSeriesIterator(
	t *testing.T,
	series testSeries,
) {
	newSeriesIter := newTestSeriesIterator(t, series)
	iter := newSeriesIter.iter
	multiReaderIterators := newSeriesIter.multiReaderIterators
	defer iter.Close()

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
	for _, iter := range multiReaderIterators {
		if iter != nil {
			assert.True(t, iter.(*testMultiIterator).closed)
		}
	}
}

func TestSeriesIteratorStats(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	segReader := func(head, tail int) xio.SegmentReader {
		var h, t checked.Bytes
		if head > 0 {
			h = checked.NewBytes(make([]byte, head), nil)
			h.IncRef()
		}
		if tail > 0 {
			t = checked.NewBytes(make([]byte, tail), nil)
			t.IncRef()
		}
		return xio.NewSegmentReader(ts.Segment{
			Head: h,
			Tail: t,
		})
	}
	readerOne := &singleSlicesOfSlicesIterator{
		readers: []xio.SegmentReader{segReader(0, 5), segReader(5, 5)},
	}
	readerTwo := &singleSlicesOfSlicesIterator{
		readers: []xio.SegmentReader{segReader(2, 2), segReader(5, 0)},
	}
	iter := seriesIterator{
		multiReaderIters: []MultiReaderIterator{
			&multiReaderIterator{slicesIter: readerOne},
			&multiReaderIterator{slicesIter: readerTwo},
		},
	}
	stats, err := iter.Stats()
	require.NoError(t, err)
	assert.Equal(t, 24, stats.ApproximateSizeInBytes)
}
