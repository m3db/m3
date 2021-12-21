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

package consolidators

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func verifyDedupeMap(
	t *testing.T,
	dedupeMap fetchDedupeMap,
	expected ...ts.Datapoint,
) {
	series := dedupeMap.list()
	require.Equal(t, 1, len(series))
	val, found := series[0].tags.Get([]byte("foo"))
	require.True(t, found)
	assert.Equal(t, "bar", string(val))
	val, found = series[0].tags.Get([]byte("qux"))
	require.True(t, found)
	assert.Equal(t, "quail", string(val))

	iter := series[0].iter
	for _, expDp := range expected {
		require.True(t, iter.Next())
		dp, _, _ := iter.Current()
		assert.Equal(t, expDp, dp)
	}
	assert.False(t, iter.Next())
}

type dp struct {
	t   xtime.UnixNano
	val float64
}

func it(
	ctrl *gomock.Controller,
	dp dp,
	id string,
	tags ...string,
) encoding.SeriesIterator {
	it := encoding.NewMockSeriesIterator(ctrl)
	it.EXPECT().ID().Return(ident.StringID(id)).AnyTimes()

	it.EXPECT().Namespace().Return(ident.StringID("ns")).AnyTimes()
	it.EXPECT().Start().Return(dp.t).AnyTimes()
	it.EXPECT().End().Return(dp.t.Add(time.Hour)).AnyTimes()

	tagIter := ident.MustNewTagStringsIterator(tags...)
	it.EXPECT().Tags().Return(tagIter).AnyTimes()

	it.EXPECT().Next().Return(true)
	it.EXPECT().Current().
		Return(ts.Datapoint{
			TimestampNanos: dp.t,
			Value:          dp.val,
		}, xtime.Second, nil).AnyTimes()
	it.EXPECT().Next().Return(false)
	it.EXPECT().Err().Return(nil).AnyTimes()
	it.EXPECT().Close().MinTimes(1)

	return it
}

func notReadIt(
	ctrl *gomock.Controller,
	dp dp,
	id string,
	tags ...string,
) encoding.SeriesIterator {
	it := encoding.NewMockSeriesIterator(ctrl)
	it.EXPECT().ID().Return(ident.StringID(id)).AnyTimes()

	it.EXPECT().Namespace().Return(ident.StringID("ns")).AnyTimes()
	it.EXPECT().Start().Return(dp.t).AnyTimes()
	it.EXPECT().End().Return(dp.t.Add(time.Hour)).AnyTimes()

	tagIter := ident.MustNewTagStringsIterator(tags...)
	it.EXPECT().Tags().Return(tagIter).AnyTimes()

	it.EXPECT().Err().Return(nil).AnyTimes()
	it.EXPECT().Close().MinTimes(1)

	return it
}

func TestTagDedupeMap(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	dedupeMap := newTagDedupeMap(dedupeMapOpts{
		size:    8,
		fanout:  NamespaceCoversAllQueryRange,
		tagOpts: models.NewTagOptions(),
	})

	start := xtime.Now().Truncate(time.Hour)
	attrs := storagemetadata.Attributes{
		MetricsType: storagemetadata.UnaggregatedMetricsType,
		Resolution:  time.Hour,
	}

	err := dedupeMap.add(it(ctrl, dp{t: start, val: 14},
		"id1", "foo", "bar", "qux", "quail"), attrs)
	require.NoError(t, err)

	verifyDedupeMap(t, dedupeMap, ts.Datapoint{TimestampNanos: start, Value: 14})

	// Lower resolution must override.
	attrs.Resolution = time.Minute

	err = dedupeMap.add(it(ctrl, dp{t: start.Add(time.Minute), val: 10},
		"id1", "foo", "bar", "qux", "quail"), attrs)
	require.NoError(t, err)

	err = dedupeMap.add(it(ctrl, dp{t: start.Add(time.Minute * 2), val: 12},
		"id2", "foo", "bar", "qux", "quail"), attrs)
	require.NoError(t, err)

	verifyDedupeMap(t, dedupeMap,
		ts.Datapoint{TimestampNanos: start.Add(time.Minute), Value: 10},
		ts.Datapoint{TimestampNanos: start.Add(time.Minute * 2), Value: 12})

	// Lower resolution must override.
	attrs.Resolution = time.Second

	err = dedupeMap.add(it(ctrl, dp{t: start, val: 100},
		"id1", "foo", "bar", "qux", "quail"), attrs)
	require.NoError(t, err)

	verifyDedupeMap(t, dedupeMap, ts.Datapoint{TimestampNanos: start, Value: 100})

	for _, it := range dedupeMap.list() {
		iter := it.iter
		require.NoError(t, iter.Err())
		iter.Close()
	}
}

func TestTagDedupeMapWithStitching(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	dedupeMap := newTagDedupeMap(dedupeMapOpts{
		size:    8,
		fanout:  NamespaceCoversAllQueryRange,
		tagOpts: models.NewTagOptions(),
	})

	var (
		start    = xtime.Now().Truncate(time.Hour).Add(-24 * time.Hour)
		stitchAt = start.Add(12 * time.Hour)
		end      = start.Add(20 * time.Hour)

		attrs = storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
			Resolution:  time.Hour,
		}
	)

	err := dedupeMap.add(
		rangeIt(ctrl, dp{t: start, val: 14}, "id1", start, stitchAt,
			"foo", "bar", "qux", "quail"), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 1)

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start.Add(time.Minute), val: 10}, "id1", stitchAt, end,
			"foo", "bar", "qux", "quail"), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 1)

	verifyDedupeMap(t, dedupeMap,
		ts.Datapoint{TimestampNanos: start, Value: 14},
		ts.Datapoint{TimestampNanos: start.Add(time.Minute), Value: 10})

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start.Add(time.Minute * 2), val: 12}, "id2", stitchAt, end, "tag", "2"), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 2)

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start, val: 100}, "id2", start, stitchAt, "tag", "2"), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 2)

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start.Add(time.Minute * 2), val: 12}, "id3", start, stitchAt, "tag", "3"), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 3)

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start.Add(time.Minute * 2), val: 12}, "id4", stitchAt, end, "tag", "4"), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 4)

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start.Add(time.Minute * 2), val: 5}, "id5", start, end, "tag", "5"), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 5)

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start.Add(time.Minute * 3), val: 6}, "id5", start, end, "tag", "5"), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 5)

	actual := map[string]startEnd{}
	for _, it := range dedupeMap.list() {
		iter := it.iter
		actual[iter.ID().String()] = startEnd{
			start: iter.Start(),
			end:   iter.End(),
		}
	}
	expected := map[string]startEnd{
		"id1": {start, end},
		"id2": {start, end},
		"id3": {start, stitchAt},
		"id4": {stitchAt, end},
		"id5": {start, end},
	}
	assert.Equal(t, expected, actual)

	for _, it := range dedupeMap.list() {
		iter := it.iter
		require.NoError(t, iter.Err())
		iter.Close()
	}
}
