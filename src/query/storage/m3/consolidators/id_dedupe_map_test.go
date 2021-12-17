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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
)

func verifyIDDedupeMap(
	t *testing.T,
	dedupeMap fetchDedupeMap,
	numSeries int,
) {
	series := dedupeMap.list()
	require.Equal(t, numSeries, len(series))
	val, found := series[0].tags.Get([]byte("foo"))
	require.True(t, found)
	assert.Equal(t, "bar", string(val))
	val, found = series[0].tags.Get([]byte("qux"))
	require.True(t, found)
	assert.Equal(t, "quail", string(val))
}

func idIt(
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

	it.EXPECT().Current().
		Return(ts.Datapoint{
			TimestampNanos: dp.t,
			Value:          dp.val,
		}, xtime.Second, nil).AnyTimes()
	it.EXPECT().Err().Return(nil).AnyTimes()
	it.EXPECT().Close().AnyTimes()

	return it
}

func rangeIt(
	ctrl *gomock.Controller,
	dp dp,
	id string,
	start, end xtime.UnixNano,
	tags ...string,
) encoding.SeriesIterator {
	it := encoding.NewMockSeriesIterator(ctrl)
	it.EXPECT().ID().Return(ident.StringID(id)).AnyTimes()

	it.EXPECT().Namespace().Return(ident.StringID("ns")).AnyTimes()
	it.EXPECT().Start().Return(start).AnyTimes()
	it.EXPECT().End().Return(end).AnyTimes()
	it.EXPECT().Next().Return(true).MaxTimes(1)

	tagIter := ident.MustNewTagStringsIterator(tags...)
	it.EXPECT().Tags().Return(tagIter).AnyTimes()

	it.EXPECT().Current().
		Return(ts.Datapoint{
			TimestampNanos: dp.t,
			Value:          dp.val,
		}, xtime.Second, nil).AnyTimes()
	it.EXPECT().Next().Return(false).AnyTimes()

	it.EXPECT().Err().Return(nil).AnyTimes()
	it.EXPECT().Close().AnyTimes()

	return it
}

func TestIDDedupeMap(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	dedupeMap := newIDDedupeMap(dedupeMapOpts{
		size:    8,
		fanout:  NamespaceCoversAllQueryRange,
		tagOpts: models.NewTagOptions(),
	})

	start := xtime.Now().Truncate(time.Hour)
	attrs := storagemetadata.Attributes{
		MetricsType: storagemetadata.UnaggregatedMetricsType,
		Resolution:  time.Hour,
	}

	err := dedupeMap.add(idIt(ctrl, dp{t: start, val: 14},
		"id1", "foo", "bar", "qux", "quail"), attrs)
	require.NoError(t, err)
	verifyIDDedupeMap(t, dedupeMap, 1)

	// Lower resolution must override.
	attrs.Resolution = time.Minute

	err = dedupeMap.add(idIt(ctrl, dp{t: start.Add(time.Minute), val: 10},
		"id1", "foo", "bar", "qux", "quail"), attrs)
	require.NoError(t, err)

	err = dedupeMap.add(idIt(ctrl, dp{t: start.Add(time.Minute * 2), val: 12},
		"id2", "foo", "bar", "qux", "quail"), attrs)
	require.NoError(t, err)

	verifyIDDedupeMap(t, dedupeMap, 2)

	// Lower resolution must override.
	attrs.Resolution = time.Second

	err = dedupeMap.add(idIt(ctrl, dp{t: start, val: 100},
		"id1", "foo", "bar", "qux", "quail"), attrs)
	require.NoError(t, err)

	verifyIDDedupeMap(t, dedupeMap, 2)

	err = dedupeMap.add(idIt(ctrl, dp{t: start.Add(time.Minute * 2), val: 12},
		"id4", "foo", "bar", "qux", "quail"), attrs)
	require.NoError(t, err)

	err = dedupeMap.add(idIt(ctrl, dp{t: start.Add(time.Minute * 2), val: 12},
		"id3", "foo", "bar", "qux", "quail"), attrs)
	require.NoError(t, err)

	// Get list multiple times and ensure they are always in same order.
	expectedIDs := make([]string, 0)
	for _, it := range dedupeMap.list() {
		expectedIDs = append(expectedIDs, it.tags.String())
	}
	for i := 0; i < 10; i++ {
		ids := make([]string, 0)
		for _, it := range dedupeMap.list() {
			ids = append(ids, it.tags.String())
		}
		require.Equal(t, expectedIDs, ids)
	}

	for _, it := range dedupeMap.list() {
		iter := it.iter
		require.NoError(t, iter.Err())
		iter.Close()
	}
}

func TestIDDedupeMapWithStitching(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	dedupeMap := newIDDedupeMap(dedupeMapOpts{
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
		rangeIt(ctrl, dp{t: start, val: 14}, "id1", start, stitchAt), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 1)

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start.Add(time.Minute), val: 10}, "id1", stitchAt, end), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 1)

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start.Add(time.Minute * 2), val: 12}, "id2", stitchAt, end), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 2)

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start, val: 100}, "id2", start, stitchAt), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 2)

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start.Add(time.Minute * 2), val: 12}, "id3", start, stitchAt), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 3)

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start.Add(time.Minute * 2), val: 12}, "id4", stitchAt, end), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 4)

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start.Add(time.Minute * 2), val: 5}, "id5", start, end), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 5)

	err = dedupeMap.add(
		rangeIt(ctrl, dp{t: start.Add(time.Minute * 3), val: 6}, "id5", start, end), attrs)
	require.NoError(t, err)
	assert.Equal(t, dedupeMap.len(), 5)

	actual := map[string]startEnd{}
	for _, it := range dedupeMap.list() {
		iter := it.iter
		actual[iter.ID().String()] = startEnd{
			start: iter.Start(),
			end:   iter.End(),
		}
		require.NoError(t, iter.Err())
		iter.Close()
	}
	expected := map[string]startEnd{
		"id1": {start, end},
		"id2": {start, end},
		"id3": {start, stitchAt},
		"id4": {stitchAt, end},
		"id5": {start, end},
	}
	assert.Equal(t, expected, actual)
}

type startEnd struct {
	start, end xtime.UnixNano
}
