// Copyright (c) 2019 Uber Technologies, Inc.
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

package storage

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	dts "github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test/seriesiter"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/checked"
	xcost "github.com/m3db/m3/src/x/cost"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xsync "github.com/m3db/m3/src/x/sync"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func verifyExpandPromSeries(
	t *testing.T,
	ctrl *gomock.Controller,
	num int,
	ex bool,
	pools xsync.PooledWorkerPool,
) {
	testTags := seriesiter.GenerateTag()
	iters := seriesiter.NewMockSeriesIters(ctrl, testTags, num, 2)

	enforcer := cost.NewMockChainedEnforcer(ctrl)
	enforcer.EXPECT().Add(xcost.Cost(2)).Times(num)
	results, err := SeriesIteratorsToPromResult(iters, pools,
		block.ResultMetadata{
			Exhaustive: ex,
			LocalOnly:  true,
			Warnings:   []block.Warning{block.Warning{Name: "foo", Message: "bar"}},
		}, enforcer, nil)
	assert.NoError(t, err)

	require.NotNil(t, results)
	ts := results.PromResult.GetTimeseries()
	require.NotNil(t, ts)
	require.Equal(t, ex, results.Metadata.Exhaustive)
	require.Equal(t, 1, len(results.Metadata.Warnings))
	require.Equal(t, "foo_bar", results.Metadata.Warnings[0].Header())
	require.Equal(t, len(ts), num)
	expectedTags := []prompb.Label{
		prompb.Label{
			Name:  testTags.Name.Bytes(),
			Value: testTags.Value.Bytes(),
		},
	}

	for i := 0; i < num; i++ {
		series := ts[i]
		require.NotNil(t, series)
		require.Equal(t, expectedTags, series.GetLabels())
	}
}

func testExpandPromSeries(t *testing.T, ex bool, pools xsync.PooledWorkerPool) {
	ctrl := gomock.NewController(t)

	for i := 0; i < 10; i++ {
		verifyExpandPromSeries(t, ctrl, i, ex, pools)
	}
}

func TestExpandPromSeriesNilPools(t *testing.T) {
	testExpandPromSeries(t, false, nil)
	testExpandPromSeries(t, true, nil)
}

func TestExpandPromSeriesValidPools(t *testing.T) {
	pool, err := xsync.NewPooledWorkerPool(100, xsync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	pool.Init()
	testExpandPromSeries(t, false, pool)
	testExpandPromSeries(t, true, pool)
}

func TestExpandPromSeriesSmallValidPools(t *testing.T) {
	pool, err := xsync.NewPooledWorkerPool(2, xsync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	pool.Init()
	testExpandPromSeries(t, false, pool)
	testExpandPromSeries(t, true, pool)
}

func TestIteratorsToPromResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	promNow := TimeToPromTimestamp(now)

	vals := ts.NewMockValues(ctrl)
	vals.EXPECT().Len().Return(0).Times(2)
	vals.EXPECT().Datapoints().Return(ts.Datapoints{})

	tags := models.NewTags(1, models.NewTagOptions()).
		AddTag(models.Tag{Name: []byte("a"), Value: []byte("b")})

	valsNonEmpty := ts.NewMockValues(ctrl)
	valsNonEmpty.EXPECT().Len().Return(1).Times(3)
	dp := ts.Datapoints{{Timestamp: now, Value: 1}}
	valsNonEmpty.EXPECT().Datapoints().Return(dp).Times(2)
	tagsNonEmpty := models.NewTags(1, models.NewTagOptions()).
		AddTag(models.Tag{Name: []byte("c"), Value: []byte("d")})

	r := &FetchResult{
		SeriesList: ts.SeriesList{
			ts.NewSeries([]byte("a"), vals, tags),
			ts.NewSeries([]byte("c"), valsNonEmpty, tagsNonEmpty),
		},
	}

	// NB: not keeping empty series.
	result := FetchResultToPromResult(r, false)
	expected := &prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{
			&prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: []byte("c"), Value: []byte("d")}},
				Samples: []prompb.Sample{{Timestamp: promNow, Value: 1}},
			},
		},
	}

	assert.Equal(t, expected, result)

	// NB: keeping empty series.
	result = FetchResultToPromResult(r, true)
	expected = &prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{
			&prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: []byte("a"), Value: []byte("b")}},
				Samples: []prompb.Sample{},
			},
			&prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: []byte("c"), Value: []byte("d")}},
				Samples: []prompb.Sample{{Timestamp: promNow, Value: 1}},
			},
		},
	}

	assert.Equal(t, expected, result)
}

// overwrite overwrites existing tags with `!!!` literals.
type overwrite func()

func setupTags(name, value string) (ident.Tags, overwrite) {
	var buckets = []pool.Bucket{{Capacity: 100, Count: 2}}
	bytesPool := pool.NewCheckedBytesPool(buckets, nil,
		func(sizes []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(sizes, nil)
		})

	bytesPool.Init()
	getFromPool := func(id string) checked.Bytes {
		pID := bytesPool.Get(len(id))
		pID.IncRef()
		pID.AppendAll([]byte(id))
		pID.DecRef()
		return pID
	}

	idPool := ident.NewPool(bytesPool, ident.PoolOptions{})
	tags := idPool.Tags()
	tags.Append(idPool.BinaryTag(getFromPool(name), getFromPool(value)))
	tags.Append(idPool.BinaryTag(getFromPool(name), getFromPool("")))
	tags.Append(idPool.BinaryTag(getFromPool(""), getFromPool(value)))

	overwrite := func() {
		tags.Finalize()
		getFromPool("!!!")
		getFromPool("!!!")
	}

	return tags, overwrite
}

func TestTagIteratorToLabels(t *testing.T) {
	name := "foo"
	value := "bar"
	tags, overwrite := setupTags(name, value)
	tagIter := ident.NewTagsIterator(tags)
	labels, err := tagIteratorToLabels(tagIter)
	require.NoError(t, err)

	verifyTags := func() {
		require.Equal(t, 3, len(labels))
		assert.Equal(t, name, string(labels[0].GetName()))
		assert.Equal(t, value, string(labels[0].GetValue()))
		assert.Equal(t, name, string(labels[1].GetName()))
		assert.Equal(t, "", string(labels[1].GetValue()))
		assert.Equal(t, "", string(labels[2].GetName()))
		assert.Equal(t, value, string(labels[2].GetValue()))
	}

	verifyTags()
	overwrite()
	verifyTags()
}

func TestDecodeIteratorsWithEmptySeries(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	name := "name"
	now := time.Now()
	buildIter := func(val string, hasVal bool) encoding.SeriesIterator {
		iter := encoding.NewMockSeriesIterator(ctrl)

		if hasVal {
			iter.EXPECT().Next().Return(true)
			dp := dts.Datapoint{Timestamp: now, Value: 1}
			iter.EXPECT().Current().Return(dp, xtime.Second, nil)
		}

		iter.EXPECT().Err().Return(nil)
		iter.EXPECT().Next().Return(false)

		tag := ident.Tag{
			Name:  ident.TagName(ident.StringID(name)),
			Value: ident.TagValue(ident.StringID(val)),
		}

		tags := ident.NewMockTagIterator(ctrl)
		tags.EXPECT().Remaining().Return(1)
		tags.EXPECT().Next().Return(true)
		tags.EXPECT().Current().Return(tag)
		tags.EXPECT().Next().Return(false)
		tags.EXPECT().Err().Return(nil)

		iter.EXPECT().Tags().Return(tags)
		return iter
	}

	verifyResult := func(t *testing.T, res PromResult) {
		ts := res.PromResult.GetTimeseries()
		exSeriesTags := []string{"bar", "qux", "quail"}
		require.Equal(t, len(exSeriesTags), len(ts))
		for i, series := range ts {
			lbs := series.GetLabels()
			require.Equal(t, 1, len(lbs))
			assert.Equal(t, name, string(lbs[0].GetName()))
			assert.Equal(t, exSeriesTags[i], string(lbs[0].GetValue()))

			samples := series.GetSamples()
			require.Equal(t, 1, len(samples))
			s := samples[0]
			assert.Equal(t, float64(1), s.GetValue())
			assert.Equal(t, now.UnixNano()/int64(time.Millisecond), s.GetTimestamp())
		}
	}

	buildIters := func() encoding.SeriesIterators {
		iters := []encoding.SeriesIterator{
			buildIter("foo", false),
			buildIter("bar", true),
			buildIter("baz", false),
			buildIter("qux", true),
			buildIter("quail", true),
		}

		it := encoding.NewMockSeriesIterators(ctrl)
		it.EXPECT().Iters().Return(iters)
		it.EXPECT().Close()
		return it
	}

	md := block.NewResultMetadata()
	opts := models.NewTagOptions()

	res, err := SeriesIteratorsToPromResult(buildIters(), nil, md, nil, opts)
	require.NoError(t, err)
	verifyResult(t, res)

	pool, err := xsync.NewPooledWorkerPool(10, xsync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	pool.Init()

	res, err = SeriesIteratorsToPromResult(buildIters(), pool, md, nil, opts)
	require.NoError(t, err)
	verifyResult(t, res)
}
