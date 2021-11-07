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
	"context"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/generated/proto/annotation"
	dts "github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/test/seriesiter"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fr(
	t *testing.T,
	its encoding.SeriesIterators,
	tags ...*models.Tags,
) consolidators.SeriesFetchResult {
	result, err := consolidators.
		NewSeriesFetchResult(its, tags, block.NewResultMetadata())
	assert.NoError(t, err)
	return result
}

func makeTag(n, v string, count int) []*models.Tags {
	tags := make([]*models.Tags, 0, count)
	for i := 0; i < count; i++ {
		t := models.EmptyTags().AddTag(models.Tag{Name: []byte(n), Value: []byte(v)})
		tags = append(tags, &t)
	}

	return tags
}

func verifyExpandPromSeries(
	t *testing.T,
	ctrl *gomock.Controller,
	num int,
	ex bool,
	pools xsync.PooledWorkerPool,
) {
	iters := seriesiter.NewMockSeriesIters(ctrl, ident.Tag{}, num, 2)
	fetchResult := fr(t, iters, makeTag("foo", "bar", num)...)
	fetchResult.Metadata = block.ResultMetadata{
		Exhaustive: ex,
		LocalOnly:  true,
		Warnings:   []block.Warning{{Name: "foo", Message: "bar"}},
	}

	results, err := SeriesIteratorsToPromResult(context.Background(), fetchResult, pools, nil)
	assert.NoError(t, err)

	require.NotNil(t, results)
	ts := results.PromResult.GetTimeseries()
	require.NotNil(t, ts)
	require.Equal(t, ex, results.Metadata.Exhaustive)
	require.Equal(t, 1, len(results.Metadata.Warnings))
	require.Equal(t, "foo_bar", results.Metadata.Warnings[0].Header())
	require.Equal(t, len(ts), num)
	expectedTags := []prompb.Label{
		{
			Name:  []byte("foo"),
			Value: []byte("bar"),
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

func TestContextCanceled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pool, err := xsync.NewPooledWorkerPool(100, xsync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	pool.Init()

	iters := seriesiter.NewMockSeriesIters(ctrl, ident.Tag{}, 1, 2)
	fetchResult := fr(t, iters, makeTag("foo", "bar", 1)...)
	_, err = SeriesIteratorsToPromResult(ctx, fetchResult, pool, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "context canceled")
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

	now := xtime.Now()
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
			{
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
			{
				Labels:  []prompb.Label{{Name: []byte("a"), Value: []byte("b")}},
				Samples: []prompb.Sample{},
			},
			{
				Labels:  []prompb.Label{{Name: []byte("c"), Value: []byte("d")}},
				Samples: []prompb.Sample{{Timestamp: promNow, Value: 1}},
			},
		},
	}

	assert.Equal(t, expected, result)
}

func TestDecodeIteratorsWithEmptySeries(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	name := "name"
	now := xtime.Now()
	buildIter := func(val string, hasVal bool) *encoding.MockSeriesIterator {
		iter := encoding.NewMockSeriesIterator(ctrl)

		if hasVal {
			iter.EXPECT().Next().Return(true)
			dp := dts.Datapoint{TimestampNanos: now, Value: 1}
			iter.EXPECT().Current().Return(dp, xtime.Second, nil)
		}

		iter.EXPECT().Err().Return(nil)
		iter.EXPECT().Next().Return(false)

		tag := ident.Tag{
			Name:  ident.TagName(ident.StringID(name)),
			Value: ident.TagValue(ident.StringID(val)),
		}

		tags := ident.NewMockTagIterator(ctrl)
		populateIter := func() {
			gomock.InOrder(
				tags.EXPECT().Remaining().Return(1),
				tags.EXPECT().Next().Return(true),
				tags.EXPECT().Current().Return(tag),
				tags.EXPECT().Next().Return(false),
				tags.EXPECT().Err().Return(nil),
				tags.EXPECT().Rewind(),
			)
		}

		populateIter()
		iter.EXPECT().Tags().Return(tags)
		iter.EXPECT().Close().MaxTimes(1)

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
			assert.Equal(t, int64(now)/int64(time.Millisecond), s.GetTimestamp())
		}
	}

	buildIters := func() consolidators.SeriesFetchResult {
		iters := []encoding.SeriesIterator{
			buildIter("foo", false),
			buildIter("bar", true),
			buildIter("baz", false),
			buildIter("qux", true),
			buildIter("quail", true),
		}

		it := encoding.NewMockSeriesIterators(ctrl)
		it.EXPECT().Iters().Return(iters).AnyTimes()
		it.EXPECT().Len().Return(len(iters)).AnyTimes()
		return fr(t, it)
	}

	opts := models.NewTagOptions()
	res, err := SeriesIteratorsToPromResult(context.Background(), buildIters(), nil, opts)
	require.NoError(t, err)
	verifyResult(t, res)

	pool, err := xsync.NewPooledWorkerPool(10, xsync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	pool.Init()

	res, err = SeriesIteratorsToPromResult(context.Background(), buildIters(), pool, opts)
	require.NoError(t, err)
	verifyResult(t, res)
}

func TestSeriesIteratorsToPromResultNormalizeLowResCounters(t *testing.T) {
	t0 := xtime.Now().Truncate(time.Hour)

	tests := []struct {
		name          string
		isCounter     bool
		maxResolution time.Duration
		given         []dts.Datapoint
		want          []prompb.Sample
	}{
		{
			name:          "low resolution gauge",
			isCounter:     false,
			maxResolution: time.Hour,
			given:         []dts.Datapoint{{t0, 1}, {t0.Add(time.Hour), 2}},
			want:          []prompb.Sample{{1, ms(t0)}, {2, ms(t0.Add(time.Hour))}},
		},
		{
			name:          "high resolution gauge",
			isCounter:     false,
			maxResolution: time.Minute,
			given:         []dts.Datapoint{{t0, 1}, {t0.Add(time.Minute), 2}},
			want:          []prompb.Sample{{1, ms(t0)}, {2, ms(t0.Add(time.Minute))}},
		},
		{
			name:          "low resolution counter, no datapoints",
			isCounter:     true,
			maxResolution: time.Hour,
		},
		{
			name:          "low resolution counter, one datapoint",
			isCounter:     true,
			maxResolution: time.Hour,
			given:         []dts.Datapoint{{t0, 1}},
			want:          []prompb.Sample{{1, ms(t0)}},
		},
		{
			name:          "high resolution counter with no resets",
			isCounter:     true,
			maxResolution: time.Minute,
			given: []dts.Datapoint{
				{t0, 1},
				{t0.Add(time.Minute), 2},
				{t0.Add(2 * time.Minute), 3},
				{t0.Add(3 * time.Minute), 4},
			},
			want: []prompb.Sample{
				{1, ms(t0)},
				{2, ms(t0.Add(time.Minute))},
				{3, ms(t0.Add(2 * time.Minute))},
				{4, ms(t0.Add(3 * time.Minute))},
			},
		},
		{
			name:          "high resolution counter with resets",
			isCounter:     true,
			maxResolution: time.Minute,
			given: []dts.Datapoint{
				{t0, 10},
				{t0.Add(time.Minute), 3},
				{t0.Add(2 * time.Minute), 5},
				{t0.Add(3 * time.Minute), 8},
			},
			want: []prompb.Sample{
				{10, ms(t0)},
				{3, ms(t0.Add(time.Minute))},
				{5, ms(t0.Add(2 * time.Minute))},
				{8, ms(t0.Add(3 * time.Minute))},
			},
		},
		{
			name:          "low resolution counter with no resets",
			isCounter:     true,
			maxResolution: time.Hour,
			given: []dts.Datapoint{
				{t0, 1},
				{t0.Add(time.Minute), 2},
				{t0.Add(time.Hour), 3},
				{t0.Add(time.Hour + time.Minute), 4},
			},
			want: []prompb.Sample{
				{2, ms(t0.Add(time.Minute))},
				{4, ms(t0.Add(time.Hour + time.Minute))},
			},
		},
		{
			name:          "low resolution counter with resets",
			isCounter:     true,
			maxResolution: time.Hour,
			given: []dts.Datapoint{
				{t0, 10},
				{t0.Add(time.Minute), 3},
				{t0.Add(time.Hour), 5},
				{t0.Add(time.Hour + time.Minute), 8},
			},
			want: []prompb.Sample{
				{13, ms(t0.Add(time.Minute))},
				{18, ms(t0.Add(time.Hour + time.Minute))},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testSeriesIteratorsToPromResultNormalize(
				t, tt.isCounter, tt.maxResolution, tt.given, tt.want)
		})
	}
}

func testSeriesIteratorsToPromResultNormalize(
	t *testing.T,
	isCounter bool,
	maxResolution time.Duration,
	given []dts.Datapoint,
	want []prompb.Sample,
) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		gaugePayload   = &annotation.Payload{MetricType: annotation.MetricType_GAUGE}
		counterPayload = &annotation.Payload{MetricType: annotation.MetricType_COUNTER, HandleValueResets: true}
	)

	firstAnnotation := annotationBytes(t, gaugePayload)
	if isCounter {
		firstAnnotation = annotationBytes(t, counterPayload)
	}

	iter := encoding.NewMockSeriesIterator(ctrl)

	iter.EXPECT().FirstAnnotation().Return(firstAnnotation).MaxTimes(1)
	for _, dp := range given {
		iter.EXPECT().Next().Return(true)
		iter.EXPECT().Current().Return(dp, xtime.Second, nil)
	}

	iter.EXPECT().Err().Return(nil)
	iter.EXPECT().Next().Return(false)

	iter.EXPECT().Tags().Return(ident.EmptyTagIterator)

	verifyResult := func(t *testing.T, expected []prompb.Sample, res PromResult) {
		timeSeries := res.PromResult.GetTimeseries()
		if len(expected) == 0 {
			require.Equal(t, 0, len(timeSeries))
		} else {
			require.Equal(t, 1, len(timeSeries))
			samples := timeSeries[0].Samples
			require.Equal(t, expected, samples)
		}
	}

	iters := []encoding.SeriesIterator{iter}

	it := encoding.NewMockSeriesIterators(ctrl)
	it.EXPECT().Iters().Return(iters).AnyTimes()
	it.EXPECT().Len().Return(len(iters)).AnyTimes()

	fetchResultMetadata := block.NewResultMetadata()
	fetchResultMetadata.Resolutions = []time.Duration{maxResolution, maxResolution / 2}
	fetchResult, err := consolidators.NewSeriesFetchResult(it, nil, fetchResultMetadata)
	assert.NoError(t, err)

	opts := models.NewTagOptions()
	res, err := SeriesIteratorsToPromResult(context.Background(), fetchResult, nil, opts)
	require.NoError(t, err)
	verifyResult(t, want, res)
}

func ms(t xtime.UnixNano) int64 {
	return t.ToNormalizedTime(time.Millisecond)
}

func annotationBytes(t *testing.T, payload *annotation.Payload) dts.Annotation {
	if payload != nil {
		annotationBytes, err := payload.Marshal()
		require.NoError(t, err)
		return annotationBytes
	}
	return nil
}
