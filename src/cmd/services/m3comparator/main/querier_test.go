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

package main

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3comparator/main/parser"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testSeriesLoadHandler struct {
	iters encoding.SeriesIterators
}

func (h *testSeriesLoadHandler) getSeriesIterators(
	name string) (encoding.SeriesIterators, error) {
	return h.iters, nil
}

var _ seriesLoadHandler = (*testSeriesLoadHandler)(nil)

type tagMap map[string]string

var (
	iteratorOpts = parser.Options{
		EncoderPool:       encoderPool,
		IteratorPools:     iterPools,
		TagOptions:        tagOptions,
		InstrumentOptions: iOpts,
	}
	metricNameTag = string(iteratorOpts.TagOptions.MetricName())
)

const (
	blockSize             = time.Hour * 12
	defaultResolution     = time.Second * 30
	metricsName           = "preloaded"
	predefinedSeriesCount = 10
	histogramBucketCount  = 4
)

func TestFetchCompressed(t *testing.T) {
	tests := []struct {
		name          string
		queryTagName  string
		queryTagValue string
		expectedCount int
	}{
		{
			name:          "querying by metric name returns preloaded data",
			queryTagName:  metricNameTag,
			queryTagValue: metricsName,
			expectedCount: predefinedSeriesCount,
		},
		{
			name:          "querying without metric name just by other tag returns preloaded data",
			queryTagName:  "tag1",
			queryTagValue: "test2",
			expectedCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := xtest.NewController(t)
			defer ctrl.Finish()

			query := matcherQuery(t, tt.queryTagName, tt.queryTagValue)
			querier := setupQuerier(ctrl, query)

			result, cleanup, err := querier.FetchCompressedResult(nil, query, nil)
			assert.NoError(t, err)
			defer cleanup()

			assert.Equal(t, tt.expectedCount, len(result.SeriesIterators()))
		})
	}
}

func TestGenerateRandomSeries(t *testing.T) {
	tests := []struct {
		name       string
		givenQuery *storage.FetchQuery
		wantSeries []tagMap
	}{
		{
			name:       "querying nonexistent_metric returns empty",
			givenQuery: matcherQuery(t, metricNameTag, "nonexistent_metric"),
			wantSeries: []tagMap{},
		},
		{
			name:       "querying nonexistant returns empty",
			givenQuery: matcherQuery(t, metricNameTag, "nonexistant"),
			wantSeries: []tagMap{},
		},
		{
			name:       "random data for known metrics",
			givenQuery: matcherQuery(t, metricNameTag, "quail"),
			wantSeries: []tagMap{
				{
					metricNameTag: "quail",
					"foobar":      "qux",
					"name":        "quail",
				},
			},
		},
		{
			name:       "a hardcoded list of metrics",
			givenQuery: matcherQuery(t, metricNameTag, "unknown"),
			wantSeries: []tagMap{
				{
					metricNameTag: "foo",
					"foobar":      "qux",
					"name":        "foo",
				},
				{
					metricNameTag: "bar",
					"foobar":      "qux",
					"name":        "bar",
				},
				{
					metricNameTag: "quail",
					"foobar":      "qux",
					"name":        "quail",
				},
			},
		},
		{
			name:       "a given number of single series metrics",
			givenQuery: matcherQuery(t, "gen", "2"),
			wantSeries: []tagMap{
				{
					metricNameTag: "foo_0",
					"foobar":      "qux",
					"name":        "foo_0",
				},
				{
					metricNameTag: "foo_1",
					"foobar":      "qux",
					"name":        "foo_1",
				},
			},
		},
		{
			name:       "single metrics with a given number of series",
			givenQuery: matcherQuery(t, metricNameTag, "multi_4"),
			wantSeries: []tagMap{
				{
					metricNameTag: "multi_4",
					"const":       "x",
					"id":          "0",
					"parity":      "0",
				},
				{
					metricNameTag: "multi_4",
					"const":       "x",
					"id":          "1",
					"parity":      "1",
				},
				{
					metricNameTag: "multi_4",
					"const":       "x",
					"id":          "2",
					"parity":      "0",
				},
				{
					metricNameTag: "multi_4",
					"const":       "x",
					"id":          "3",
					"parity":      "1",
				},
			},
		},
		{
			name:       "histogram metrics",
			givenQuery: matcherQuery(t, metricNameTag, "histogram_2_bucket"),
			wantSeries: []tagMap{
				{
					metricNameTag: "histogram_2_bucket",
					"const":       "x",
					"id":          "0",
					"parity":      "0",
					"le":          "1",
				},
				{
					metricNameTag: "histogram_2_bucket",
					"const":       "x",
					"id":          "0",
					"parity":      "0",
					"le":          "10",
				},
				{
					metricNameTag: "histogram_2_bucket",
					"const":       "x",
					"id":          "0",
					"parity":      "0",
					"le":          "100",
				},
				{
					metricNameTag: "histogram_2_bucket",
					"const":       "x",
					"id":          "0",
					"parity":      "0",
					"le":          "+Inf",
				},

				{
					metricNameTag: "histogram_2_bucket",
					"const":       "x",
					"id":          "1",
					"parity":      "1",
					"le":          "1",
				},
				{
					metricNameTag: "histogram_2_bucket",
					"const":       "x",
					"id":          "1",
					"parity":      "1",
					"le":          "10",
				},
				{
					metricNameTag: "histogram_2_bucket",
					"const":       "x",
					"id":          "1",
					"parity":      "1",
					"le":          "100",
				},
				{
					metricNameTag: "histogram_2_bucket",
					"const":       "x",
					"id":          "1",
					"parity":      "1",
					"le":          "+Inf",
				},
			},
		},
		{
			name: "apply tag filter",
			givenQuery: and(
				matcherQuery(t, metricNameTag, "multi_5"),
				matcherQuery(t, "parity", "1")),
			wantSeries: []tagMap{
				{
					metricNameTag: "multi_5",
					"const":       "x",
					"id":          "1",
					"parity":      "1",
				},
				{
					metricNameTag: "multi_5",
					"const":       "x",
					"id":          "3",
					"parity":      "1",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := xtest.NewController(t)
			defer ctrl.Finish()

			querier, err := setupRandomGenQuerier(ctrl)
			assert.NoError(t, err)

			result, cleanup, err := querier.FetchCompressedResult(nil, tt.givenQuery, nil)
			assert.NoError(t, err)
			defer cleanup()

			iters := result.SeriesIterators()
			require.Equal(t, len(tt.wantSeries), len(iters))
			for i, expectedTags := range tt.wantSeries {
				iter := iters[i]
				assert.Equal(t, expectedTags, extractTags(iter))
				assert.True(t, iter.Next(), "Must have some datapoints generated.")
			}
		})
	}
}

func TestHistogramBucketsAddUp(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	querier, err := setupRandomGenQuerier(ctrl)
	assert.NoError(t, err)

	histogramQuery := matcherQuery(t, metricNameTag, "histogram_1_bucket")
	result, cleanup, err := querier.FetchCompressedResult(nil, histogramQuery, nil)
	assert.NoError(t, err)
	defer cleanup()

	iters := result.SeriesIterators()
	require.Equal(t, histogramBucketCount,
		len(iters), "number of histogram buckets")

	iter0 := iters[0]
	for iter0.Next() {
		v0, t1, _ := iter0.Current()
		for i := 1; i < histogramBucketCount; i++ {
			iter := iters[i]
			require.True(t, iter.Next(), "all buckets must have the same length")
			vi, ti, _ := iter.Current()
			assert.True(t, vi.Value >= v0.Value, "bucket values must be non decreasing")
			assert.Equal(t, v0.TimestampNanos, vi.TimestampNanos, "bucket values timestamps must match")
			assert.Equal(t, t1, ti)
		}
	}

	for _, iter := range iters {
		require.False(t, iter.Next(), "all buckets must have the same length")
	}
}

func matcherQuery(t *testing.T, matcherName, matcherValue string) *storage.FetchQuery {
	matcher, err := models.NewMatcher(models.MatchEqual, []byte(matcherName), []byte(matcherValue))
	assert.NoError(t, err)

	now := time.Now()

	return &storage.FetchQuery{
		TagMatchers: []models.Matcher{matcher},
		Start:       now.Add(-time.Hour),
		End:         now,
	}
}

func and(query1, query2 *storage.FetchQuery) *storage.FetchQuery {
	return &storage.FetchQuery{
		TagMatchers: append(query1.TagMatchers, query2.TagMatchers...),
		Start:       query1.Start,
		End:         query1.End,
	}
}

func extractTags(seriesIter encoding.SeriesIterator) tagMap {
	tagsIter := seriesIter.Tags().Duplicate()
	defer tagsIter.Close()

	tags := make(tagMap)
	for tagsIter.Next() {
		tag := tagsIter.Current()
		tags[tag.Name.String()] = tag.Value.String()
	}

	return tags
}

func setupQuerier(ctrl *gomock.Controller, query *storage.FetchQuery) *querier {
	metricsTag := ident.NewTags(ident.Tag{
		Name:  ident.BytesID(tagOptions.MetricName()),
		Value: ident.BytesID(metricsName),
	},
		ident.Tag{
			Name:  ident.BytesID("tag1"),
			Value: ident.BytesID("test"),
		},
	)
	metricsTag2 := ident.NewTags(ident.Tag{
		Name:  ident.BytesID(tagOptions.MetricName()),
		Value: ident.BytesID(metricsName),
	},
		ident.Tag{
			Name:  ident.BytesID("tag1"),
			Value: ident.BytesID("test2"),
		},
	)

	iters := make([]encoding.SeriesIterator, 0, predefinedSeriesCount)
	for i := 0; i < predefinedSeriesCount; i++ {
		m := metricsTag
		if i > 5 {
			m = metricsTag2
		}
		iters = append(iters, encoding.NewSeriesIterator(
			encoding.SeriesIteratorOptions{
				Namespace:      ident.StringID("ns"),
				Tags:           ident.NewTagsIterator(m),
				StartInclusive: xtime.ToUnixNano(query.Start),
				EndExclusive:   xtime.ToUnixNano(query.End),
			}, nil))
	}

	seriesIterators := encoding.NewMockSeriesIterators(ctrl)
	seriesIterators.EXPECT().Len().Return(predefinedSeriesCount).MinTimes(1)
	seriesIterators.EXPECT().Iters().Return(iters).Times(1)
	seriesIterators.EXPECT().Close()

	seriesLoader := &testSeriesLoadHandler{seriesIterators}

	return &querier{iteratorOpts: iteratorOpts, handler: seriesLoader}
}

func setupRandomGenQuerier(ctrl *gomock.Controller) (*querier, error) {
	iters := encoding.NewMockSeriesIterators(ctrl)
	iters.EXPECT().Len().Return(0).AnyTimes()

	emptySeriesLoader := &testSeriesLoadHandler{iters}

	return newQuerier(iteratorOpts, emptySeriesLoader, blockSize, defaultResolution, histogramBucketCount)
}
