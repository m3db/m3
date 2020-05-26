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

func (h *testSeriesLoadHandler) getSeriesIterators(name string) (encoding.SeriesIterators, error) {
	return h.iters, nil
}

var _ seriesLoadHandler = (*testSeriesLoadHandler)(nil)

type tagMap map[string]string

var (
	iteratorOpts = iteratorOptions{
		encoderPool:   encoderPool,
		iteratorPools: iterPools,
		tagOptions:    tagOptions,
		iOpts:         iOpts,
	}
	metricNameTag = string(iteratorOpts.tagOptions.MetricName())
)

const (
	blockSize             = time.Hour * 12
	defaultResolution     = time.Second * 30
	metricsName           = "preloaded"
	predefinedSeriesCount = 10
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

			result, cleanup, err := querier.FetchCompressed(nil, query, nil)
			assert.NoError(t, err)
			defer cleanup()

			assert.Equal(t, tt.expectedCount, result.SeriesIterators.Len())
		})
	}
}

func TestFetchCompressedGeneratesRandomData(t *testing.T) {
	tests := []struct {
		name       string
		givenQuery *storage.FetchQuery
		wantSeries []tagMap
	}{
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

			querier, _ := newQuerier(iteratorOpts, emptySeriesLoader(ctrl), blockSize, defaultResolution)

			result, cleanup, err := querier.FetchCompressed(nil, tt.givenQuery, nil)
			assert.NoError(t, err)
			defer cleanup()

			require.Equal(t, len(tt.wantSeries), result.SeriesIterators.Len())
			for i, expectedTags := range tt.wantSeries {
				iter := result.SeriesIterators.Iters()[i]
				assert.Equal(t, expectedTags, extractTags(iter))
				assert.True(t, iter.Next(), "Must have some datapoints generated.")
			}
		})
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

func emptySeriesLoader(ctrl *gomock.Controller) seriesLoadHandler {
	iters := encoding.NewMockSeriesIterators(ctrl)
	iters.EXPECT().Len().Return(0).AnyTimes()

	return &testSeriesLoadHandler{iters}
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
