// Copyright (c) 2021 Uber Technologies, Inc.
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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/encoding"
	dts "github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
)

var normalization = PromOptions{
	AggregateNormalization: true,
}

type normalizeTest struct {
	vals         []float64
	tags         []string
	expectedVals []float64
}

func keyFromTags(tags []string) string { return strings.Join(tags, "_") }

func valsFromTested(tests []normalizeTest) map[string][]float64 {
	normalized := make(map[string][]float64, len(tests))
	for _, t := range tests {
		normalized[keyFromTags(t.tags)] = t.expectedVals
	}

	return normalized
}

func valsFromPromResult(res PromResult) map[string][]float64 {
	series := res.PromResult.Timeseries
	normalized := make(map[string][]float64, len(series))
	for _, s := range series {
		tags := make([]string, 0, len(s.Labels))
		for _, l := range s.Labels {
			tags = append(tags, string(l.Name), string(l.Value))
		}

		vals := make([]float64, 0, len(s.Samples))
		for _, v := range s.Samples {
			vals = append(vals, v.Value)
		}

		normalized[keyFromTags(tags)] = vals
	}

	return normalized
}

func TestNormalizeAggregatedSeries(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	testSeries := []normalizeTest{
		{
			vals:         []float64{1, 0, 7, 4, 8, 13},
			tags:         []string{"___name", "some_series", "baz", "qux"},
			expectedVals: []float64{1, 0, 7, 4, 8, 13},
		},
		{
			vals:         []float64{1, 0, 7, 0, 8, 0, 13, 0, 3, 0},
			tags:         []string{"___name", "should_normalize", "__rollup__", "true"},
			expectedVals: []float64{0, 0, 7, 7, 15, 15, 28, 28, 31, 31},
		},
		{
			vals:         []float64{1, 0, 2, 0, 3, 0},
			tags:         []string{"__rollup__", "rolled_up_count_short"},
			expectedVals: []float64{1, 0, 2, 0, 3, 0},
		},
		{
			vals:         []float64{1, 2, 3, 5, 6, 23},
			tags:         []string{"___name", "rollup_hist", "__rollup__", "rolled_up_hist", "le", "+Inf"},
			expectedVals: []float64{1, 2, 3, 5, 6, 23},
		},
		{
			vals:         []float64{1, 2, 3, 5, 6, 23},
			tags:         []string{"___name", "regular_hist", "le", "+Inf"},
			expectedVals: []float64{1, 2, 3, 5, 6, 23},
		},
		{
			vals:         []float64{1, 0, 7, 4, 8, 13},
			tags:         []string{"___name", "rollup_hist", "__rollup__", "rolled_up_hist", "le", "0.25"},
			expectedVals: []float64{1, 2, 7, 5, 8, 23},
		},
		{
			vals:         []float64{1, 0, 7, 4, 8, 13},
			tags:         []string{"___name", "regular_hist", "le", "0.25"},
			expectedVals: []float64{1, 0, 7, 4, 8, 13},
		},
		{
			vals:         []float64{1, 2, 3, 5, 6, 23},
			tags:         []string{"___name", "rollup_hist_rev", "__rollup__", "rolled_up_hist_rev", "le", "0.25"},
			expectedVals: []float64{1, 2, 7, 5, 8, 23},
		},
		{
			vals:         []float64{1, 0, 7, 4, 8, 13},
			tags:         []string{"___name", "rollup_hist_rev", "__rollup__", "rolled_up_hist_rev", "le", "+Inf"},
			expectedVals: []float64{1, 0, 7, 4, 8, 13},
		},
	}

	buildIter := func(
		vals []float64, tags ...string,
	) *encoding.MockSeriesIterator {
		iter := encoding.NewMockSeriesIterator(ctrl)

		for i, v := range vals {
			iter.EXPECT().Next().Return(true)
			dp := dts.Datapoint{TimestampNanos: xtime.UnixNano(1 + i), Value: v}
			iter.EXPECT().Current().Return(dp, xtime.Second, nil)
		}

		iter.EXPECT().Err().Return(nil)
		iter.EXPECT().Next().Return(false)

		tagIter, err := ident.NewTagStringsIterator(tags...)
		require.NoError(t, err)

		iter.EXPECT().Tags().Return(tagIter)
		iter.EXPECT().Close().MaxTimes(1)

		return iter
	}

	iterSlice := make([]encoding.SeriesIterator, 0, len(testSeries))
	for _, tt := range testSeries {
		iterSlice = append(iterSlice, buildIter(tt.vals, tt.tags...))
	}

	iters := encoding.NewSeriesIterators(iterSlice, nil)
	opts := models.NewTagOptions()
	pool, err := xsync.NewPooledWorkerPool(10, xsync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	pool.Init()

	fetchResult, err := consolidators.NewSeriesFetchResult(
		iters, nil, block.NewResultMetadata(),
	)

	require.NoError(t, err)
	res, err := SeriesIteratorsToPromResult(
		context.Background(), fetchResult, pool, opts, normalization,
	)

	require.NoError(t, err)

	ex := valsFromTested(testSeries)
	ac := valsFromPromResult(res)
	for k, v := range ex {
		require.Equal(t, ac[k], v)
	}

	require.Equal(t, ex, ac)
}
