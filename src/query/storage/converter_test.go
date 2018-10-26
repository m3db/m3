// Copyright (c) 2018 Uber Technologies, Inc.
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
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test/seriesiter"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3x/ident"
	xsync "github.com/m3db/m3x/sync"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func verifyExpandSeries(t *testing.T, ctrl *gomock.Controller, num int, pools xsync.PooledWorkerPool) {
	testTags := seriesiter.GenerateTag()
	iters := seriesiter.NewMockSeriesIters(ctrl, testTags, num, 2)

	results, err := SeriesIteratorsToFetchResult(iters, pools, true, nil)
	assert.NoError(t, err)

	require.NotNil(t, results)
	require.NotNil(t, results.SeriesList)
	require.Len(t, results.SeriesList, num)
	expectedTags := []models.Tag{{Name: testTags.Name.Bytes(), Value: testTags.Value.Bytes()}}
	for i := 0; i < num; i++ {
		series := results.SeriesList[i]
		require.NotNil(t, series)
		require.Equal(t, expectedTags, series.Tags.Tags)
	}
}

func testExpandSeries(t *testing.T, pools xsync.PooledWorkerPool) {
	ctrl := gomock.NewController(t)

	for i := 0; i < 100; i++ {
		verifyExpandSeries(t, ctrl, i, pools)
	}
}

func TestExpandSeriesNilPools(t *testing.T) {
	testExpandSeries(t, nil)
}

func TestExpandSeriesValidPools(t *testing.T) {
	pool, err := xsync.NewPooledWorkerPool(100, xsync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	pool.Init()
	testExpandSeries(t, pool)
}

func TestExpandSeriesSmallValidPools(t *testing.T) {
	pool, err := xsync.NewPooledWorkerPool(2, xsync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	pool.Init()
	testExpandSeries(t, pool)
}

func TestFailingExpandSeriesValidPools(t *testing.T) {
	var (
		numValidSeries = 8
		numValues      = 2
		poolSize       = 2
		numUncalled    = 10
	)
	pool, err := xsync.NewPooledWorkerPool(poolSize, xsync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	pool.Init()
	ctrl := gomock.NewController(t)

	iters := seriesiter.NewMockSeriesIterSlice(ctrl, seriesiter.NewMockValidTagGenerator(ctrl), numValidSeries, numValues)
	// Add poolSize + 1 failing iterators; there can be slight timing
	// inconsistencies which can sometimes cause failures in this test
	// as one of the `uncalled` iterators gets unexpectedly used.
	// This is not a big issue in practice, as all it means is one further
	// iterator is expanded before erroring out.
	for i := 0; i < poolSize+1; i++ {
		invalidIter := encoding.NewMockSeriesIterator(ctrl)
		invalidIter.EXPECT().ID().Return(ident.StringID("foo")).Times(1)

		tags := ident.NewMockTagIterator(ctrl)
		tags.EXPECT().Next().Return(false).MaxTimes(1)
		tags.EXPECT().Remaining().Return(0).MaxTimes(1)
		tags.EXPECT().Err().Return(errors.New("error")).MaxTimes(1)
		invalidIter.EXPECT().Tags().Return(tags).MaxTimes(1)

		iters = append(iters, invalidIter)
	}

	for i := 0; i < numUncalled; i++ {
		uncalledIter := encoding.NewMockSeriesIterator(ctrl)
		iters = append(iters, uncalledIter)
	}

	mockIters := encoding.NewMockSeriesIterators(ctrl)
	mockIters.EXPECT().Iters().Return(iters).Times(1)
	mockIters.EXPECT().Len().Return(len(iters)).Times(1)
	mockIters.EXPECT().Close().Times(1)

	result, err := SeriesIteratorsToFetchResult(
		mockIters,
		pool,
		true,
		nil,
	)
	require.Nil(t, result)
	require.EqualError(t, err, "error")
}

var (
	name  = []byte("foo")
	value = []byte("bar")
)

func TestPromReadQueryToM3(t *testing.T) {
	tests := []struct {
		name        string
		matchers    []*prompb.LabelMatcher
		expected    []*models.Matcher
		expectError bool
	}{
		{
			name: "single exact match",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: name, Value: value},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchEqual, Name: name, Value: value},
			},
		},
		{
			name: "single exact match negated",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_NEQ, Name: name, Value: value},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchNotEqual, Name: name, Value: value},
			},
		},
		{
			name: "single regexp match",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_RE, Name: name, Value: value},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchRegexp, Name: name, Value: value},
			},
		},
		{
			name: "single regexp match negated",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_NRE, Name: name, Value: value},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchNotRegexp, Name: name, Value: value},
			},
		},
		{
			name: "mixed exact match and regexp match",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: name, Value: value},
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_RE, Name: []byte("baz"), Value: []byte("qux")},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchEqual, Name: name, Value: value},
				&models.Matcher{Type: models.MatchRegexp, Name: []byte("baz"), Value: []byte("qux")},
			},
		},
		{
			name: "unrecognized matcher type",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_Type(math.MaxInt32), Name: name, Value: value},
			},
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := &prompb.Query{
				StartTimestampMs: 123000,
				EndTimestampMs:   456000,
				Matchers:         test.matchers,
			}
			result, err := PromReadQueryToM3(input)
			if test.expectError {
				assert.Error(t, err)
			} else {
				assert.True(t, result.Start.Equal(time.Unix(123, 0)), "start does not match")
				assert.True(t, result.End.Equal(time.Unix(456, 0)), "end does not match")
				require.Equal(t, len(test.expected), len(result.TagMatchers),
					"tag matchers length not match")
				for i, expected := range test.expected {
					expectedStr := expected.String()
					actualStr := result.TagMatchers[i].String()
					assert.Equal(t, expectedStr, actualStr,
						fmt.Sprintf("matcher does not match: idx=%d, expected=%s, actual=%s",
							i, expectedStr, actualStr))
				}
			}
		})
	}
}

var (
	benchResult *prompb.QueryResult
)

// BenchmarkFetchResultToPromResult-8   	     100	  10563444 ns/op	25368543 B/op	    4443 allocs/op
func BenchmarkFetchResultToPromResult(b *testing.B) {
	var (
		numSeries              = 1000
		numDatapointsPerSeries = 1000
		numTagsPerSeries       = 10
		fr                     = &FetchResult{
			SeriesList: make(ts.SeriesList, 0, numSeries),
		}
	)

	for i := 0; i < numSeries; i++ {
		values := make(ts.Datapoints, 0, numDatapointsPerSeries)
		for i := 0; i < numDatapointsPerSeries; i++ {
			values = append(values, ts.Datapoint{
				Timestamp: time.Time{},
				Value:     float64(i),
			})
		}

		tags := models.NewTags(numTagsPerSeries, nil)
		for i := 0; i < numTagsPerSeries; i++ {
			tags = tags.AddTag(models.Tag{
				Name:  []byte(fmt.Sprintf("name-%d", i)),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			})
		}

		series := ts.NewSeries(
			fmt.Sprintf("series-%d", i), values, tags)

		fr.SeriesList = append(fr.SeriesList, series)
	}

	for i := 0; i < b.N; i++ {
		benchResult = FetchResultToPromResult(fr)
	}
}

func TestIteratorToTsSeries(t *testing.T) {
	t.Run("errors on iterator error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockIter := encoding.NewMockSeriesIterator(ctrl)

		expectedErr := errors.New("expected")
		mockIter.EXPECT().Err().Return(expectedErr)

		mockIter = seriesiter.NewMockSeriesIteratorFromBase(mockIter, seriesiter.NewMockValidTagGenerator(ctrl), 1)

		dps, err := iteratorToTsSeries(mockIter, models.NewTagOptions())

		assert.Nil(t, dps)
		assert.EqualError(t, err, expectedErr.Error())
	})
}
