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
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func verifyExpandSeries(t *testing.T, ctrl *gomock.Controller, num int, pools pool.ObjectPool) {
	testTags := seriesiter.GenerateTag()
	iters := seriesiter.NewMockSeriesIters(ctrl, testTags, num, 2)

	results, err := SeriesIteratorsToFetchResult(iters, pools, true)
	assert.NoError(t, err)

	require.NotNil(t, results)
	require.NotNil(t, results.SeriesList)
	require.Len(t, results.SeriesList, num)
	expectedTags := models.Tags{{Name: testTags.Name.String(), Value: testTags.Value.String()}}
	for i := 0; i < num; i++ {
		series := results.SeriesList[i]
		require.NotNil(t, series)
		require.Equal(t, expectedTags, series.Tags)
	}
}

func testExpandSeries(t *testing.T, pools pool.ObjectPool) {
	ctrl := gomock.NewController(t)

	for i := 0; i < 100; i++ {
		verifyExpandSeries(t, ctrl, i, pools)
	}
}

func TestExpandSeriesNilPools(t *testing.T) {
	testExpandSeries(t, nil)
}

func TestExpandSeriesInvalidPoolType(t *testing.T) {
	objectPool := pool.NewObjectPool(pool.NewObjectPoolOptions())
	objectPool.Init(func() interface{} {
		return ""
	})
	testExpandSeries(t, objectPool)
}

func TestExpandSeriesValidPools(t *testing.T) {
	objectPool := pool.NewObjectPool(pool.NewObjectPoolOptions())
	objectPool.Init(func() interface{} {
		workerPool := xsync.NewWorkerPool(100)
		workerPool.Init()
		return workerPool
	})
	testExpandSeries(t, objectPool)
}

func TestExpandSeriesSmallValidPools(t *testing.T) {
	objectPool := pool.NewObjectPool(pool.NewObjectPoolOptions())
	objectPool.Init(func() interface{} {
		workerPool := xsync.NewWorkerPool(2)
		workerPool.Init()
		return workerPool
	})
	testExpandSeries(t, objectPool)
}

func TestFailingExpandSeriesValidPools(t *testing.T) {
	objectPool := pool.NewObjectPool(pool.NewObjectPoolOptions())
	objectPool.Init(func() interface{} {
		workerPool := xsync.NewWorkerPool(2)
		workerPool.Init()
		return workerPool
	})
	ctrl := gomock.NewController(t)
	testTags := seriesiter.GenerateTag()
	validTagGenerator := func() ident.TagIterator {
		return seriesiter.GenerateSingleSampleTagIterator(ctrl, testTags)
	}
	iters := seriesiter.NewMockSeriesIterSlice(ctrl, validTagGenerator, 4, 2)
	invalidIters := make([]encoding.SeriesIterator, 2)
	for i := 0; i < 2; i++ {
		invalidIter := encoding.NewMockSeriesIterator(ctrl)
		invalidIter.EXPECT().ID().Return(ident.StringID("foo")).Times(1)

		tags := ident.NewMockTagIterator(ctrl)
		tags.EXPECT().Next().Return(false).MaxTimes(1)
		tags.EXPECT().Remaining().Return(0).MaxTimes(1)
		tags.EXPECT().Err().Return(errors.New("error")).MaxTimes(1)
		invalidIter.EXPECT().Tags().Return(tags).MaxTimes(1)

		invalidIters[i] = invalidIter
	}
	iters = append(iters, invalidIters...)
	for i := 0; i < 10; i++ {
		uncalledIter := encoding.NewMockSeriesIterator(ctrl)
		iters = append(iters, uncalledIter)
	}

	mockIters := encoding.NewMockSeriesIterators(ctrl)
	mockIters.EXPECT().Iters().Return(iters).Times(1)
	mockIters.EXPECT().Len().Return(len(iters)).Times(1)
	mockIters.EXPECT().Close().Times(1)

	result, err := SeriesIteratorsToFetchResult(mockIters, objectPool, true)
	require.Nil(t, result)
	require.EqualError(t, err, "error")
}

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
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchEqual, Name: "foo", Value: "bar"},
			},
		},
		{
			name: "single exact match negated",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_NEQ, Name: "foo", Value: "bar"},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchNotEqual, Name: "foo", Value: "bar"},
			},
		},
		{
			name: "single regexp match",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_RE, Name: "foo", Value: "bar"},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchRegexp, Name: "foo", Value: "bar"},
			},
		},
		{
			name: "single regexp match negated",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_NRE, Name: "foo", Value: "bar"},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchNotRegexp, Name: "foo", Value: "bar"},
			},
		},
		{
			name: "mixed exact match and regexp match",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_RE, Name: "baz", Value: "qux"},
			},
			expected: []*models.Matcher{
				&models.Matcher{Type: models.MatchEqual, Name: "foo", Value: "bar"},
				&models.Matcher{Type: models.MatchRegexp, Name: "baz", Value: "qux"},
			},
		},
		{
			name: "unrecognized matcher type",
			matchers: []*prompb.LabelMatcher{
				&prompb.LabelMatcher{Type: prompb.LabelMatcher_Type(math.MaxInt32), Name: "foo", Value: "bar"},
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

		tags := make(models.Tags, 0, numTagsPerSeries)
		for i := 0; i < numTagsPerSeries; i++ {
			tags = append(tags, models.Tag{
				Name:  fmt.Sprintf("name-%d", i),
				Value: fmt.Sprintf("value-%d", i),
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
