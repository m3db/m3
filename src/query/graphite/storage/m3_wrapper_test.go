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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/cost"
	xctx "github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	m3ts "github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTranslateQuery(t *testing.T) {
	query := `foo.ba[rz].q*x.terminator.will.be.*.back?`
	end := time.Now()
	start := end.Add(time.Hour * -2)
	opts := FetchOptions{
		StartTime: start,
		EndTime:   end,
		DataOptions: DataOptions{
			Timeout: time.Minute,
		},
	}

	translated, err := translateQuery(query, opts)
	assert.NoError(t, err)
	assert.Equal(t, end, translated.End)
	assert.Equal(t, start, translated.Start)
	assert.Equal(t, time.Duration(0), translated.Interval)
	assert.Equal(t, query, translated.Raw)
	matchers := translated.TagMatchers
	expected := models.Matchers{
		{Type: models.MatchRegexp, Name: graphite.TagName(0), Value: []byte("foo")},
		{Type: models.MatchRegexp, Name: graphite.TagName(1), Value: []byte("ba[rz]")},
		{Type: models.MatchRegexp, Name: graphite.TagName(2), Value: []byte("q.*x")},
		{Type: models.MatchRegexp, Name: graphite.TagName(3), Value: []byte("terminator")},
		{Type: models.MatchRegexp, Name: graphite.TagName(4), Value: []byte("will")},
		{Type: models.MatchRegexp, Name: graphite.TagName(5), Value: []byte("be")},
		{Type: models.MatchRegexp, Name: graphite.TagName(6), Value: []byte(".*")},
		{Type: models.MatchRegexp, Name: graphite.TagName(7), Value: []byte("back?")},
		{Type: models.MatchNotRegexp, Name: graphite.TagName(8), Value: []byte(".*")},
	}

	assert.Equal(t, expected, matchers)
}

func TestTranslateQueryTrailingDot(t *testing.T) {
	query := `foo.`
	end := time.Now()
	start := end.Add(time.Hour * -2)
	opts := FetchOptions{
		StartTime: start,
		EndTime:   end,
		DataOptions: DataOptions{
			Timeout: time.Minute,
		},
	}

	translated, err := translateQuery(query, opts)
	assert.Nil(t, translated)
	assert.Error(t, err)

	matchers, err := TranslateQueryToMatchers(query)
	assert.Nil(t, matchers)
	assert.Error(t, err)
}

func TestTranslateTimeseries(t *testing.T) {
	ctx := xctx.New()
	resolution := 10 * time.Second
	steps := 1
	start := time.Now()
	end := start.Add(time.Duration(steps) * resolution)
	expected := 5
	seriesList := make(m3ts.SeriesList, expected)
	for i := 0; i < expected; i++ {
		vals := m3ts.NewFixedStepValues(resolution, steps, float64(i), start)
		series := m3ts.NewSeries([]byte(fmt.Sprint("a", i)),
			vals, models.NewTags(0, nil))
		series.SetResolution(resolution)
		seriesList[i] = series
	}

	translated, err := translateTimeseries(ctx, seriesList, start, end)
	require.NoError(t, err)

	require.Equal(t, expected, len(translated))
	for i, tt := range translated {
		ex := []float64{float64(i)}
		assert.Equal(t, ex, tt.SafeValues())
		assert.Equal(t, fmt.Sprint("a", i), tt.Name())
	}
}

func TestTranslateTimeseriesWithTags(t *testing.T) {
	ctx := xctx.New()
	resolution := 10 * time.Second
	steps := 1
	start := time.Now()
	end := start.Add(time.Duration(steps) * resolution)
	expected := 5
	seriesList := make(m3ts.SeriesList, expected)
	for i := 0; i < expected; i++ {
		vals := m3ts.NewFixedStepValues(resolution, steps, float64(i), start)
		series := m3ts.NewSeries([]byte(fmt.Sprint("a", i)), vals,
			models.NewTags(0, nil))
		series.SetResolution(resolution)
		seriesList[i] = series
	}

	translated, err := translateTimeseries(ctx, seriesList, start, end)
	require.NoError(t, err)

	require.Equal(t, expected, len(translated))
	for i, tt := range translated {
		ex := []float64{float64(i)}
		assert.Equal(t, ex, tt.SafeValues())
		assert.Equal(t, fmt.Sprint("a", i), tt.Name())
	}
}

func TestFetchByQuery(t *testing.T) {
	store := mock.NewMockStorage()
	start := time.Now().Add(time.Hour * -1)
	resolution := 10 * time.Second
	steps := 3
	vals := m3ts.NewFixedStepValues(resolution, steps, 3, start)
	seriesList := m3ts.SeriesList{
		m3ts.NewSeries([]byte("a"), vals, models.NewTags(0, nil)),
	}
	for _, series := range seriesList {
		series.SetResolution(resolution)
	}

	store.SetFetchResult(&storage.FetchResult{SeriesList: seriesList}, nil)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	childEnforcer := cost.NewMockChainedEnforcer(ctrl)
	childEnforcer.EXPECT().Close()

	enforcer := cost.NewMockChainedEnforcer(ctrl)
	enforcer.EXPECT().Child(cost.QueryLevel).Return(childEnforcer).MinTimes(1)

	wrapper := NewM3WrappedStorage(store, enforcer)
	ctx := xctx.New()
	ctx.SetRequestContext(context.TODO())
	end := time.Now()
	opts := FetchOptions{
		StartTime: start,
		EndTime:   end,
		DataOptions: DataOptions{
			Timeout: time.Minute,
		},
	}

	query := "a*b"
	result, err := wrapper.FetchByQuery(ctx, query, opts)
	assert.NoError(t, err)
	require.Equal(t, 1, len(result.SeriesList))
	series := result.SeriesList[0]
	assert.Equal(t, "a", series.Name())
	assert.Equal(t, []float64{3, 3, 3}, series.SafeValues())

	// NB: ensure the fetch was called with the base enforcer's child correctly
	assert.Equal(t, childEnforcer, store.LastFetchOptions().Enforcer)
}

func TestFetchByInvalidQuery(t *testing.T) {
	logging.InitWithCores(nil)
	store := mock.NewMockStorage()
	start := time.Now().Add(time.Hour * -1)
	end := time.Now()
	opts := FetchOptions{
		StartTime: start,
		EndTime:   end,
		DataOptions: DataOptions{
			Timeout: time.Minute,
		},
	}

	query := "a."
	ctx := xctx.New()
	wrapper := NewM3WrappedStorage(store, nil)
	result, err := wrapper.FetchByQuery(ctx, query, opts)
	assert.NoError(t, err)
	require.Equal(t, 0, len(result.SeriesList))
}
