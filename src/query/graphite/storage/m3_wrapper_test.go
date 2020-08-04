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

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/cost"
	xctx "github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	m3ts "github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"

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
		{Type: models.MatchEqual, Name: graphite.TagName(0), Value: []byte("foo")},
		{Type: models.MatchRegexp, Name: graphite.TagName(1), Value: []byte("ba[rz]")},
		{Type: models.MatchRegexp, Name: graphite.TagName(2), Value: []byte(`q[^\.]*x`)},
		{Type: models.MatchEqual, Name: graphite.TagName(3), Value: []byte("terminator")},
		{Type: models.MatchEqual, Name: graphite.TagName(4), Value: []byte("will")},
		{Type: models.MatchEqual, Name: graphite.TagName(5), Value: []byte("be")},
		{Type: models.MatchField, Name: graphite.TagName(6)},
		{Type: models.MatchRegexp, Name: graphite.TagName(7), Value: []byte(`back[^\.]`)},
		{Type: models.MatchNotField, Name: graphite.TagName(8)},
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

	matchers, err := TranslateQueryToMatchersWithTerminator(query)
	assert.Nil(t, matchers)
	assert.Error(t, err)
}

func buildResult(
	ctrl *gomock.Controller,
	resolution time.Duration,
	size int,
	steps int,
	start time.Time,
) block.Result {
	resos := make([]int64, 0, size)
	metas := make([]block.SeriesMeta, 0, size)
	for i := 0; i < size; i++ {
		resos = append(resos, int64(resolution))
		metas = append(metas, block.SeriesMeta{Name: []byte(fmt.Sprint("a", i))})
	}

	var (
		bl = block.NewMockBlock(ctrl)
		it = block.NewMockSeriesIter(ctrl)
	)

	orderedOps := make([]*gomock.Call, 0, size*2+7)
	addOp := func(op *gomock.Call) { orderedOps = append(orderedOps, op) }
	addOp(bl.EXPECT().SeriesIter().Return(it, nil))
	addOp(it.EXPECT().SeriesMeta().Return(metas))
	for i := 0; i < size; i++ {
		addOp(it.EXPECT().Next().Return(true))
		vals := m3ts.NewFixedStepValues(resolution, steps, float64(i), start)
		c := block.NewUnconsolidatedSeries(vals.Datapoints(), metas[i], block.UnconsolidatedSeriesStats{})
		addOp(it.EXPECT().Current().Return(c))
	}

	addOp(it.EXPECT().Next().Return(false))
	addOp(it.EXPECT().Err().Return(nil))
	addOp(bl.EXPECT().Close().Return(nil))

	gomock.InOrder(orderedOps...)
	return block.Result{
		Blocks: []block.Block{bl},
		Metadata: block.ResultMetadata{
			Resolutions: resos,
		},
	}
}

func TestTranslateTimeseries(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := xctx.New()
	resolution := 10 * time.Second
	steps := 3
	start := time.Now().Truncate(resolution).Add(time.Second)
	end := start.Add(time.Duration(steps) * resolution).Add(time.Second * -2)

	// NB: truncated steps should have 1 less data point than input series since
	// the first data point is not valid.
	truncatedSteps := steps - 1
	truncated := start.Truncate(resolution).Add(resolution)
	truncatedEnd := truncated.Add(resolution * time.Duration(truncatedSteps))

	expected := 5
	result := buildResult(ctrl, resolution, expected, steps, start)
	translated, err := translateTimeseries(ctx, result, start, end)
	require.NoError(t, err)

	require.Equal(t, expected, len(translated))
	for i, tt := range translated {
		ex := make([]float64, truncatedSteps)
		for j := range ex {
			ex[j] = float64(i)
		}

		assert.Equal(t, truncated, tt.StartTime())
		assert.Equal(t, truncatedEnd, tt.EndTime())
		assert.Equal(t, ex, tt.SafeValues())
		assert.Equal(t, fmt.Sprint("a", i), tt.Name())
	}
}

func TestFetchByQuery(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	store := storage.NewMockStorage(ctrl)
	resolution := 10 * time.Second
	start := time.Now().Add(time.Hour * -1).Truncate(resolution).Add(time.Second)
	steps := 3
	res := buildResult(ctrl, resolution, 1, steps, start)
	res.Metadata = block.ResultMetadata{
		Exhaustive:  false,
		LocalOnly:   true,
		Warnings:    []block.Warning{block.Warning{Name: "foo", Message: "bar"}},
		Resolutions: []int64{int64(resolution)},
	}

	store.EXPECT().FetchBlocks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(res, nil)

	childEnforcer := cost.NewMockChainedEnforcer(ctrl)
	childEnforcer.EXPECT().Close()

	enforcer := cost.NewMockChainedEnforcer(ctrl)
	enforcer.EXPECT().Child(cost.QueryLevel).Return(childEnforcer).MinTimes(1)

	wrapper := NewM3WrappedStorage(store, enforcer, instrument.NewOptions())
	ctx := xctx.New()
	ctx.SetRequestContext(context.TODO())
	end := start.Add(time.Duration(steps) * resolution)
	opts := FetchOptions{
		StartTime: start,
		EndTime:   end,
		DataOptions: DataOptions{
			Timeout: time.Minute,
		},
	}

	query := "a*b"
	result, err := wrapper.FetchByQuery(ctx, query, opts)
	require.NoError(t, err)
	require.Equal(t, 1, len(result.SeriesList))
	series := result.SeriesList[0]
	assert.Equal(t, "a0", series.Name())
	// NB: last point is expected to be truncated.
	assert.Equal(t, []float64{0, 0}, series.SafeValues())

	assert.False(t, result.Metadata.Exhaustive)
	assert.True(t, result.Metadata.LocalOnly)
	require.Equal(t, 1, len(result.Metadata.Warnings))
	require.Equal(t, "foo_bar", result.Metadata.Warnings[0].Header())
}

func TestFetchByInvalidQuery(t *testing.T) {
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
	wrapper := NewM3WrappedStorage(store, nil, instrument.NewOptions())
	result, err := wrapper.FetchByQuery(ctx, query, opts)
	assert.NoError(t, err)
	require.Equal(t, 0, len(result.SeriesList))
}
