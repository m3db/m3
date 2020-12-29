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

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/query/block"
	xctx "github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	m3ts "github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/ts/m3db"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// alloc once so default pools are created just once
	testM3DBOpts = m3db.NewOptions()
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

	translated, err := translateQuery(query, opts, M3WrappedStorageOptions{})
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

func TestTranslateQueryStarStar(t *testing.T) {
	query := `foo**bar`
	end := time.Now()
	start := end.Add(time.Hour * -2)
	opts := FetchOptions{
		StartTime: start,
		EndTime:   end,
		DataOptions: DataOptions{
			Timeout: time.Minute,
		},
	}

	translated, err := translateQuery(query, opts, M3WrappedStorageOptions{})
	assert.NoError(t, err)
	assert.Equal(t, end, translated.End)
	assert.Equal(t, start, translated.Start)
	assert.Equal(t, time.Duration(0), translated.Interval)
	assert.Equal(t, query, translated.Raw)
	matchers := translated.TagMatchers
	expected := models.Matchers{
		{Type: models.MatchRegexp, Name: graphite.TagName(0), Value: []byte(".*")},
		{Type: models.MatchRegexp, Name: doc.IDReservedFieldName, Value: []byte("foo.*bar")},
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

	translated, err := translateQuery(query, opts, M3WrappedStorageOptions{})
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
	unconsolidatedSeries := make([]block.UnconsolidatedSeries, 0, size)
	resos := make([]time.Duration, 0, size)
	metas := make([]block.SeriesMeta, 0, size)
	for i := 0; i < size; i++ {
		resos = append(resos, resolution)
		meta := block.SeriesMeta{Name: []byte(fmt.Sprint("a", i))}
		metas = append(metas, meta)
		vals := m3ts.NewFixedStepValues(resolution, steps, float64(i), start)
		series := block.NewUnconsolidatedSeries(vals.Datapoints(),
			meta, block.UnconsolidatedSeriesStats{})
		unconsolidatedSeries = append(unconsolidatedSeries, series)
	}

	bl := block.NewMockBlock(ctrl)
	bl.EXPECT().
		SeriesIter().
		DoAndReturn(func() (block.SeriesIter, error) {
			return block.NewUnconsolidatedSeriesIter(unconsolidatedSeries), nil
		}).
		AnyTimes()
	bl.EXPECT().Close().Return(nil)

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
	translated, err := translateTimeseries(ctx, result, start, end,
		testM3DBOpts, truncateBoundsToResolutionOptions{})
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
		Resolutions: []time.Duration{resolution},
	}

	store.EXPECT().FetchBlocks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(res, nil)

	wrapper := NewM3WrappedStorage(store, testM3DBOpts,
		instrument.NewOptions(), M3WrappedStorageOptions{})
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
	wrapper := NewM3WrappedStorage(store, testM3DBOpts,
		instrument.NewOptions(), M3WrappedStorageOptions{})
	result, err := wrapper.FetchByQuery(ctx, query, opts)
	assert.NoError(t, err)
	require.Equal(t, 0, len(result.SeriesList))
}

func TestTruncateBoundsToResolution(t *testing.T) {
	var (
		resolution    = 60 * time.Second
		expectedStart = time.Date(2020, time.October, 8, 30, 51, 00, 0, time.UTC)
		expectedEnd   = time.Date(2020, time.October, 8, 30, 56, 00, 0, time.UTC)
		opts          = truncateBoundsToResolutionOptions{
			shiftStepsStartWhenAtResolutionBoundary:    intRefValue(1),
			shiftStepsEndWhenAtResolutionBoundary:      intRefValue(1),
			shiftStepsEndWhenStartAtResolutionBoundary: intRefValue(1),
		}
		tests = []struct {
			start time.Time
			end   time.Time
		}{

			{
				start: time.Date(2020, time.October, 8, 30, 50, 00, 0, time.UTC),
				end:   time.Date(2020, time.October, 8, 30, 55, 00, 0, time.UTC),
			},

			{
				start: time.Date(2020, time.October, 8, 30, 50, 00, 0, time.UTC),
				end:   time.Date(2020, time.October, 8, 30, 55, 39, 0, time.UTC),
			},
			{
				start: time.Date(2020, time.October, 8, 30, 50, 12, 0, time.UTC),
				end:   time.Date(2020, time.October, 8, 30, 55, 00, 0, time.UTC),
			},
			{
				start: time.Date(2020, time.October, 8, 30, 50, 00, 0, time.UTC),
				end:   time.Date(2020, time.October, 8, 30, 55, 39, 0, time.UTC),
			},
		}
	)

	for _, test := range tests {

		actualStart, actualEnd := truncateBoundsToResolution(test.start, test.end, resolution, opts)
		assert.Equal(t, expectedStart, actualStart)
		assert.Equal(t, expectedEnd, actualEnd)
	}
}

func TestTranslateTimeseriesWithTruncateBoundsToResolutionOptions(t *testing.T) {
	var (
		ctrl       = xtest.NewController(t)
		resolution = 60 * time.Second
		ctx        = xctx.New()
	)
	defer ctrl.Finish()

	tests := []struct {
		name                                    string
		start                                   time.Time
		end                                     time.Time
		shiftStepsStart                         int
		shiftStepsEnd                           int
		shiftStepsStartWhenAtResolutionBoundary *int
		shiftStepsEndWhenAtResolutionBoundary   *int
		renderPartialStart                      bool
		renderPartialEnd                        bool
		numDataPointsFetched                    int
		numDataPointsExpected                   int
		expectedStart                           time.Time
		expectedEnd                             time.Time
	}{
		{
			name:                  "default behavior",
			start:                 time.Date(2020, time.October, 8, 15, 0, 12, 0, time.UTC),
			end:                   time.Date(2020, time.October, 8, 15, 05, 00, 0, time.UTC),
			numDataPointsFetched:  7,
			numDataPointsExpected: 4,
			expectedStart:         time.Date(2020, time.October, 8, 15, 1, 0, 0, time.UTC),
			expectedEnd:           time.Date(2020, time.October, 8, 15, 5, 0, 0, time.UTC),
		},
		{
			name:                  "render partial start and end",
			start:                 time.Date(2020, time.October, 8, 15, 0, 12, 0, time.UTC),
			end:                   time.Date(2020, time.October, 8, 15, 05, 00, 0, time.UTC),
			renderPartialStart:    true,
			renderPartialEnd:      true,
			numDataPointsFetched:  7,
			numDataPointsExpected: 5,
			expectedStart:         time.Date(2020, time.October, 8, 15, 0, 0, 0, time.UTC),
			expectedEnd:           time.Date(2020, time.October, 8, 15, 5, 0, 0, time.UTC),
		},
		{
			name:                  "render just end",
			start:                 time.Date(2020, time.October, 8, 15, 0, 00, 0, time.UTC),
			end:                   time.Date(2020, time.October, 8, 15, 05, 27, 0, time.UTC),
			renderPartialEnd:      true,
			numDataPointsFetched:  7,
			numDataPointsExpected: 6,
			expectedStart:         time.Date(2020, time.October, 8, 15, 0, 0, 0, time.UTC),
			expectedEnd:           time.Date(2020, time.October, 8, 15, 6, 0, 0, time.UTC),
		},
		{
			name:                  "no render partial, not truncated by resolution",
			start:                 time.Date(2020, time.October, 8, 15, 0, 12, 0, time.UTC),
			end:                   time.Date(2020, time.October, 8, 15, 05, 27, 0, time.UTC),
			numDataPointsFetched:  25,
			numDataPointsExpected: 5,
			expectedStart:         time.Date(2020, time.October, 8, 15, 1, 0, 0, time.UTC),
			expectedEnd:           time.Date(2020, time.October, 8, 15, 6, 0, 0, time.UTC),
		},
		{
			name:                  "no render partial, truncated start by resolution",
			start:                 time.Date(2020, time.October, 8, 15, 0, 0, 0, time.UTC),
			end:                   time.Date(2020, time.October, 8, 15, 05, 0, 0, time.UTC),
			numDataPointsFetched:  25,
			numDataPointsExpected: 5,
			expectedStart:         time.Date(2020, time.October, 8, 15, 0, 0, 0, time.UTC),
			expectedEnd:           time.Date(2020, time.October, 8, 15, 5, 0, 0, time.UTC),
		},
		{
			name:                  "constant shift start and end",
			start:                 time.Date(2020, time.October, 8, 15, 0, 0, 0, time.UTC),
			end:                   time.Date(2020, time.October, 8, 15, 05, 5, 0, time.UTC),
			shiftStepsStart:       1,
			shiftStepsEnd:         1,
			numDataPointsFetched:  25,
			numDataPointsExpected: 5,
			expectedStart:         time.Date(2020, time.October, 8, 15, 1, 0, 0, time.UTC),
			expectedEnd:           time.Date(2020, time.October, 8, 15, 6, 0, 0, time.UTC),
		},
		{
			name:                                    "constant shift start and end + boundary shift start and end with start at boundary",
			start:                                   time.Date(2020, time.October, 8, 15, 0, 0, 0, time.UTC),
			end:                                     time.Date(2020, time.October, 8, 15, 05, 05, 0, time.UTC),
			shiftStepsStart:                         1,
			shiftStepsEnd:                           1,
			shiftStepsStartWhenAtResolutionBoundary: intRefValue(2),
			shiftStepsEndWhenAtResolutionBoundary:   intRefValue(2),
			numDataPointsFetched:                    25,
			numDataPointsExpected:                   4,
			expectedStart:                           time.Date(2020, time.October, 8, 15, 2, 0, 0, time.UTC),
			expectedEnd:                             time.Date(2020, time.October, 8, 15, 6, 0, 0, time.UTC),
		},
		{
			name:                                    "constant shift start and end + boundary shift start and end with start and end at boundary",
			start:                                   time.Date(2020, time.October, 8, 15, 0, 0, 0, time.UTC),
			end:                                     time.Date(2020, time.October, 8, 15, 6, 0, 0, time.UTC),
			shiftStepsStart:                         1,
			shiftStepsEnd:                           1,
			shiftStepsStartWhenAtResolutionBoundary: intRefValue(2),
			shiftStepsEndWhenAtResolutionBoundary:   intRefValue(2),
			numDataPointsFetched:                    25,
			numDataPointsExpected:                   6,
			expectedStart:                           time.Date(2020, time.October, 8, 15, 2, 0, 0, time.UTC),
			expectedEnd:                             time.Date(2020, time.October, 8, 15, 8, 0, 0, time.UTC),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected := 9
			result := buildResult(ctrl, resolution, expected, test.numDataPointsFetched, test.start)
			translated, err := translateTimeseries(ctx, result, test.start, test.end,
				testM3DBOpts, truncateBoundsToResolutionOptions{
					shiftStepsStart:                         test.shiftStepsStart,
					shiftStepsEnd:                           test.shiftStepsEnd,
					shiftStepsStartWhenAtResolutionBoundary: test.shiftStepsStartWhenAtResolutionBoundary,
					shiftStepsEndWhenAtResolutionBoundary:   test.shiftStepsEndWhenAtResolutionBoundary,
					renderPartialStart:                      test.renderPartialStart,
					renderPartialEnd:                        test.renderPartialEnd,
				})
			require.NoError(t, err)

			require.Equal(t, expected, len(translated))
			for i, tt := range translated {
				ex := make([]float64, test.numDataPointsExpected)
				for j := range ex {
					ex[j] = float64(i)
				}

				require.Equal(t, fmt.Sprint("a", i), tt.Name(), "unexpected name")
				require.Equal(t, ex, tt.SafeValues(), "unexpected values")
				require.Equal(t, test.expectedStart, tt.StartTime(), "unexpected start time")
				require.Equal(t, test.expectedEnd, tt.EndTime(), "unexpected end time")
			}
		})
	}
}

func intRefValue(i int) *int {
	value := i
	return &value
}
