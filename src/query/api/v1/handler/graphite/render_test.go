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

package graphite

import (
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/graphite/graphite"
	graphiteStorage "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/headers"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testHandlerOptions(t *testing.T) options.HandlerOptions {
	fetchOptsBuilder, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{
			Timeout: 15 * time.Second,
		})
	require.NoError(t, err)

	return options.EmptyHandlerOptions().
		SetQueryContextOptions(models.QueryContextOptions{}).
		SetGraphiteFindFetchOptionsBuilder(fetchOptsBuilder).
		SetGraphiteRenderFetchOptionsBuilder(fetchOptsBuilder)
}

func makeBlockResult(
	ctrl *gomock.Controller,
	results *storage.FetchResult,
) block.Result {
	size := len(results.SeriesList)
	unconsolidatedSeries := make([]block.UnconsolidatedSeries, 0, size)
	metas := make([]block.SeriesMeta, 0, size)
	for _, elem := range results.SeriesList {
		meta := block.SeriesMeta{Name: elem.Name()}
		series := block.NewUnconsolidatedSeries(elem.Values().Datapoints(),
			meta, block.UnconsolidatedSeriesStats{})
		unconsolidatedSeries = append(unconsolidatedSeries, series)
		metas = append(metas, meta)
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
		Blocks:   []block.Block{bl},
		Metadata: results.Metadata,
	}
}

func TestParseNoQuery(t *testing.T) {
	mockStorage := mock.NewMockStorage()

	opts := testHandlerOptions(t).SetStorage(mockStorage)
	handler := NewRenderHandler(opts)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, newGraphiteReadHTTPRequest(t))

	res := recorder.Result()
	require.Equal(t, 400, res.StatusCode)
}

func TestParseQueryNoResults(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	store := storage.NewMockStorage(ctrl)
	blockResult := makeBlockResult(ctrl, &storage.FetchResult{})
	store.EXPECT().FetchBlocks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(blockResult, nil)

	opts := testHandlerOptions(t).SetStorage(store)
	handler := NewRenderHandler(opts)

	req := newGraphiteReadHTTPRequest(t)
	req.URL.RawQuery = "target=foo.bar&from=-2h&until=now"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	require.Equal(t, 200, res.StatusCode)

	buf, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, []byte("[]"), buf)
}

func TestParseQueryResults(t *testing.T) {
	resolution := 10 * time.Second
	truncateStart := xtime.Now().Add(-30 * time.Minute).Truncate(resolution)
	start := truncateStart.Add(time.Second)
	vals := ts.NewFixedStepValues(resolution, 3, 3, start)
	tags := models.NewTags(0, nil)
	tags = tags.AddTag(models.Tag{Name: graphite.TagName(0), Value: []byte("foo")})
	tags = tags.AddTag(models.Tag{Name: graphite.TagName(1), Value: []byte("bar")})
	seriesList := ts.SeriesList{
		ts.NewSeries([]byte("series_name"), vals, tags),
	}

	meta := block.NewResultMetadata()
	meta.Resolutions = []time.Duration{resolution}
	fr := &storage.FetchResult{
		SeriesList: seriesList,
		Metadata:   meta,
	}

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	store := storage.NewMockStorage(ctrl)
	blockResult := makeBlockResult(ctrl, fr)
	store.EXPECT().FetchBlocks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(blockResult, nil)

	opts := testHandlerOptions(t).SetStorage(store)
	handler := NewRenderHandler(opts)

	req := newGraphiteReadHTTPRequest(t)
	req.URL.RawQuery = fmt.Sprintf("target=foo.bar&from=%d&until=%d",
		start.ToTime().Unix(), start.ToTime().Unix()+30)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	assert.Equal(t, 200, res.StatusCode)

	buf, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	exTimestamp := truncateStart.ToTime().Unix() + 10
	expected := fmt.Sprintf(
		`[{"target":"series_name","datapoints":[[3.000000,%d],`+
			`[3.000000,%d],[null,%d]],"step_size_ms":%d}]`,
		exTimestamp, exTimestamp+10, exTimestamp+20, resolution/time.Millisecond)

	require.Equal(t, expected, string(buf))
}

func TestParseQueryResultsMaxDatapoints(t *testing.T) {
	startStr := "03/07/14"
	endStr := "03/07/15"
	start, err := graphite.ParseTime(startStr, time.Now(), 0)
	require.NoError(t, err)
	end, err := graphite.ParseTime(endStr, time.Now(), 0)
	require.NoError(t, err)

	resolution := 10 * time.Second
	vals := ts.NewFixedStepValues(resolution, 4, 4, xtime.ToUnixNano(start))
	seriesList := ts.SeriesList{
		ts.NewSeries([]byte("a"), vals, models.NewTags(0, nil)),
	}

	meta := block.NewResultMetadata()
	meta.Resolutions = []time.Duration{resolution}
	fr := &storage.FetchResult{
		SeriesList: seriesList,
		Metadata:   meta,
	}

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	store := storage.NewMockStorage(ctrl)
	blockResult := makeBlockResult(ctrl, fr)
	store.EXPECT().FetchBlocks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(blockResult, nil)

	opts := testHandlerOptions(t).SetStorage(store)
	handler := NewRenderHandler(opts)

	req := newGraphiteReadHTTPRequest(t)
	req.URL.RawQuery = fmt.Sprintf(
		"target=foo.bar&from=%s&until=%s&maxDataPoints=1",
		startStr, endStr,
	)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	require.Equal(t, 200, res.StatusCode)

	buf, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)

	// Expected resolution should be in milliseconds and subsume all datapoints.
	exStep := end.Sub(start) / time.Millisecond
	expected := fmt.Sprintf(
		`[{"target":"a","datapoints":[[4.000000,%d]],"step_size_ms":%d}]`,
		start.Unix(), exStep)

	require.Equal(t, expected, string(buf))
}

func TestParseQueryResultsMultiTarget(t *testing.T) {
	minsAgo := 12
	resolution := 10 * time.Second
	start := time.Now().
		Add(-1 * time.Duration(minsAgo) * time.Minute).
		Truncate(resolution)

	vals := ts.NewFixedStepValues(resolution, 3, 3, xtime.ToUnixNano(start))
	seriesList := ts.SeriesList{
		ts.NewSeries([]byte("a"), vals, models.NewTags(0, nil)),
	}

	meta := block.NewResultMetadata()
	meta.Resolutions = []time.Duration{resolution}
	fr := &storage.FetchResult{
		SeriesList: seriesList,
		Metadata:   meta,
	}

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	store := storage.NewMockStorage(ctrl)
	store.EXPECT().FetchBlocks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makeBlockResult(ctrl, fr), nil)
	store.EXPECT().FetchBlocks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makeBlockResult(ctrl, fr), nil)

	opts := testHandlerOptions(t).SetStorage(store)
	handler := NewRenderHandler(opts)

	req := newGraphiteReadHTTPRequest(t)
	req.URL.RawQuery = fmt.Sprintf(
		"target=foo.bar&target=baz.qux&from=%d&until=%d",
		start.Unix(), start.Unix()+30,
	)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	require.Equal(t, 200, res.StatusCode)

	buf, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)

	expected := fmt.Sprintf(
		`[{"target":"a","datapoints":[[3.000000,%d],`+
			`[3.000000,%d],[3.000000,%d]],"step_size_ms":%d},`+
			`{"target":"a","datapoints":[[3.000000,%d],`+
			`[3.000000,%d],[3.000000,%d]],"step_size_ms":%d}]`,
		start.Unix(), start.Unix()+10, start.Unix()+20, resolution/time.Millisecond,
		start.Unix(), start.Unix()+10, start.Unix()+20, resolution/time.Millisecond)

	require.Equal(t, expected, string(buf))
}

func TestParseQueryResultsMultiTargetWithLimits(t *testing.T) {
	for _, tt := range limitTests {
		t.Run(tt.name, func(t *testing.T) {
			minsAgo := 12
			start := time.Now().Add(-1 * time.Duration(minsAgo) * time.Minute)
			resolution := 10 * time.Second
			vals := ts.NewFixedStepValues(resolution, 3, 3, xtime.ToUnixNano(start))
			seriesList := ts.SeriesList{
				ts.NewSeries([]byte("a"), vals, models.NewTags(0, nil)),
			}

			meta := block.NewResultMetadata()
			meta.Resolutions = []time.Duration{resolution}
			meta.Exhaustive = tt.ex
			frOne := &storage.FetchResult{SeriesList: seriesList, Metadata: meta}

			metaTwo := block.NewResultMetadata()
			metaTwo.Resolutions = []time.Duration{resolution}
			if !tt.ex2 {
				metaTwo.AddWarning("foo", "bar")
			}

			frTwo := &storage.FetchResult{SeriesList: seriesList, Metadata: metaTwo}

			ctrl := xtest.NewController(t)
			defer ctrl.Finish()

			store := storage.NewMockStorage(ctrl)
			store.EXPECT().FetchBlocks(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(makeBlockResult(ctrl, frOne), nil)
			store.EXPECT().FetchBlocks(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(makeBlockResult(ctrl, frTwo), nil)

			opts := testHandlerOptions(t).SetStorage(store)
			handler := NewRenderHandler(opts)

			req := newGraphiteReadHTTPRequest(t)
			req.URL.RawQuery = fmt.Sprintf(
				"target=foo.bar&target=bar.baz&from=%d&until=%d",
				start.Unix(), start.Unix()+30,
			)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)

			actual := recorder.Header().Get(headers.LimitHeader)
			assert.Equal(t, tt.header, actual)
		})
	}
}

func TestParseQueryResultsAllNaN(t *testing.T) {
	resolution := 10 * time.Second
	truncateStart := time.Now().Add(-30 * time.Minute).Truncate(resolution)
	start := truncateStart.Add(time.Second)
	vals := ts.NewFixedStepValues(resolution, 3, math.NaN(), xtime.ToUnixNano(start))
	tags := models.NewTags(0, nil)
	seriesList := ts.SeriesList{
		ts.NewSeries([]byte("series_name"), vals, tags),
	}

	meta := block.NewResultMetadata()
	meta.Resolutions = []time.Duration{resolution}
	fr := &storage.FetchResult{
		SeriesList: seriesList,
		Metadata:   meta,
	}

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	store := storage.NewMockStorage(ctrl)
	blockResult := makeBlockResult(ctrl, fr)
	store.EXPECT().FetchBlocks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(blockResult, nil)

	graphiteStorageOpts := graphiteStorage.M3WrappedStorageOptions{
		RenderSeriesAllNaNs: true,
	}
	opts := testHandlerOptions(t).
		SetStorage(store).
		SetGraphiteStorageOptions(graphiteStorageOpts)
	handler := NewRenderHandler(opts)

	req := newGraphiteReadHTTPRequest(t)
	req.URL.RawQuery = fmt.Sprintf("target=foo.bar&from=%d&until=%d",
		start.Unix(), start.Unix()+30)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	assert.Equal(t, 200, res.StatusCode)

	buf, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	exTimestamp := truncateStart.Unix() + 10
	expected := fmt.Sprintf(
		`[{"target":"series_name","datapoints":[[null,%d],`+
			`[null,%d],[null,%d]],"step_size_ms":%d}]`,
		exTimestamp, exTimestamp+10, exTimestamp+20, resolution/time.Millisecond)

	require.Equal(t, expected, string(buf))
}

func newGraphiteReadHTTPRequest(t *testing.T) *http.Request {
	req, err := http.NewRequest(ReadHTTPMethods[0], ReadURL, nil)
	require.NoError(t, err)
	return req
}
