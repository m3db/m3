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

package prom

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/prometheus"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
)

const promQuery = `http_requests_total{job="prometheus",group="canary"}`

const (
	queryParam = "query"
	startParam = "start"
	endParam   = "end"
)

var testPromQLEngineFn = func(_ time.Duration) (*promql.Engine, error) {
	return newMockPromQLEngine(), nil
}

type testHandlers struct {
	queryable          *mockQueryable
	readHandler        http.Handler
	readInstantHandler http.Handler
}

func setupTest(t *testing.T) testHandlers {
	fetchOptsBuilderCfg := handleroptions.FetchOptionsBuilderOptions{
		Timeout: 15 * time.Second,
	}
	fetchOptsBuilder, err := handleroptions.NewFetchOptionsBuilder(fetchOptsBuilderCfg)
	require.NoError(t, err)
	instrumentOpts := instrument.NewOptions()
	engineOpts := executor.NewEngineOptions().
		SetLookbackDuration(time.Minute).
		SetInstrumentOptions(instrumentOpts)
	engine := executor.NewEngine(engineOpts)
	hOpts := options.EmptyHandlerOptions().
		SetFetchOptionsBuilder(fetchOptsBuilder).
		SetEngine(engine)

	queryable := &mockQueryable{}
	readHandler, err := newReadHandler(hOpts, opts{
		queryable:  queryable,
		instant:    false,
		newQueryFn: newRangeQueryFn(testPromQLEngineFn, queryable),
	})
	require.NoError(t, err)
	readInstantHandler, err := newReadHandler(hOpts, opts{
		queryable:  queryable,
		instant:    true,
		newQueryFn: newInstantQueryFn(testPromQLEngineFn, queryable),
	})
	require.NoError(t, err)
	return testHandlers{
		queryable:          queryable,
		readHandler:        readHandler,
		readInstantHandler: readInstantHandler,
	}
}

func defaultParams() url.Values {
	vals := url.Values{}
	now := time.Now()
	vals.Add(queryParam, promQuery)
	vals.Add(startParam, now.Format(time.RFC3339))
	vals.Add(endParam, now.Add(time.Hour).Format(time.RFC3339))
	vals.Add(handleroptions.StepParam, (time.Duration(10) * time.Second).String())
	return vals
}

func defaultParamsWithoutQuery() url.Values {
	vals := url.Values{}
	now := time.Now()
	vals.Add(startParam, now.Format(time.RFC3339))
	vals.Add(endParam, now.Add(time.Hour).Format(time.RFC3339))
	vals.Add(handleroptions.StepParam, (time.Duration(10) * time.Second).String())
	return vals
}

func TestPromReadHandler(t *testing.T) {
	setup := setupTest(t)

	req, _ := http.NewRequest("GET", native.PromReadURL, nil)
	req.URL.RawQuery = defaultParams().Encode()

	recorder := httptest.NewRecorder()
	setup.readHandler.ServeHTTP(recorder, req)

	var resp response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, statusSuccess, resp.Status)
}

func TestPromReadHandlerInvalidQuery(t *testing.T) {
	setup := setupTest(t)

	req, _ := http.NewRequest("GET", native.PromReadURL, nil)
	req.URL.RawQuery = defaultParamsWithoutQuery().Encode()

	recorder := httptest.NewRecorder()
	setup.readHandler.ServeHTTP(recorder, req)

	var resp response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, statusError, resp.Status)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestPromReadHandlerErrors(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		httpCode int
	}{
		{
			name:     "prom error",
			err:      fmt.Errorf("prom error"),
			httpCode: http.StatusBadRequest,
		},
		{
			name:     "prom timeout",
			err:      promql.ErrQueryTimeout("timeout"),
			httpCode: http.StatusGatewayTimeout,
		},
		{
			name:     "prom cancel",
			err:      promql.ErrQueryCanceled("cancel"),
			httpCode: 499,
		},
		{
			name:     "storage 500",
			err:      prometheus.NewStorageErr(fmt.Errorf("500 storage error")),
			httpCode: http.StatusInternalServerError,
		},
		{
			name:     "storage 400",
			err:      prometheus.NewStorageErr(xerrors.NewInvalidParamsError(fmt.Errorf("400 storage error"))),
			httpCode: http.StatusBadRequest,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			setup := setupTest(t)
			setup.queryable.selectFn = func(
				sortSeries bool,
				hints *promstorage.SelectHints,
				labelMatchers ...*labels.Matcher,
			) promstorage.SeriesSet {
				return promstorage.ErrSeriesSet(tc.err)
			}

			req, _ := http.NewRequestWithContext(context.Background(), "GET", native.PromReadURL, nil)
			req.URL.RawQuery = defaultParams().Encode()

			recorder := httptest.NewRecorder()
			setup.readHandler.ServeHTTP(recorder, req)

			var resp response
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
			require.Equal(t, statusError, resp.Status)
			require.Equal(t, tc.httpCode, recorder.Code)
		})
	}
}

func TestPromReadInstantHandler(t *testing.T) {
	setup := setupTest(t)

	req, _ := http.NewRequest("GET", native.PromReadInstantURL, nil)
	req.URL.RawQuery = defaultParams().Encode()

	recorder := httptest.NewRecorder()
	setup.readInstantHandler.ServeHTTP(recorder, req)

	var resp response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, statusSuccess, resp.Status)
}

func TestPromReadInstantHandlerInvalidQuery(t *testing.T) {
	setup := setupTest(t)

	req, _ := http.NewRequest("GET", native.PromReadInstantURL, nil)
	req.URL.RawQuery = defaultParamsWithoutQuery().Encode()

	recorder := httptest.NewRecorder()
	setup.readInstantHandler.ServeHTTP(recorder, req)

	var resp response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, statusError, resp.Status)
}

func TestPromReadInstantHandlerParseMinTime(t *testing.T) {
	setup := setupTest(t)

	var (
		query   *promstorage.SelectHints
		selects int
	)
	setup.queryable.selectFn = func(
		sortSeries bool,
		hints *promstorage.SelectHints,
		labelMatchers ...*labels.Matcher,
	) promstorage.SeriesSet {
		selects++
		query = hints
		return &mockSeriesSet{}
	}

	req, _ := http.NewRequest("GET", native.PromReadInstantURL, nil)
	params := defaultParams()
	params.Set("time", minTimeFormatted)
	req.URL.RawQuery = params.Encode()

	var resp response
	recorder := httptest.NewRecorder()

	setup.readInstantHandler.ServeHTTP(recorder, req)

	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, statusSuccess, resp.Status)

	require.Equal(t, 1, selects)

	fudge := 5 * time.Minute // Need to account for lookback
	expected := time.Unix(0, 0)
	actual := millisTime(query.Start)
	require.True(t, abs(expected.Sub(actual)) <= fudge,
		fmt.Sprintf("expected=%v, actual=%v, fudge=%v, delta=%v",
			expected, actual, fudge, expected.Sub(actual)))

	fudge = 5 * time.Minute // Need to account for lookback
	expected = time.Unix(0, 0)
	actual = millisTime(query.Start)
	require.True(t, abs(expected.Sub(actual)) <= fudge,
		fmt.Sprintf("expected=%v, actual=%v, fudge=%v, delta=%v",
			expected, actual, fudge, expected.Sub(actual)))
}

func TestPromReadInstantHandlerParseMaxTime(t *testing.T) {
	setup := setupTest(t)

	var (
		query   *promstorage.SelectHints
		selects int
	)
	setup.queryable.selectFn = func(
		sortSeries bool,
		hints *promstorage.SelectHints,
		labelMatchers ...*labels.Matcher,
	) promstorage.SeriesSet {
		selects++
		query = hints
		return &mockSeriesSet{}
	}

	req, _ := http.NewRequest("GET", native.PromReadInstantURL, nil)
	params := defaultParams()
	params.Set("time", maxTimeFormatted)
	req.URL.RawQuery = params.Encode()

	var resp response
	recorder := httptest.NewRecorder()

	setup.readInstantHandler.ServeHTTP(recorder, req)

	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, statusSuccess, resp.Status)

	require.Equal(t, 1, selects)

	fudge := 6 * time.Minute // Need to account for lookback + time.Now() skew
	expected := time.Now()
	actual := millisTime(query.Start)
	require.True(t, abs(expected.Sub(actual)) <= fudge,
		fmt.Sprintf("expected=%v, actual=%v, fudge=%v, delta=%v",
			expected, actual, fudge, expected.Sub(actual)))

	fudge = 6 * time.Minute // Need to account for lookback + time.Now() skew
	expected = time.Now()
	actual = millisTime(query.Start)
	require.True(t, abs(expected.Sub(actual)) <= fudge,
		fmt.Sprintf("expected=%v, actual=%v, fudge=%v, delta=%v",
			expected, actual, fudge, expected.Sub(actual)))
}

func TestLimitedReturnedDataVector(t *testing.T) {
	handler := &readHandler{
		logger: zap.NewNop(),
	}

	r := &promql.Result{
		Value: promql.Vector{
			{Point: promql.Point{T: 1, V: 1.0}},
			{Point: promql.Point{T: 2, V: 2.0}},
			{Point: promql.Point{T: 3, V: 3.0}},
		},
	}

	tests := []struct {
		name                string
		maxSeries           int
		maxDatapoints       int
		expectedSeries      int
		expectedTotalSeries int
		expectedDatapoints  int
		expectedLimited     bool
	}{
		{
			name:            "Omit limits",
			expectedLimited: false,
		},
		{
			name:            "Series below max",
			maxSeries:       4,
			expectedLimited: false,
		},
		{
			name:            "Series at max",
			maxSeries:       3,
			expectedLimited: false,
		},
		{
			name:                "Series above max",
			maxSeries:           2,
			expectedLimited:     true,
			expectedSeries:      2,
			expectedTotalSeries: 3,
			expectedDatapoints:  2,
		},
		{
			name:            "Datapoints below max",
			maxDatapoints:   4,
			expectedLimited: false,
		},
		{
			name:            "Datapoints at max",
			maxDatapoints:   3,
			expectedLimited: false,
		},
		{
			name:                "Datapoints above max",
			maxDatapoints:       2,
			expectedLimited:     true,
			expectedSeries:      2,
			expectedTotalSeries: 3,
			expectedDatapoints:  2,
		},
		{
			name:                "Series and datapoints limit (former lower)",
			maxSeries:           1,
			maxDatapoints:       2,
			expectedLimited:     true,
			expectedSeries:      1,
			expectedTotalSeries: 3,
			expectedDatapoints:  1,
		},
		{
			name:                "Series and datapoints limit (former higher)",
			maxSeries:           2,
			maxDatapoints:       1,
			expectedLimited:     true,
			expectedSeries:      1,
			expectedTotalSeries: 3,
			expectedDatapoints:  1,
		},
		{
			name:                "Series and datapoints limit (only one under limit)",
			maxSeries:           1,
			maxDatapoints:       10,
			expectedLimited:     true,
			expectedSeries:      1,
			expectedTotalSeries: 3,
			expectedDatapoints:  1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := *r
			limited := handler.limitReturnedData("", &result, &storage.FetchOptions{
				ReturnedSeriesLimit:     test.maxSeries,
				ReturnedDatapointsLimit: test.maxDatapoints,
			})
			require.Equal(t, test.expectedLimited, limited.Limited)

			seriesCount := len(result.Value.(promql.Vector))

			if limited.Limited {
				require.Equal(t, test.expectedSeries, seriesCount)
				require.Equal(t, test.expectedSeries, limited.Series)
				require.Equal(t, test.expectedTotalSeries, limited.TotalSeries)
				require.Equal(t, test.expectedDatapoints, limited.Datapoints)
			} else {
				// Full results
				require.Equal(t, 3, seriesCount)
			}
		})
	}
}

func TestLimitedReturnedDataMatrix(t *testing.T) {
	handler := &readHandler{
		logger: zap.NewNop(),
	}

	r := &promql.Result{
		Value: promql.Matrix{
			{Points: []promql.Point{
				{T: 1, V: 1.0},
			}},
			{Points: []promql.Point{
				{T: 1, V: 1.0},
				{T: 2, V: 2.0},
			}},
			{Points: []promql.Point{
				{T: 1, V: 1.0},
				{T: 2, V: 2.0},
				{T: 3, V: 3.0},
			}},
		},
	}

	tests := []struct {
		name                string
		maxSeries           int
		maxDatapoints       int
		expectedSeries      int
		expectedTotalSeries int
		expectedDatapoints  int
		expectedLimited     bool
	}{
		{
			name:            "Omit limits",
			expectedLimited: false,
		},
		{
			name:            "Series below max",
			maxSeries:       4,
			expectedLimited: false,
		},
		{
			name:            "Series at max",
			maxSeries:       3,
			expectedLimited: false,
		},
		{
			name:                "Series above max",
			maxSeries:           2,
			expectedLimited:     true,
			expectedSeries:      2,
			expectedTotalSeries: 3,
			expectedDatapoints:  3,
		},
		{
			name:            "Datapoints below max",
			maxDatapoints:   7,
			expectedLimited: false,
		},
		{
			name:            "Datapoints at max",
			maxDatapoints:   6,
			expectedLimited: false,
		},
		{
			name:                "Datapoints above max - 2 series left - A",
			maxDatapoints:       5,
			expectedLimited:     true,
			expectedSeries:      2,
			expectedTotalSeries: 3,
			expectedDatapoints:  3,
		},
		{
			name:                "Datapoints above max - 2 series left - B",
			maxDatapoints:       3,
			expectedLimited:     true,
			expectedSeries:      2,
			expectedTotalSeries: 3,
			expectedDatapoints:  3,
		},
		{
			name:                "Datapoints above max - 1 series left - A",
			maxDatapoints:       2,
			expectedLimited:     true,
			expectedSeries:      1,
			expectedTotalSeries: 3,
			expectedDatapoints:  1,
		},
		{
			name:                "Datapoints above max - 1 series left - B",
			maxDatapoints:       1,
			expectedLimited:     true,
			expectedSeries:      1,
			expectedTotalSeries: 3,
			expectedDatapoints:  1,
		},
		{
			name:                "Series and datapoints limit (former lower)",
			maxSeries:           1,
			maxDatapoints:       6,
			expectedLimited:     true,
			expectedSeries:      1,
			expectedTotalSeries: 3,
			expectedDatapoints:  1,
		},
		{
			name:                "Series and datapoints limit (former higher)",
			maxSeries:           6,
			maxDatapoints:       1,
			expectedLimited:     true,
			expectedSeries:      1,
			expectedTotalSeries: 3,
			expectedDatapoints:  1,
		},
		{
			name:                "Series and datapoints limit (only one under limit)",
			maxSeries:           1,
			maxDatapoints:       10,
			expectedLimited:     true,
			expectedSeries:      1,
			expectedTotalSeries: 3,
			expectedDatapoints:  1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := *r
			limited := handler.limitReturnedData("", &result, &storage.FetchOptions{
				ReturnedSeriesLimit:     test.maxSeries,
				ReturnedDatapointsLimit: test.maxDatapoints,
			})
			require.Equal(t, test.expectedLimited, limited.Limited)

			m := result.Value.(promql.Matrix)
			seriesCount := len(m)
			datapointCount := 0
			for _, d := range m {
				datapointCount += len(d.Points)
			}

			if limited.Limited {
				require.Equal(t, test.expectedSeries, seriesCount, "series count")
				require.Equal(t, test.expectedDatapoints, datapointCount, "datapoint count")
				require.Equal(t, test.expectedSeries, limited.Series, "series")
				require.Equal(t, test.expectedTotalSeries, limited.TotalSeries, "total series")
				require.Equal(t, test.expectedDatapoints, limited.Datapoints, "datapoints")
			} else {
				// Full results
				require.Equal(t, 3, seriesCount, "expect full series")
				require.Equal(t, 6, datapointCount, "expect full datapoints")
			}
		})
	}
}

func abs(v time.Duration) time.Duration {
	if v < 0 {
		return v * -1
	}
	return v
}

func millisTime(timestampMilliseconds int64) time.Time {
	return time.Unix(0, timestampMilliseconds*int64(time.Millisecond))
}
