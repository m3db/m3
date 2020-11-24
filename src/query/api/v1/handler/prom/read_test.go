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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/executor"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/prometheus/prometheus/pkg/labels"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

const promQuery = `http_requests_total{job="prometheus",group="canary"}`

const (
	queryParam = "query"
	startParam = "start"
	endParam   = "end"
)

var testPromQLEngine = newMockPromQLEngine()

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
		promQLEngine: testPromQLEngine,
		queryable:    queryable,
		instant:      false,
		newQueryFn:   newRangeQueryFn(testPromQLEngine, queryable),
	})
	require.NoError(t, err)
	readInstantHandler, err := newReadHandler(hOpts, opts{
		promQLEngine: testPromQLEngine,
		queryable:    queryable,
		instant:      true,
		newQueryFn:   newInstantQueryFn(testPromQLEngine, queryable),
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

func TestPromReadHandlerExecuteInvalidParamsError(t *testing.T) {
	setup := setupTest(t)
	setup.queryable.selectFn = func(
		sortSeries bool,
		hints *promstorage.SelectHints,
		labelMatchers ...*labels.Matcher,
	) (promstorage.SeriesSet, promstorage.Warnings, error) {
		return nil, nil, xerrors.NewInvalidParamsError(fmt.Errorf("user input error"))
	}

	req, _ := http.NewRequest("GET", native.PromReadURL, nil)
	req.URL.RawQuery = defaultParams().Encode()

	recorder := httptest.NewRecorder()
	setup.readHandler.ServeHTTP(recorder, req)

	var resp response
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.Equal(t, statusError, resp.Status)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
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
	) (promstorage.SeriesSet, promstorage.Warnings, error) {
		selects++
		query = hints
		return &mockSeriesSet{}, nil, nil
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
	) (promstorage.SeriesSet, promstorage.Warnings, error) {
		selects++
		query = hints
		return &mockSeriesSet{}, nil, nil
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

func abs(v time.Duration) time.Duration {
	if v < 0 {
		return v * -1
	}
	return v
}

func millisTime(timestampMilliseconds int64) time.Time {
	return time.Unix(0, timestampMilliseconds*int64(time.Millisecond))
}
