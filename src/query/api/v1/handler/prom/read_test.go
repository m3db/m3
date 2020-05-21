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
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/stretchr/testify/require"
)

const promQuery = `http_requests_total{job="prometheus",group="canary"}`

var testPromQLEngine = newMockPromQLEngine()

type testHandlers struct {
	readHandler        http.Handler
	readInstantHandler http.Handler
}

func setupTest(t *testing.T) testHandlers {
	opts := Options{
		PromQLEngine: testPromQLEngine,
	}
	timeoutOpts := &prometheus.TimeoutOpts{
		FetchTimeout: 15 * time.Second,
	}

	fetchOptsBuilderCfg := handleroptions.FetchOptionsBuilderOptions{}
	fetchOptsBuilder := handleroptions.NewFetchOptionsBuilder(fetchOptsBuilderCfg)
	instrumentOpts := instrument.NewOptions()
	engineOpts := executor.NewEngineOptions().
		SetLookbackDuration(time.Minute).
		SetGlobalEnforcer(nil).
		SetInstrumentOptions(instrumentOpts)
	engine := executor.NewEngine(engineOpts)
	hOpts := options.EmptyHandlerOptions().
		SetFetchOptionsBuilder(fetchOptsBuilder).
		SetEngine(engine).
		SetTimeoutOpts(timeoutOpts)
	queryable := mockQueryable{}
	readHandler := newReadHandler(opts, hOpts, queryable)
	readInstantHandler := newReadInstantHandler(opts, hOpts, queryable)
	return testHandlers{
		readHandler:        readHandler,
		readInstantHandler: readInstantHandler,
	}
}

func defaultParams() url.Values {
	vals := url.Values{}
	now := time.Now()
	vals.Add(queryParam, promQuery)
	vals.Add(startParam, now.Format(time.RFC3339))
	vals.Add(endParam, string(now.Add(time.Hour).Format(time.RFC3339)))
	vals.Add(handleroptions.StepParam, (time.Duration(10) * time.Second).String())
	return vals
}

func defaultParamsWithoutQuery() url.Values {
	vals := url.Values{}
	now := time.Now()
	vals.Add(startParam, now.Format(time.RFC3339))
	vals.Add(endParam, string(now.Add(time.Hour).Format(time.RFC3339)))
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
