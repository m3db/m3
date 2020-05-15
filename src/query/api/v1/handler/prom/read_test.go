package prom

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/options"
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

	fetchOptsBuilderCfg := handleroptions.FetchOptionsBuilderOptions{}
	fetchOptsBuilder := handleroptions.NewFetchOptionsBuilder(fetchOptsBuilderCfg)
	hOpts := options.EmptyHandlerOptions().
		SetFetchOptionsBuilder(fetchOptsBuilder)
	queryable := mockQueryable{}
	readHandler := newReadHandler(opts, hOpts, queryable)
	readInstantHandler := newReadHandler(opts, hOpts, queryable)
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
