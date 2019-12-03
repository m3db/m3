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

package httpd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/api/v1/handler"
	m3json "github.com/m3db/m3/src/query/api/v1/handler/json"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote"
	qcost "github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test/m3"
	"github.com/m3db/m3/src/x/instrument"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// Created by init().
	testWorkerPool            xsync.PooledWorkerPool
	defaultLookbackDuration   = time.Minute
	defaultCPUProfileduration = 5 * time.Second
	defaultPlacementServices  = []string{"m3db"}
	svcDefaultOptions         = []handler.ServiceOptionsDefault{
		func(o handler.ServiceOptions) handler.ServiceOptions {
			return o
		},
	}
)

func makeTagOptions() models.TagOptions {
	return models.NewTagOptions().SetMetricName([]byte("some_name"))
}

func newEngine(
	s storage.Storage,
	lookbackDuration time.Duration,
	enforcer qcost.ChainedEnforcer,
	instrumentOpts instrument.Options,
) executor.Engine {
	engineOpts := executor.NewEngineOptions().
		SetStore(s).
		SetLookbackDuration(lookbackDuration).
		SetGlobalEnforcer(enforcer).
		SetInstrumentOptions(instrumentOpts)

	return executor.NewEngine(engineOpts)
}

func setupHandler(store storage.Storage) (*Handler, error) {
	instrumentOpts := instrument.NewOptions()
	downsamplerAndWriter := ingest.NewDownsamplerAndWriter(store, nil, testWorkerPool)
	engine := newEngine(store, time.Minute, nil, instrumentOpts)
	return NewHandler(
		downsamplerAndWriter,
		makeTagOptions(),
		engine,
		nil,
		nil,
		config.Configuration{LookbackDuration: &defaultLookbackDuration},
		nil,
		nil,
		handler.NewFetchOptionsBuilder(handler.FetchOptionsBuilderOptions{}),
		models.QueryContextOptions{},
		instrumentOpts,
		defaultCPUProfileduration,
		defaultPlacementServices,
		svcDefaultOptions)
}

func TestHandlerFetchTimeoutError(t *testing.T) {
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)
	downsamplerAndWriter := ingest.NewDownsamplerAndWriter(storage, nil, testWorkerPool)

	negValue := -1 * time.Second
	dbconfig := &dbconfig.DBConfiguration{Client: client.Configuration{FetchTimeout: &negValue}}
	engine := newEngine(storage, time.Minute, nil, instrument.NewOptions())
	cfg := config.Configuration{LookbackDuration: &defaultLookbackDuration}
	_, err := NewHandler(
		downsamplerAndWriter,
		makeTagOptions(),
		engine,
		nil,
		nil,
		cfg,
		dbconfig,
		nil,
		handler.NewFetchOptionsBuilder(handler.FetchOptionsBuilderOptions{}),
		models.QueryContextOptions{},
		instrument.NewOptions(),
		defaultCPUProfileduration,
		defaultPlacementServices,
		svcDefaultOptions)

	require.Error(t, err)
}

func TestHandlerFetchTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)
	downsamplerAndWriter := ingest.NewDownsamplerAndWriter(storage, nil, testWorkerPool)

	fourMin := 4 * time.Minute
	dbconfig := &dbconfig.DBConfiguration{Client: client.Configuration{FetchTimeout: &fourMin}}
	engine := newEngine(storage, time.Minute, nil, instrument.NewOptions())
	cfg := config.Configuration{LookbackDuration: &defaultLookbackDuration}
	h, err := NewHandler(
		downsamplerAndWriter,
		makeTagOptions(),
		engine,
		nil,
		nil,
		cfg,
		dbconfig,
		nil,
		handler.NewFetchOptionsBuilder(handler.FetchOptionsBuilderOptions{}),
		models.QueryContextOptions{},
		instrument.NewOptions(),
		defaultCPUProfileduration,
		defaultPlacementServices,
		svcDefaultOptions)
	require.NoError(t, err)
	assert.Equal(t, 4*time.Minute, h.timeoutOpts.FetchTimeout)
}

func TestPromRemoteReadGet(t *testing.T) {
	req := httptest.NewRequest("GET", remote.PromReadURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)

	h, err := setupHandler(storage)
	require.NoError(t, err, "unable to setup handler")
	assert.Equal(t, 30*time.Second, h.timeoutOpts.FetchTimeout)
	err = h.RegisterRoutes()
	require.NoError(t, err, "unable to register routes")
	h.Router().ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusMethodNotAllowed, "GET method not defined")
}

func TestPromRemoteReadPost(t *testing.T) {
	req := httptest.NewRequest("POST", remote.PromReadURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)

	h, err := setupHandler(storage)
	require.NoError(t, err, "unable to setup handler")
	err = h.RegisterRoutes()
	require.NoError(t, err, "unable to register routes")
	h.Router().ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusBadRequest, "Empty request")
}

func TestPromNativeReadGet(t *testing.T) {
	req := httptest.NewRequest("GET", native.PromReadURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)

	h, err := setupHandler(storage)
	require.NoError(t, err, "unable to setup handler")
	h.RegisterRoutes()
	h.Router().ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusBadRequest, "Empty request")
}

func TestPromNativeReadPost(t *testing.T) {
	req := httptest.NewRequest("POST", native.PromReadURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)

	h, err := setupHandler(storage)
	require.NoError(t, err, "unable to setup handler")
	h.RegisterRoutes()
	h.Router().ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusBadRequest, "Empty request")
}

func TestJSONWritePost(t *testing.T) {
	req := httptest.NewRequest("POST", m3json.WriteJSONURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)

	h, err := setupHandler(storage)
	require.NoError(t, err, "unable to setup handler")
	h.RegisterRoutes()
	h.Router().ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusBadRequest, "Empty request")
}

func TestRoutesGet(t *testing.T) {
	req := httptest.NewRequest("GET", routesURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)

	h, err := setupHandler(storage)
	require.NoError(t, err, "unable to setup handler")
	h.RegisterRoutes()
	h.Router().ServeHTTP(res, req)

	require.Equal(t, res.Code, http.StatusOK)

	response := &struct {
		Routes []string `json:"routes"`
	}{}

	err = json.NewDecoder(res.Body).Decode(response)
	require.NoError(t, err)

	foundRoutesURL := false
	for _, route := range response.Routes {
		if route == routesURL {
			foundRoutesURL = true
			break
		}
	}
	assert.True(t, foundRoutesURL, "routes URL not served by routes endpoint")
}

func TestHealthGet(t *testing.T) {
	req := httptest.NewRequest("GET", healthURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)

	h, err := setupHandler(storage)
	require.NoError(t, err, "unable to setup handler")
	h.RegisterRoutes()

	h.Router().ServeHTTP(res, req)

	require.Equal(t, res.Code, http.StatusOK)

	response := &struct {
		Uptime string `json:"uptime"`
	}{}

	err = json.NewDecoder(res.Body).Decode(response)
	require.NoError(t, err)

	result, err := time.ParseDuration(response.Uptime)
	require.NoError(t, err)

	assert.True(t, result > 0)
}

func TestCORSMiddleware(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _ := m3.NewStorageAndSession(t, ctrl)
	h, err := setupHandler(s)
	require.NoError(t, err, "unable to setup handler")

	setupTestRoute(h.router)
	res := doTestRequest(h.Router())

	assert.Equal(t, "hello!", res.Body.String())
	assert.Equal(t, "*", res.Header().Get("Access-Control-Allow-Origin"))
}

func doTestRequest(handler http.Handler) *httptest.ResponseRecorder {

	req := httptest.NewRequest("GET", testRoute, nil)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	return res
}

func TestTracingMiddleware(t *testing.T) {
	mtr := mocktracer.New()
	router := mux.NewRouter()
	setupTestRoute(router)

	handler := applyMiddleware(router, mtr)
	doTestRequest(handler)

	assert.NotEmpty(t, mtr.FinishedSpans())
}

const testRoute = "/foobar"

func setupTestRoute(r *mux.Router) {
	r.HandleFunc(testRoute, func(writer http.ResponseWriter, r *http.Request) {
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("hello!"))
	})
}

func init() {
	var err error
	testWorkerPool, err = xsync.NewPooledWorkerPool(
		16,
		xsync.NewPooledWorkerPoolOptions().
			SetGrowOnDemand(true),
	)

	if err != nil {
		panic(fmt.Sprintf("unable to create pooled worker pool: %v", err))
	}

	testWorkerPool.Init()
}
