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
	m3json "github.com/m3db/m3/src/query/api/v1/handler/json"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/executor"
	graphite "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test/m3"
	"github.com/m3db/m3/src/query/ts/m3db"
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
	testM3DBOpts              = m3db.NewOptions()
	defaultLookbackDuration   = time.Minute
	defaultCPUProfileduration = 5 * time.Second
	defaultPlacementServices  = []string{"m3db"}
	svcDefaultOptions         = []handleroptions.ServiceOptionsDefault{
		func(o handleroptions.ServiceOptions) handleroptions.ServiceOptions {
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
	instrumentOpts instrument.Options,
) executor.Engine {
	engineOpts := executor.NewEngineOptions().
		SetStore(s).
		SetLookbackDuration(lookbackDuration).
		SetInstrumentOptions(instrumentOpts)

	return executor.NewEngine(engineOpts)
}

func setupHandler(
	store storage.Storage,
	customHandlers ...options.CustomHandler,
) (*Handler, error) {
	instrumentOpts := instrument.NewOptions()
	downsamplerAndWriter := ingest.NewDownsamplerAndWriter(store, nil, testWorkerPool, instrument.NewOptions())
	engine := newEngine(store, time.Minute, instrumentOpts)
	opts, err := options.NewHandlerOptions(
		downsamplerAndWriter,
		makeTagOptions(),
		engine,
		nil,
		nil,
		nil,
		config.Configuration{LookbackDuration: &defaultLookbackDuration},
		nil,
		handleroptions.NewFetchOptionsBuilder(handleroptions.FetchOptionsBuilderOptions{}),
		models.QueryContextOptions{},
		instrumentOpts,
		defaultCPUProfileduration,
		defaultPlacementServices,
		svcDefaultOptions,
		NewQueryRouter(),
		NewQueryRouter(),
		graphite.M3WrappedStorageOptions{},
		testM3DBOpts,
	)

	if err != nil {
		return nil, err
	}

	return NewHandler(opts, customHandlers...), nil
}

func TestHandlerFetchTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)
	downsamplerAndWriter := ingest.NewDownsamplerAndWriter(storage, nil, testWorkerPool, instrument.NewOptions())

	fourMin := 4 * time.Minute
	dbconfig := &dbconfig.DBConfiguration{Client: client.Configuration{FetchTimeout: &fourMin}}
	engine := newEngine(storage, time.Minute, instrument.NewOptions())
	cfg := config.Configuration{LookbackDuration: &defaultLookbackDuration}
	opts, err := options.NewHandlerOptions(
		downsamplerAndWriter,
		makeTagOptions(),
		engine,
		nil,
		nil,
		nil,
		cfg,
		dbconfig,
		handleroptions.NewFetchOptionsBuilder(handleroptions.FetchOptionsBuilderOptions{}),
		models.QueryContextOptions{},
		instrument.NewOptions(),
		defaultCPUProfileduration,
		defaultPlacementServices,
		svcDefaultOptions,
		nil,
		nil,
		graphite.M3WrappedStorageOptions{},
		testM3DBOpts,
	)
	require.NoError(t, err)

	h := NewHandler(opts)
	assert.Equal(t, 4*time.Minute, h.options.TimeoutOpts().FetchTimeout)
}

func TestPromRemoteReadGet(t *testing.T) {
	req := httptest.NewRequest("GET", remote.PromReadURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)

	h, err := setupHandler(storage)
	require.NoError(t, err, "unable to setup handler")
	assert.Equal(t, 30*time.Second, h.options.TimeoutOpts().FetchTimeout)
	err = h.RegisterRoutes()
	require.NoError(t, err, "unable to register routes")
	h.Router().ServeHTTP(res, req)
	require.Equal(t, http.StatusBadRequest, res.Code)
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
	require.Equal(t, http.StatusBadRequest, res.Code, "Empty request")
}

func TestPromNativeReadGet(t *testing.T) {
	tests := []struct {
		routePrefix string
	}{
		{""},
		{"/prometheus"},
		{"/m3query"},
	}

	for _, tt := range tests {
		url := tt.routePrefix + native.PromReadURL
		t.Run("Testing endpoint GET "+url, func(t *testing.T) {
			req := httptest.NewRequest("GET", url, nil)
			res := httptest.NewRecorder()
			ctrl := gomock.NewController(t)
			storage, _ := m3.NewStorageAndSession(t, ctrl)

			h, err := setupHandler(storage)
			require.NoError(t, err, "unable to setup handler")
			h.RegisterRoutes()
			h.Router().ServeHTTP(res, req)
			require.Equal(t, http.StatusBadRequest, res.Code, "Empty request")
		})
	}
}

func TestPromNativeReadPost(t *testing.T) {
	tests := []struct {
		routePrefix string
	}{
		{""},
		{"/prometheus"},
		{"/m3query"},
	}

	for _, tt := range tests {
		url := tt.routePrefix + native.PromReadURL
		t.Run("Testing endpoint GET "+url, func(t *testing.T) {
			req := httptest.NewRequest("POST", url, nil)
			res := httptest.NewRecorder()
			ctrl := gomock.NewController(t)
			storage, _ := m3.NewStorageAndSession(t, ctrl)

			h, err := setupHandler(storage)
			require.NoError(t, err, "unable to setup handler")
			h.RegisterRoutes()
			h.Router().ServeHTTP(res, req)
			require.Equal(t, http.StatusBadRequest, res.Code, "Empty request")
		})
	}
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
	require.Equal(t, http.StatusBadRequest, res.Code, "Empty request")
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

func TestCompressionMiddleware(t *testing.T) {
	mtr := mocktracer.New()
	router := mux.NewRouter()
	setupTestRoute(router)

	handler := applyMiddleware(router, mtr)
	req := httptest.NewRequest("GET", testRoute, nil)
	req.Header.Add("Accept-Encoding", "gzip")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	enc, found := res.HeaderMap["Content-Encoding"]
	require.True(t, found)
	require.Equal(t, 1, len(enc))
	assert.Equal(t, "gzip", enc[0])
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

type customHandler struct {
	t *testing.T
}

func (h *customHandler) Route() string     { return "/custom" }
func (h *customHandler) Methods() []string { return []string{http.MethodGet} }
func (h *customHandler) Handler(
	opts options.HandlerOptions,
) (http.Handler, error) {
	assert.Equal(h.t, "z", string(opts.TagOptions().MetricName()))
	fn := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("success!"))
	}

	return http.HandlerFunc(fn), nil
}

func TestCustomRoutes(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/custom", nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	store, _ := m3.NewStorageAndSession(t, ctrl)
	instrumentOpts := instrument.NewOptions()
	downsamplerAndWriter := ingest.NewDownsamplerAndWriter(store, nil, testWorkerPool, instrument.NewOptions())
	engine := newEngine(store, time.Minute, instrumentOpts)
	opts, err := options.NewHandlerOptions(
		downsamplerAndWriter, makeTagOptions().SetMetricName([]byte("z")), engine, nil, nil, nil,
		config.Configuration{LookbackDuration: &defaultLookbackDuration}, nil,
		handleroptions.NewFetchOptionsBuilder(handleroptions.FetchOptionsBuilderOptions{}),
		models.QueryContextOptions{}, instrumentOpts, defaultCPUProfileduration,
		defaultPlacementServices, svcDefaultOptions, NewQueryRouter(), NewQueryRouter(),
		graphite.M3WrappedStorageOptions{}, testM3DBOpts)

	require.NoError(t, err)
	custom := &customHandler{t: t}
	handler := NewHandler(opts, custom)
	require.NoError(t, err, "unable to setup handler")
	err = handler.RegisterRoutes()
	require.NoError(t, err, "unable to register routes")
	handler.Router().ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusOK)
}
