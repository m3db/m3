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
	"github.com/m3db/m3/src/cmd/services/m3query/config"
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
	"github.com/m3db/m3/src/query/util/queryhttp"
	"github.com/m3db/m3/src/x/instrument"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/prometheus/prometheus/promql"
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
	fetchOptsBuilder, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{
			Timeout: 15 * time.Second,
		})
	if err != nil {
		return nil, err
	}
	opts, err := options.NewHandlerOptions(
		downsamplerAndWriter,
		makeTagOptions(),
		engine,
		newPromEngine(),
		nil,
		nil,
		config.Configuration{LookbackDuration: &defaultLookbackDuration},
		nil,
		fetchOptsBuilder,
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

func newPromEngine() *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		MaxSamples:         10000,
		Timeout:            100 * time.Second,
	})
}

func TestPromRemoteReadGet(t *testing.T) {
	req := httptest.NewRequest("GET", remote.PromReadURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)

	h, err := setupHandler(storage)
	require.NoError(t, err, "unable to setup handler")
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

	setupTestRouteRegistry(h.registry)
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
	setupTestRouteRouter(router)

	handler := applyMiddleware(router, mtr)
	doTestRequest(handler)

	assert.NotEmpty(t, mtr.FinishedSpans())
}

func TestCompressionMiddleware(t *testing.T) {
	mtr := mocktracer.New()
	router := mux.NewRouter()
	setupTestRouteRouter(router)

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

func setupTestRouteRegistry(r *queryhttp.EndpointRegistry) {
	r.Register(queryhttp.RegisterOptions{
		Path: testRoute,
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte("hello!"))
		}),
		Methods: methods(http.MethodGet),
	})
}

func setupTestRouteRouter(r *mux.Router) {
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

type assertFn func(t *testing.T, prev http.Handler, r *http.Request)

type customHandler struct {
	t         *testing.T
	routeName string
	methods   []string
	assertFn  assertFn
}

func (h *customHandler) Route() string     { return h.routeName }
func (h *customHandler) Methods() []string { return h.methods }
func (h *customHandler) Handler(
	opts options.HandlerOptions,
	prev http.Handler,
) (http.Handler, error) {
	assert.Equal(h.t, "z", string(opts.TagOptions().MetricName()))
	fn := func(w http.ResponseWriter, r *http.Request) {
		h.assertFn(h.t, prev, r)
		_, err := w.Write([]byte("success!"))
		require.NoError(h.t, err)
	}

	return http.HandlerFunc(fn), nil
}

func TestCustomRoutes(t *testing.T) {
	ctrl := gomock.NewController(t)
	store, _ := m3.NewStorageAndSession(t, ctrl)
	instrumentOpts := instrument.NewOptions()
	downsamplerAndWriter := ingest.NewDownsamplerAndWriter(store, nil, testWorkerPool, instrument.NewOptions())
	engine := newEngine(store, time.Minute, instrumentOpts)
	fetchOptsBuilder, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{
			Timeout: 15 * time.Second,
		})
	require.NoError(t, err)
	opts, err := options.NewHandlerOptions(
		downsamplerAndWriter, makeTagOptions().SetMetricName([]byte("z")),
		engine, newPromEngine(), nil, nil,
		config.Configuration{LookbackDuration: &defaultLookbackDuration}, nil,
		fetchOptsBuilder, models.QueryContextOptions{}, instrumentOpts, defaultCPUProfileduration,
		defaultPlacementServices, svcDefaultOptions, NewQueryRouter(), NewQueryRouter(),
		graphite.M3WrappedStorageOptions{}, testM3DBOpts)
	require.NoError(t, err)
	custom := &customHandler{
		t:         t,
		routeName: "/custom",
		methods:   []string{http.MethodGet, http.MethodHead},
		assertFn: func(t *testing.T, prev http.Handler, r *http.Request) {
			assert.Nil(t, prev, "Should not shadow already existing handler")
		},
	}
	customShadowGet := &customHandler{
		t:         t,
		routeName: "/custom",
		methods:   []string{http.MethodGet},
		assertFn: func(t *testing.T, prev http.Handler, r *http.Request) {
			assert.NotNil(t, prev, "Should shadow already existing handler")
		},
	}
	customShadowHead := &customHandler{
		t:         t,
		routeName: "/custom",
		methods:   []string{http.MethodHead},
		assertFn: func(t *testing.T, prev http.Handler, r *http.Request) {
			assert.NotNil(t, prev, "Should shadow already existing handler")
		},
	}
	customNew := &customHandler{
		t:         t,
		routeName: "/custom/new",
		methods:   []string{http.MethodGet, http.MethodHead},
		assertFn: func(t *testing.T, prev http.Handler, r *http.Request) {
			assert.Nil(t, prev, "Should not shadow already existing handler")
		},
	}
	handler := NewHandler(opts, custom, customShadowGet, customShadowHead, customNew)
	require.NoError(t, err, "unable to setup handler")
	err = handler.RegisterRoutes()
	require.NoError(t, err, "unable to register routes")

	for _, method := range custom.methods {
		assertRoute(t, custom.routeName, method, handler, http.StatusOK)
	}

	for _, method := range customNew.methods {
		assertRoute(t, customNew.routeName, method, handler, http.StatusOK)
	}

	assertRoute(t, customNew.routeName, http.MethodPost, handler, http.StatusMethodNotAllowed)
	assertRoute(t, "/unknown", http.MethodGet, handler, http.StatusNotFound)
}

func assertRoute(t *testing.T, routeName string, method string, handler *Handler, expectedStatusCode int) {
	req := httptest.NewRequest(method, routeName, nil)
	res := httptest.NewRecorder()
	handler.Router().ServeHTTP(res, req)
	require.Equal(t, expectedStatusCode, res.Code)
}
