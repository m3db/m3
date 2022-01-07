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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/query/block"
	parserpromql "github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/test"
	"github.com/prometheus/prometheus/promql"
	parser2 "github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	handleroptions3 "github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/api/v1/handler/graphite"
	"github.com/m3db/m3/src/query/api/v1/handler/influxdb"
	m3json "github.com/m3db/m3/src/query/api/v1/handler/json"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote"
	"github.com/m3db/m3/src/query/api/v1/middleware"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/executor"
	graphiteStorage "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	m3storage "github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/test/m3"
	"github.com/m3db/m3/src/x/instrument"
	xsync "github.com/m3db/m3/src/x/sync"
)

var (
	// Created by init().
	testWorkerPool            xsync.PooledWorkerPool
	testM3DBOpts              = m3storage.NewOptions(encoding.NewOptions())
	defaultLookbackDuration   = time.Minute
	defaultCPUProfileduration = 5 * time.Second
	defaultPlacementServices  = []string{"m3db"}
	svcDefaultOptions         = []handleroptions3.ServiceOptionsDefault{
		func(o handleroptions3.ServiceOptions) handleroptions3.ServiceOptions {
			return o
		},
	}
)

type str struct {
}

func (s str) FetchProm(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (storage.PromResult, error) {
	panic("implement me")
}

func (s str) FetchBlocks(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (block.Result, error) {
	fmt.Printf("query: %s, %s. \n", query.Raw, query.TagMatchers.String())
	return block.Result{
		Blocks: []block.Block{test.NewBlockFromValues(models.Bounds{StepSize: time.Second, Duration: time.Second}, [][]float64{{1.0}})},
	}, nil
}

func (s str) FetchCompressed(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (consolidators.MultiFetchResult, error) {
	panic("implement me")
}

func (s str) SearchSeries(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.SearchResults, error) {
	panic("implement me")
}

func (s str) CompleteTags(ctx context.Context, query *storage.CompleteTagsQuery, options *storage.FetchOptions) (*consolidators.CompleteTagsResult, error) {
	panic("implement me")
}

func (s str) QueryStorageMetadataAttributes(ctx context.Context, queryStart, queryEnd time.Time, opts *storage.FetchOptions) ([]storagemetadata.Attributes, error) {
	panic("implement me")
}

func (s str) Write(ctx context.Context, query *storage.WriteQuery) error {
	panic("implement me")
}

func (s str) Type() storage.Type {
	panic("implement me")
}

func (s str) Close() error {
	panic("implement me")
}

func (s str) ErrorBehavior() storage.ErrorBehavior {
	panic("implement me")
}

func (s str) Name() string {
	panic("implement me")
}

func TestStuff(t *testing.T) {
	parser2.Functions["m3_count"] = &parser2.Function{
		Name:       "m3_count",
		ArgTypes:   []parser2.ValueType{parser2.ValueTypeVector},
		ReturnType: parser2.ValueTypeVector,
	}
	var s storage.Storage = &str{}
	eng := newEngine(s, time.Second*60, instrument.NewOptions())

	parser, err := parserpromql.Parse("m3_count(ambassador_process_cpu_seconds_total{})", time.Second, models.NewTagOptions(), parserpromql.NewParseOptions())
	require.NoError(t, err)

	bl, err := eng.ExecuteExpr(context.Background(), parser, &executor.QueryOptions{}, &storage.FetchOptions{Step: time.Second}, models.RequestParams{Step: time.Second})
	require.NoError(t, err)
	it, err := bl.StepIter()
	require.NoError(t, err)
	println("next")
	println("next", it.Next(), it.Current().Values()[0])
}

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
	promEngineFn := func(_ time.Duration) (*promql.Engine, error) {
		return newPromEngine(), nil
	}
	opts, err := options.NewHandlerOptions(
		downsamplerAndWriter,
		makeTagOptions(),
		engine,
		promEngineFn,
		nil,
		nil,
		config.Configuration{LookbackDuration: &defaultLookbackDuration},
		nil,
		fetchOptsBuilder,
		fetchOptsBuilder,
		fetchOptsBuilder,
		models.QueryContextOptions{},
		instrumentOpts,
		defaultCPUProfileduration,
		defaultPlacementServices,
		svcDefaultOptions,
		NewQueryRouter(),
		NewQueryRouter(),
		graphiteStorage.M3WrappedStorageOptions{},
		testM3DBOpts,
		NewGraphiteRenderRouter(),
		NewGraphiteFindRouter(),
		defaultLookbackDuration,
	)
	if err != nil {
		return nil, err
	}

	return NewHandler(opts, config.MiddlewareConfiguration{}, customHandlers...), nil
}

func newPromEngine() *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		MaxSamples: 10000,
		Timeout:    100 * time.Second,
		NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 {
			return durationMilliseconds(1 * time.Minute)
		},
	})
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
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

func TestInfluxDBWritePost(t *testing.T) {
	req := httptest.NewRequest(influxdb.InfluxWriteHTTPMethod, influxdb.InfluxWriteURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)

	h, err := setupHandler(storage)
	require.NoError(t, err, "unable to setup handler")
	err = h.RegisterRoutes()
	require.NoError(t, err)
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

func TestGraphite(t *testing.T) {
	tests := []struct {
		url    string
		target string
	}{
		{graphite.ReadURL, "GET"},
		{graphite.ReadURL, "POST"},
		{graphite.FindURL, "GET"},
		{graphite.FindURL, "POST"},
	}

	for _, tt := range tests {
		url := graphite.ReadURL
		t.Run(tt.url+"_"+tt.target, func(t *testing.T) {
			req := httptest.NewRequest(tt.target, url, nil)
			res := httptest.NewRecorder()
			ctrl := gomock.NewController(t)
			storage, _ := m3.NewStorageAndSession(t, ctrl)

			h, err := setupHandler(storage)
			require.NoError(t, err, "unable to setup handler")
			err = h.RegisterRoutes()
			require.NoError(t, err)
			h.Router().ServeHTTP(res, req)
			require.Equal(t, http.StatusBadRequest, res.Code, "Empty request")
		})
	}
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
	t          *testing.T
	routeName  string
	methods    []string
	assertFn   assertFn
	middleware middleware.OverrideOptions
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
func (h *customHandler) MiddlewareOverride() middleware.OverrideOptions {
	return h.middleware
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
	promEngineFn := func(_ time.Duration) (*promql.Engine, error) {
		return newPromEngine(), nil
	}
	opts, err := options.NewHandlerOptions(
		downsamplerAndWriter, makeTagOptions().SetMetricName([]byte("z")),
		engine, promEngineFn, nil, nil,
		config.Configuration{LookbackDuration: &defaultLookbackDuration}, nil,
		fetchOptsBuilder, fetchOptsBuilder, fetchOptsBuilder,
		models.QueryContextOptions{}, instrumentOpts, defaultCPUProfileduration,
		defaultPlacementServices, svcDefaultOptions, NewQueryRouter(), NewQueryRouter(),
		graphiteStorage.M3WrappedStorageOptions{}, testM3DBOpts, NewGraphiteRenderRouter(), NewGraphiteFindRouter(),
		defaultLookbackDuration,
	)
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
	handler := NewHandler(opts, config.MiddlewareConfiguration{},
		custom, customShadowGet, customShadowHead, customNew)
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
