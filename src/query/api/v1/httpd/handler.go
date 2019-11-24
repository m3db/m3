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
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof" // needed for pprof handler registration
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/experimental/annotated"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/database"
	"github.com/m3db/m3/src/query/api/v1/handler/graphite"
	m3json "github.com/m3db/m3/src/query/api/v1/handler/json"
	"github.com/m3db/m3/src/query/api/v1/handler/namespace"
	"github.com/m3db/m3/src/query/api/v1/handler/openapi"
	"github.com/m3db/m3/src/query/api/v1/handler/placement"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/validator"
	"github.com/m3db/m3/src/query/api/v1/handler/topic"
	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/util/logging"
	xdebug "github.com/m3db/m3/src/x/debug"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
	"github.com/m3db/m3/src/x/net/http/cors"

	"github.com/gorilla/mux"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	opentracing "github.com/opentracing/opentracing-go"
)

const (
	healthURL = "/health"
	routesURL = "/routes"
)

var (
	remoteSource = map[string]string{"source": "remote"}
	nativeSource = map[string]string{"source": "native"}

	v1APIGroup           = map[string]string{"api_group": "v1"}
	experimentalAPIGroup = map[string]string{"api_group": "experimental"}

	defaultTimeout = 30 * time.Second
)

// Handler represents an HTTP handler.
type Handler struct {
	router                *mux.Router
	handler               http.Handler
	storage               storage.Storage
	downsamplerAndWriter  ingest.DownsamplerAndWriter
	engine                executor.Engine
	clusters              m3.Clusters
	clusterClient         clusterclient.Client
	config                config.Configuration
	embeddedDbCfg         *dbconfig.DBConfiguration
	createdAt             time.Time
	tagOptions            models.TagOptions
	timeoutOpts           *prometheus.TimeoutOpts
	enforcer              cost.ChainedEnforcer
	fetchOptionsBuilder   handler.FetchOptionsBuilder
	queryContextOptions   models.QueryContextOptions
	instrumentOpts        instrument.Options
	cpuProfileDuration    time.Duration
	placementServiceNames []string
	serviceOptionDefaults []handler.ServiceOptionsDefault
}

// Router returns the http handler registered with all relevant routes for query.
func (h *Handler) Router() http.Handler {
	return h.handler
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(
	downsamplerAndWriter ingest.DownsamplerAndWriter,
	tagOptions models.TagOptions,
	engine executor.Engine,
	m3dbClusters m3.Clusters,
	clusterClient clusterclient.Client,
	cfg config.Configuration,
	embeddedDbCfg *dbconfig.DBConfiguration,
	enforcer cost.ChainedEnforcer,
	fetchOptionsBuilder handler.FetchOptionsBuilder,
	queryContextOptions models.QueryContextOptions,
	instrumentOpts instrument.Options,
	cpuProfileDuration time.Duration,
	placementServiceNames []string,
	serviceOptionDefaults []handler.ServiceOptionsDefault,
) (*Handler, error) {
	r := mux.NewRouter()

	handlerWithMiddleware := applyMiddleware(r, opentracing.GlobalTracer())

	var timeoutOpts = &prometheus.TimeoutOpts{}
	if embeddedDbCfg == nil || embeddedDbCfg.Client.FetchTimeout == nil {
		timeoutOpts.FetchTimeout = defaultTimeout
	} else {
		if *embeddedDbCfg.Client.FetchTimeout <= 0 {
			return nil, errors.New("m3db client fetch timeout should be > 0")
		}

		timeoutOpts.FetchTimeout = *embeddedDbCfg.Client.FetchTimeout
	}

	return &Handler{
		router:                r,
		handler:               handlerWithMiddleware,
		storage:               downsamplerAndWriter.Storage(),
		downsamplerAndWriter:  downsamplerAndWriter,
		engine:                engine,
		clusters:              m3dbClusters,
		clusterClient:         clusterClient,
		config:                cfg,
		embeddedDbCfg:         embeddedDbCfg,
		createdAt:             time.Now(),
		tagOptions:            tagOptions,
		timeoutOpts:           timeoutOpts,
		enforcer:              enforcer,
		fetchOptionsBuilder:   fetchOptionsBuilder,
		queryContextOptions:   queryContextOptions,
		instrumentOpts:        instrumentOpts,
		cpuProfileDuration:    cpuProfileDuration,
		placementServiceNames: placementServiceNames,
		serviceOptionDefaults: serviceOptionDefaults,
	}, nil
}

func applyMiddleware(base *mux.Router, tracer opentracing.Tracer) http.Handler {
	withMiddleware := http.Handler(&cors.Handler{
		Handler: base,
		Info: &cors.Info{
			"*": true,
		},
	})

	// apply jaeger middleware, which will start a span
	// for each incoming request
	withMiddleware = nethttp.Middleware(tracer, withMiddleware,
		nethttp.OperationNameFunc(func(r *http.Request) string {
			return fmt.Sprintf("%s %s", r.Method, r.URL.Path)
		}))
	return withMiddleware
}

// RegisterRoutes registers all http routes.
func (h *Handler) RegisterRoutes() error {
	// Wrap requests with response time logging as well as panic recovery.
	var (
		wrapped = func(n http.Handler) http.Handler {
			return logging.WithResponseTimeAndPanicErrorLogging(n, h.instrumentOpts)
		}
		panicOnly = func(n http.Handler) http.Handler {
			return logging.WithPanicErrorResponder(n, h.instrumentOpts)
		}
		nowFn    = time.Now
		keepNans = h.config.ResultOptions.KeepNans
	)

	h.router.HandleFunc(openapi.URL,
		wrapped(openapi.NewDocHandler(h.instrumentOpts)).ServeHTTP,
	).Methods(openapi.HTTPMethod)
	h.router.PathPrefix(openapi.StaticURLPrefix).Handler(wrapped(openapi.StaticHandler()))

	// Prometheus remote read/write endpoints
	remoteSourceInstrumentOpts := h.instrumentOpts.
		SetMetricsScope(h.instrumentOpts.MetricsScope().
			Tagged(remoteSource).
			Tagged(v1APIGroup),
		)

	promRemoteReadHandler := remote.NewPromReadHandler(h.engine,
		h.fetchOptionsBuilder, h.timeoutOpts, keepNans, remoteSourceInstrumentOpts)
	promRemoteWriteHandler, err := remote.NewPromWriteHandler(h.downsamplerAndWriter,
		h.tagOptions, h.config.WriteForwarding.PromRemoteWrite, nowFn, remoteSourceInstrumentOpts)
	if err != nil {
		return err
	}

	nativeSourceInstrumentOpts := h.instrumentOpts.
		SetMetricsScope(h.instrumentOpts.MetricsScope().
			Tagged(nativeSource).
			Tagged(v1APIGroup),
		)
	nativePromReadHandler := native.NewPromReadHandler(h.engine,
		h.fetchOptionsBuilder, h.tagOptions, &h.config.Limits,
		h.timeoutOpts, keepNans, nativeSourceInstrumentOpts)

	h.router.HandleFunc(remote.PromReadURL,
		wrapped(promRemoteReadHandler).ServeHTTP,
	).Methods(remote.PromReadHTTPMethod)
	h.router.HandleFunc(remote.PromWriteURL,
		panicOnly(promRemoteWriteHandler).ServeHTTP,
	).Methods(remote.PromWriteHTTPMethod)
	h.router.HandleFunc(native.PromReadURL,
		wrapped(nativePromReadHandler).ServeHTTP,
	).Methods(native.PromReadHTTPMethod)
	h.router.HandleFunc(native.PromReadInstantURL,
		wrapped(native.NewPromReadInstantHandler(h.engine, h.fetchOptionsBuilder,
			h.tagOptions, h.timeoutOpts, h.instrumentOpts)).ServeHTTP,
	).Methods(native.PromReadInstantHTTPMethod)

	// Native M3 search and write endpoints
	h.router.HandleFunc(handler.SearchURL,
		wrapped(handler.NewSearchHandler(h.storage,
			h.fetchOptionsBuilder, h.instrumentOpts)).ServeHTTP,
	).Methods(handler.SearchHTTPMethod)
	h.router.HandleFunc(m3json.WriteJSONURL,
		wrapped(m3json.NewWriteJSONHandler(h.storage, h.instrumentOpts)).ServeHTTP,
	).Methods(m3json.JSONWriteHTTPMethod)

	// Tag completion endpoints
	h.router.HandleFunc(native.CompleteTagsURL,
		wrapped(native.NewCompleteTagsHandler(h.storage,
			h.fetchOptionsBuilder, h.instrumentOpts)).ServeHTTP,
	).Methods(native.CompleteTagsHTTPMethod)
	h.router.HandleFunc(remote.TagValuesURL,
		wrapped(remote.NewTagValuesHandler(h.storage, h.fetchOptionsBuilder,
			nowFn, h.instrumentOpts)).ServeHTTP,
	).Methods(remote.TagValuesHTTPMethod)

	// List tag endpoints
	for _, method := range native.ListTagsHTTPMethods {
		h.router.HandleFunc(native.ListTagsURL,
			wrapped(native.NewListTagsHandler(h.storage, h.fetchOptionsBuilder,
				nowFn, h.instrumentOpts)).ServeHTTP,
		).Methods(method)
	}

	// Query parse endpoints
	h.router.HandleFunc(native.PromParseURL,
		wrapped(native.NewPromParseHandler(h.instrumentOpts)).ServeHTTP,
	).Methods(native.PromParseHTTPMethod)
	h.router.HandleFunc(native.PromThresholdURL,
		wrapped(native.NewPromThresholdHandler(h.instrumentOpts)).ServeHTTP,
	).Methods(native.PromThresholdHTTPMethod)

	// Series match endpoints
	for _, method := range remote.PromSeriesMatchHTTPMethods {
		h.router.HandleFunc(remote.PromSeriesMatchURL,
			wrapped(remote.NewPromSeriesMatchHandler(h.storage,
				h.tagOptions, h.fetchOptionsBuilder, h.instrumentOpts)).ServeHTTP,
		).Methods(method)
	}

	// Debug endpoints
	h.router.HandleFunc(validator.PromDebugURL,
		wrapped(validator.NewPromDebugHandler(nativePromReadHandler,
			h.fetchOptionsBuilder, *h.config.LookbackDuration, h.instrumentOpts)).ServeHTTP,
	).Methods(validator.PromDebugHTTPMethod)

	// Graphite endpoints
	h.router.HandleFunc(graphite.ReadURL,
		wrapped(graphite.NewRenderHandler(h.storage,
			h.queryContextOptions, h.enforcer, h.instrumentOpts)).ServeHTTP,
	).Methods(graphite.ReadHTTPMethods...)

	h.router.HandleFunc(graphite.FindURL,
		wrapped(graphite.NewFindHandler(h.storage,
			h.fetchOptionsBuilder, h.instrumentOpts)).ServeHTTP,
	).Methods(graphite.FindHTTPMethods...)

	placementOpts, err := h.placementOpts()
	if err != nil {
		return err
	}

	var placementServices []handler.ServiceNameAndDefaults
	for _, serviceName := range h.placementServiceNames {
		service := handler.ServiceNameAndDefaults{
			ServiceName: serviceName,
			Defaults:    h.serviceOptionDefaults,
		}
		placementServices = append(placementServices, service)
	}

	debugWriter, err := xdebug.NewPlacementAndNamespaceZipWriterWithDefaultSources(
		h.cpuProfileDuration,
		h.clusterClient,
		placementOpts,
		placementServices,
		h.instrumentOpts)
	if err != nil {
		return fmt.Errorf("unable to create debug writer: %v", err)
	}

	// Register debug dump handler.
	h.router.HandleFunc(xdebug.DebugURL,
		wrapped(debugWriter.HTTPHandler()).ServeHTTP)

	if h.clusterClient != nil {
		err = database.RegisterRoutes(h.router, h.clusterClient,
			h.config, h.embeddedDbCfg, h.serviceOptionDefaults, h.instrumentOpts)
		if err != nil {
			return err
		}

		placement.RegisterRoutes(h.router, h.serviceOptionDefaults, placementOpts)
		namespace.RegisterRoutes(h.router, h.clusterClient, h.serviceOptionDefaults, h.instrumentOpts)
		topic.RegisterRoutes(h.router, h.clusterClient, h.config, h.instrumentOpts)

		// Experimental endpoints.
		if h.config.Experimental.Enabled {
			experimentalAnnotatedWriteHandler := annotated.NewHandler(
				h.downsamplerAndWriter,
				h.tagOptions,
				h.instrumentOpts.MetricsScope().
					Tagged(remoteSource).
					Tagged(experimentalAPIGroup),
			)
			h.router.HandleFunc(annotated.WriteURL,
				wrapped(experimentalAnnotatedWriteHandler).ServeHTTP,
			).Methods(annotated.WriteHTTPMethod)
		}
	}

	h.registerHealthEndpoints()
	h.registerProfileEndpoints()
	h.registerRoutesEndpoint()

	return nil
}

func (h *Handler) placementOpts() (placement.HandlerOptions, error) {
	return placement.NewHandlerOptions(
		h.clusterClient,
		h.config,
		h.m3AggServiceOptions(),
		h.instrumentOpts,
	)
}

func (h *Handler) m3AggServiceOptions() *handler.M3AggServiceOptions {
	if h.clusters == nil {
		return nil
	}

	maxResolution := time.Duration(0)
	for _, ns := range h.clusters.ClusterNamespaces() {
		resolution := ns.Options().Attributes().Resolution
		if resolution > maxResolution {
			maxResolution = resolution
		}
	}

	if maxResolution == 0 {
		return nil
	}

	return &handler.M3AggServiceOptions{
		MaxAggregationWindowSize: maxResolution,
	}
}

// Endpoints useful for profiling the service
func (h *Handler) registerHealthEndpoints() {
	h.router.HandleFunc(healthURL, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(struct {
			Uptime string `json:"uptime"`
		}{
			Uptime: time.Since(h.createdAt).String(),
		})
	}).Methods(http.MethodGet)
}

// Endpoints useful for profiling the service
func (h *Handler) registerProfileEndpoints() {
	h.router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
}

// Endpoints useful for viewing routes directory
func (h *Handler) registerRoutesEndpoint() {
	h.router.HandleFunc(routesURL, func(w http.ResponseWriter, r *http.Request) {
		var routes []string
		err := h.router.Walk(
			func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
				str, err := route.GetPathTemplate()
				if err != nil {
					return err
				}
				routes = append(routes, str)
				return nil
			})
		if err != nil {
			xhttp.Error(w, err, http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(struct {
			Routes []string `json:"routes"`
		}{
			Routes: routes,
		})
	}).Methods(http.MethodGet)
}
