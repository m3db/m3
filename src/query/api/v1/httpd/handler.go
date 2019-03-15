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
	xhttp "github.com/m3db/m3/src/x/net/http"
	"github.com/m3db/m3/src/x/net/http/cors"

	"github.com/gorilla/mux"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
)

const (
	healthURL = "/health"
	routesURL = "/routes"
)

var (
	remoteSource = map[string]string{"source": "remote"}
	nativeSource = map[string]string{"source": "native"}

	defaultTimeout = 30 * time.Second
)

// Handler represents an HTTP handler.
type Handler struct {
	router               *mux.Router
	handler              http.Handler
	storage              storage.Storage
	downsamplerAndWriter ingest.DownsamplerAndWriter
	engine               *executor.Engine
	clusters             m3.Clusters
	clusterClient        clusterclient.Client
	config               config.Configuration
	embeddedDbCfg        *dbconfig.DBConfiguration
	scope                tally.Scope
	createdAt            time.Time
	tagOptions           models.TagOptions
	timeoutOpts          *prometheus.TimeoutOpts
	enforcer             cost.ChainedEnforcer
}

// Router returns the http handler registered with all relevant routes for query.
func (h *Handler) Router() http.Handler {
	return h.handler
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(
	downsamplerAndWriter ingest.DownsamplerAndWriter,
	tagOptions models.TagOptions,
	engine *executor.Engine,
	m3dbClusters m3.Clusters,
	clusterClient clusterclient.Client,
	cfg config.Configuration,
	embeddedDbCfg *dbconfig.DBConfiguration,
	enforcer cost.ChainedEnforcer,
	scope tally.Scope,
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

	h := &Handler{
		router:               r,
		handler:              handlerWithMiddleware,
		storage:              downsamplerAndWriter.Storage(),
		downsamplerAndWriter: downsamplerAndWriter,
		engine:               engine,
		clusters:             m3dbClusters,
		clusterClient:        clusterClient,
		config:               cfg,
		embeddedDbCfg:        embeddedDbCfg,
		scope:                scope,
		createdAt:            time.Now(),
		tagOptions:           tagOptions,
		timeoutOpts:          timeoutOpts,
		enforcer:             enforcer,
	}
	return h, nil
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
	wrapped := logging.WithResponseTimeAndPanicErrorLogging
	panicOnly := logging.WithPanicErrorResponder

	h.router.HandleFunc(openapi.URL,
		wrapped(&openapi.DocHandler{}).ServeHTTP,
	).Methods(openapi.HTTPMethod)
	h.router.PathPrefix(openapi.StaticURLPrefix).Handler(wrapped(openapi.StaticHandler()))

	// Prometheus remote read/write endpoints
	promRemoteReadHandler := remote.NewPromReadHandler(h.engine, h.scope.Tagged(remoteSource), h.timeoutOpts)
	promRemoteWriteHandler, err := remote.NewPromWriteHandler(
		h.downsamplerAndWriter,
		h.tagOptions,
		h.scope.Tagged(remoteSource),
	)
	if err != nil {
		return err
	}

	nativePromReadHandler := native.NewPromReadHandler(
		h.engine,
		h.tagOptions,
		&h.config.Limits,
		h.scope.Tagged(nativeSource),
		h.timeoutOpts,
		h.config.ResultOptions.KeepNans,
	)

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
		wrapped(native.NewPromReadInstantHandler(h.engine, h.tagOptions, h.timeoutOpts)).ServeHTTP,
	).Methods(native.PromReadInstantHTTPMethod)

	// Native M3 search and write endpoints
	h.router.HandleFunc(handler.SearchURL,
		wrapped(handler.NewSearchHandler(h.storage)).ServeHTTP,
	).Methods(handler.SearchHTTPMethod)
	h.router.HandleFunc(m3json.WriteJSONURL,
		wrapped(m3json.NewWriteJSONHandler(h.storage)).ServeHTTP,
	).Methods(m3json.JSONWriteHTTPMethod)

	// Tag completion endpoints
	h.router.HandleFunc(native.CompleteTagsURL,
		wrapped(native.NewCompleteTagsHandler(h.storage)).ServeHTTP,
	).Methods(native.CompleteTagsHTTPMethod)
	h.router.HandleFunc(remote.TagValuesURL,
		wrapped(remote.NewTagValuesHandler(h.storage)).ServeHTTP,
	).Methods(remote.TagValuesHTTPMethod)

	// Series match endpoints
	h.router.HandleFunc(remote.PromSeriesMatchURL,
		wrapped(remote.NewPromSeriesMatchHandler(h.storage, h.tagOptions)).ServeHTTP,
	).Methods(remote.PromSeriesMatchHTTPMethod)

	// Debug endpoints
	h.router.HandleFunc(validator.PromDebugURL,
		wrapped(validator.NewPromDebugHandler(nativePromReadHandler, h.scope, *h.config.LookbackDuration)).ServeHTTP,
	).Methods(validator.PromDebugHTTPMethod)

	// Graphite endpoints
	h.router.HandleFunc(graphite.ReadURL,
		wrapped(graphite.NewRenderHandler(h.storage, h.enforcer)).ServeHTTP,
	).Methods(graphite.ReadHTTPMethods...)

	h.router.HandleFunc(graphite.FindURL,
		wrapped(graphite.NewFindHandler(h.storage)).ServeHTTP,
	).Methods(graphite.FindHTTPMethods...)

	if h.clusterClient != nil {
		placementOpts := placement.HandlerOptions{
			ClusterClient:       h.clusterClient,
			Config:              h.config,
			M3AggServiceOptions: h.m3AggServiceOptions(),
		}

		placement.RegisterRoutes(h.router, placementOpts)
		namespace.RegisterRoutes(h.router, h.clusterClient)
		database.RegisterRoutes(h.router, h.clusterClient, h.config, h.embeddedDbCfg)
		topic.RegisterRoutes(h.router, h.clusterClient, h.config)
	}

	h.registerHealthEndpoints()
	h.registerProfileEndpoints()
	h.registerRoutesEndpoint()

	return nil
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
