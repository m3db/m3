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
	_ "net/http/pprof" // needed for pprof handler registration
	"time"

	"github.com/m3db/m3/src/query/api/experimental/annotated"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/database"
	"github.com/m3db/m3/src/query/api/v1/handler/graphite"
	"github.com/m3db/m3/src/query/api/v1/handler/influxdb"
	m3json "github.com/m3db/m3/src/query/api/v1/handler/json"
	"github.com/m3db/m3/src/query/api/v1/handler/namespace"
	"github.com/m3db/m3/src/query/api/v1/handler/openapi"
	"github.com/m3db/m3/src/query/api/v1/handler/placement"
	"github.com/m3db/m3/src/query/api/v1/handler/prom"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote"
	"github.com/m3db/m3/src/query/api/v1/handler/topic"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/util/logging"
	xdebug "github.com/m3db/m3/src/x/debug"
	xhttp "github.com/m3db/m3/src/x/net/http"
	"github.com/m3db/m3/src/x/net/http/cors"

	"github.com/gorilla/mux"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/prometheus/util/httputil"
)

const (
	healthURL = "/health"
	routesURL = "/routes"
	// EngineHeaderName defines header name which is used to switch between
	// prometheus and m3query engines.
	EngineHeaderName = "M3-Engine"
	// EngineURLParam defines query url parameter which is used to switch between
	// prometheus and m3query engines.
	EngineURLParam = "engine"
)

var (
	remoteSource = map[string]string{"source": "remote"}
	nativeSource = map[string]string{"source": "native"}

	v1APIGroup           = map[string]string{"api_group": "v1"}
	experimentalAPIGroup = map[string]string{"api_group": "experimental"}
)

// Handler represents the top-level HTTP handler.
type Handler struct {
	router         *mux.Router
	handler        http.Handler
	options        options.HandlerOptions
	customHandlers []options.CustomHandler
}

// Router returns the http handler registered with all relevant routes for query.
func (h *Handler) Router() http.Handler {
	return h.handler
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(
	handlerOptions options.HandlerOptions,
	customHandlers ...options.CustomHandler,
) *Handler {
	r := mux.NewRouter()
	handlerWithMiddleware := applyMiddleware(r, opentracing.GlobalTracer())

	return &Handler{
		router:         r,
		handler:        handlerWithMiddleware,
		options:        handlerOptions,
		customHandlers: customHandlers,
	}
}

func applyMiddleware(base *mux.Router, tracer opentracing.Tracer) http.Handler {
	withMiddleware := http.Handler(&cors.Handler{
		Handler: base,
		Info: &cors.Info{
			"*": true,
		},
	})

	// Apply OpenTracing compatible middleware, which will start a span
	// for each incoming request.
	withMiddleware = nethttp.Middleware(tracer, withMiddleware,
		nethttp.OperationNameFunc(func(r *http.Request) string {
			return fmt.Sprintf("%s %s", r.Method, r.URL.Path)
		}))

	// NB: wrap the handler with a `CompressionHandler`; this allows all
	// routes to support `Accept-Encoding:gzip` and `Accept-Encoding:deflate`
	// requests with the given compression types.
	return httputil.CompressionHandler{
		Handler: withMiddleware,
	}
}

// RegisterRoutes registers all http routes.
func (h *Handler) RegisterRoutes() error {
	instrumentOpts := h.options.InstrumentOpts()
	// Wrap requests with response time logging as well as panic recovery.
	var (
		wrapped = func(n http.Handler) http.Handler {
			return logging.WithResponseTimeAndPanicErrorLogging(n, instrumentOpts)
		}

		panicOnly = func(n http.Handler) http.Handler {
			return logging.WithPanicErrorResponder(n, instrumentOpts)
		}
	)

	h.router.HandleFunc(openapi.URL,
		wrapped(openapi.NewDocHandler(instrumentOpts)).ServeHTTP,
	).Methods(openapi.HTTPMethod)
	h.router.PathPrefix(openapi.StaticURLPrefix).
		Handler(wrapped(openapi.StaticHandler()))

	// Prometheus remote read/write endpoints.
	remoteSourceOpts := h.options.SetInstrumentOpts(instrumentOpts.
		SetMetricsScope(instrumentOpts.MetricsScope().
			Tagged(remoteSource).
			Tagged(v1APIGroup),
		))

	promRemoteReadHandler := remote.NewPromReadHandler(remoteSourceOpts)
	promRemoteWriteHandler, err := remote.NewPromWriteHandler(remoteSourceOpts)
	if err != nil {
		return err
	}

	nativeSourceOpts := h.options.SetInstrumentOpts(instrumentOpts.
		SetMetricsScope(instrumentOpts.MetricsScope().
			Tagged(nativeSource).
			Tagged(v1APIGroup),
		))

	// Register custom endpoints.
	for _, custom := range h.customHandlers {
		handler, err := custom.Handler(nativeSourceOpts)
		if err != nil {
			return err
		}

		h.router.HandleFunc(custom.Route(), handler.ServeHTTP).
			Methods(custom.Methods()...)
	}

	opts := prom.Options{
		PromQLEngine: h.options.PrometheusEngine(),
	}
	promqlQueryHandler := wrapped(prom.NewReadHandler(opts, nativeSourceOpts))
	promqlInstantQueryHandler := wrapped(prom.NewReadInstantHandler(opts, nativeSourceOpts))
	nativePromReadHandler := wrapped(native.NewPromReadHandler(nativeSourceOpts))
	nativePromReadInstantHandler := wrapped(native.NewPromReadInstantHandler(nativeSourceOpts))

	h.options.QueryRouter().Setup(options.QueryRouterOptions{
		DefaultQueryEngine: h.options.DefaultQueryEngine(),
		PromqlHandler:      promqlQueryHandler.ServeHTTP,
		M3QueryHandler:     nativePromReadHandler.ServeHTTP,
	})

	h.options.InstantQueryRouter().Setup(options.QueryRouterOptions{
		DefaultQueryEngine: h.options.DefaultQueryEngine(),
		PromqlHandler:      promqlInstantQueryHandler.ServeHTTP,
		M3QueryHandler:     nativePromReadInstantHandler.ServeHTTP,
	})

	h.router.
		HandleFunc(native.PromReadURL, h.options.QueryRouter().ServeHTTP).
		Methods(native.PromReadHTTPMethods...)
	h.router.
		HandleFunc(native.PromReadInstantURL, h.options.InstantQueryRouter().ServeHTTP).
		Methods(native.PromReadInstantHTTPMethods...)

	h.router.HandleFunc("/prometheus"+native.PromReadURL, promqlQueryHandler.ServeHTTP).Methods(native.PromReadHTTPMethods...)
	h.router.HandleFunc("/prometheus"+native.PromReadInstantURL, promqlInstantQueryHandler.ServeHTTP).Methods(native.PromReadInstantHTTPMethods...)

	h.router.HandleFunc(remote.PromReadURL,
		wrapped(promRemoteReadHandler).ServeHTTP,
	).Methods(remote.PromReadHTTPMethods...)
	h.router.HandleFunc(remote.PromWriteURL,
		panicOnly(promRemoteWriteHandler).ServeHTTP,
	).Methods(remote.PromWriteHTTPMethod)
	h.router.HandleFunc("/m3query"+native.PromReadURL, nativePromReadHandler.ServeHTTP).Methods(native.PromReadHTTPMethods...)
	h.router.HandleFunc("/m3query"+native.PromReadInstantURL, nativePromReadInstantHandler.ServeHTTP).Methods(native.PromReadInstantHTTPMethods...)

	// InfluxDB write endpoint.
	h.router.HandleFunc(influxdb.InfluxWriteURL,
		wrapped(influxdb.NewInfluxWriterHandler(h.options)).ServeHTTP).Methods(influxdb.InfluxWriteHTTPMethod)

	// Native M3 search and write endpoints.
	h.router.HandleFunc(handler.SearchURL,
		wrapped(handler.NewSearchHandler(h.options)).ServeHTTP,
	).Methods(handler.SearchHTTPMethod)
	h.router.HandleFunc(m3json.WriteJSONURL,
		wrapped(m3json.NewWriteJSONHandler(h.options)).ServeHTTP,
	).Methods(m3json.JSONWriteHTTPMethod)

	// Tag completion endpoints.
	h.router.HandleFunc(native.CompleteTagsURL,
		wrapped(native.NewCompleteTagsHandler(h.options)).ServeHTTP,
	).Methods(native.CompleteTagsHTTPMethod)
	h.router.HandleFunc(remote.TagValuesURL,
		wrapped(remote.NewTagValuesHandler(h.options)).ServeHTTP,
	).Methods(remote.TagValuesHTTPMethod)

	// List tag endpoints.
	h.router.HandleFunc(native.ListTagsURL,
		wrapped(native.NewListTagsHandler(h.options)).ServeHTTP,
	).Methods(native.ListTagsHTTPMethods...)

	// Query parse endpoints.
	h.router.HandleFunc(native.PromParseURL,
		wrapped(native.NewPromParseHandler(h.options)).ServeHTTP,
	).Methods(native.PromParseHTTPMethod)
	h.router.HandleFunc(native.PromThresholdURL,
		wrapped(native.NewPromThresholdHandler(h.options)).ServeHTTP,
	).Methods(native.PromThresholdHTTPMethod)

	// Series match endpoints.
	h.router.HandleFunc(remote.PromSeriesMatchURL,
		wrapped(remote.NewPromSeriesMatchHandler(h.options)).ServeHTTP,
	).Methods(remote.PromSeriesMatchHTTPMethods...)

	// Graphite endpoints.
	h.router.HandleFunc(graphite.ReadURL,
		wrapped(graphite.NewRenderHandler(h.options)).ServeHTTP,
	).Methods(graphite.ReadHTTPMethods...)

	h.router.HandleFunc(graphite.FindURL,
		wrapped(graphite.NewFindHandler(h.options)).ServeHTTP,
	).Methods(graphite.FindHTTPMethods...)

	placementOpts, err := h.placementOpts()
	if err != nil {
		return err
	}

	var (
		serviceOptionDefaults = h.options.ServiceOptionDefaults()
		clusterClient         = h.options.ClusterClient()
		config                = h.options.Config()
	)

	var placementServices []handleroptions.ServiceNameAndDefaults
	for _, serviceName := range h.options.PlacementServiceNames() {
		service := handleroptions.ServiceNameAndDefaults{
			ServiceName: serviceName,
			Defaults:    serviceOptionDefaults,
		}

		placementServices = append(placementServices, service)
	}

	debugWriter, err := xdebug.NewPlacementAndNamespaceZipWriterWithDefaultSources(
		h.options.CPUProfileDuration(),
		clusterClient,
		placementOpts,
		placementServices,
		instrumentOpts)
	if err != nil {
		return fmt.Errorf("unable to create debug writer: %v", err)
	}

	// Register debug dump handler.
	h.router.HandleFunc(xdebug.DebugURL,
		wrapped(debugWriter.HTTPHandler()).ServeHTTP)

	if clusterClient != nil {
		err = database.RegisterRoutes(h.router, clusterClient,
			h.options.Config(), h.options.EmbeddedDbCfg(),
			serviceOptionDefaults, instrumentOpts)
		if err != nil {
			return err
		}

		placement.RegisterRoutes(h.router,
			serviceOptionDefaults, placementOpts)
		namespace.RegisterRoutes(h.router, clusterClient, serviceOptionDefaults, instrumentOpts)
		topic.RegisterRoutes(h.router, clusterClient, config, instrumentOpts)

		// Experimental endpoints.
		if config.Experimental.Enabled {
			experimentalAnnotatedWriteHandler := annotated.NewHandler(
				h.options.DownsamplerAndWriter(),
				h.options.TagOptions(),
				instrumentOpts.MetricsScope().
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
		h.options.ClusterClient(),
		h.options.Config(),
		h.m3AggServiceOptions(),
		h.options.InstrumentOpts(),
	)
}

func (h *Handler) m3AggServiceOptions() *handleroptions.M3AggServiceOptions {
	clusters := h.options.Clusters()
	if clusters == nil {
		return nil
	}

	maxResolution := time.Duration(0)
	for _, ns := range clusters.ClusterNamespaces() {
		resolution := ns.Options().Attributes().Resolution
		if resolution > maxResolution {
			maxResolution = resolution
		}
	}

	if maxResolution == 0 {
		return nil
	}

	return &handleroptions.M3AggServiceOptions{
		MaxAggregationWindowSize: maxResolution,
	}
}

// Endpoints useful for profiling the service.
func (h *Handler) registerHealthEndpoints() {
	h.router.HandleFunc(healthURL, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(struct {
			Uptime string `json:"uptime"`
		}{
			Uptime: time.Since(h.options.CreatedAt()).String(),
		})
	}).Methods(http.MethodGet)
}

// Endpoints useful for profiling the service.
func (h *Handler) registerProfileEndpoints() {
	h.router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
}

// Endpoints useful for viewing routes directory.
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
