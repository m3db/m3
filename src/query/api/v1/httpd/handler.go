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
	"path"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

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
	"github.com/m3db/m3/src/x/headers"
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
	EngineHeaderName = headers.M3HeaderPrefix + "Engine"
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
	logger         *zap.Logger
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
	logger := handlerOptions.InstrumentOpts().Logger()

	return &Handler{
		router:         r,
		handler:        handlerWithMiddleware,
		options:        handlerOptions,
		customHandlers: customHandlers,
		logger:         logger,
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
	var (
		instrumentOpts = h.options.InstrumentOpts()

		// Wrap requests with response time logging as well as panic recovery.
		wrapped = func(n http.Handler) http.Handler {
			return logging.WithResponseTimeAndPanicErrorLogging(n, instrumentOpts)
		}

		panicOnly = func(n http.Handler) http.Handler {
			return logging.WithPanicErrorResponder(n, instrumentOpts)
		}

		wrappedRouteFn = func(path string, handler http.Handler, methods ...string) {
			h.addRoute(h.router, path, wrapped(handler), methods)
		}

		routeFn = func(path string, handler http.Handler, methods ...string) {
			h.addRoute(h.router, path, handler, methods)
		}
	)

	wrappedRouteFn(openapi.URL, openapi.NewDocHandler(instrumentOpts), openapi.HTTPMethod)

	h.router.PathPrefix(openapi.StaticURLPrefix).
		Handler(wrapped(openapi.StaticHandler())).
		Name(openapi.StaticURLPrefix)


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

	opts := prom.Options{
		PromQLEngine: h.options.PrometheusEngine(),
	}
	promqlQueryHandler := prom.NewReadHandler(opts, nativeSourceOpts)
	promqlInstantQueryHandler := prom.NewReadInstantHandler(opts, nativeSourceOpts)
	nativePromReadHandler := native.NewPromReadHandler(nativeSourceOpts)
	nativePromReadInstantHandler := native.NewPromReadInstantHandler(nativeSourceOpts)

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

	wrappedRouteFn(native.PromReadURL, h.options.QueryRouter(),
		native.PromReadHTTPMethods...)
	wrappedRouteFn(native.PromReadInstantURL, h.options.InstantQueryRouter(),
		native.PromReadInstantHTTPMethods...)

	wrappedRouteFn(native.PrometheusReadURL, promqlQueryHandler,
		native.PromReadHTTPMethods...)
	wrappedRouteFn(native.PrometheusReadInstantURL, promqlInstantQueryHandler,
		native.PromReadInstantHTTPMethods...)

	wrappedRouteFn(remote.PromReadURL, promRemoteReadHandler,
		remote.PromReadHTTPMethods...)
	routeFn(remote.PromWriteURL, panicOnly(promRemoteWriteHandler),
		remote.PromWriteHTTPMethod)

	wrappedRouteFn(native.M3QueryReadURL, nativePromReadHandler,
		native.PromReadHTTPMethods...)
	wrappedRouteFn(native.M3QueryReadInstantURL, nativePromReadInstantHandler,
		native.PromReadInstantHTTPMethods...)

	// InfluxDB write endpoint.
	wrappedRouteFn(influxdb.InfluxWriteURL, influxdb.NewInfluxWriterHandler(h.options),
		influxdb.InfluxWriteHTTPMethod)

	// Native M3 search and write endpoints.
	wrappedRouteFn(handler.SearchURL, handler.NewSearchHandler(h.options),
		handler.SearchHTTPMethod)
	wrappedRouteFn(m3json.WriteJSONURL, m3json.NewWriteJSONHandler(h.options),
		m3json.JSONWriteHTTPMethod)

	// Tag completion endpoints.
	wrappedRouteFn(native.CompleteTagsURL, native.NewCompleteTagsHandler(h.options),
		native.CompleteTagsHTTPMethod)
	wrappedRouteFn(remote.TagValuesURL, remote.NewTagValuesHandler(h.options),
		remote.TagValuesHTTPMethod)

	// List tag endpoints.
	wrappedRouteFn(native.ListTagsURL, native.NewListTagsHandler(h.options),
		native.ListTagsHTTPMethods...)

	// Query parse endpoints.
	wrappedRouteFn(native.PromParseURL, native.NewPromParseHandler(h.options),
		native.PromParseHTTPMethod)
	wrappedRouteFn(native.PromThresholdURL, native.NewPromThresholdHandler(h.options),
		native.PromThresholdHTTPMethod)

	// Series match endpoints.
	wrappedRouteFn(remote.PromSeriesMatchURL, remote.NewPromSeriesMatchHandler(h.options),
		remote.PromSeriesMatchHTTPMethods...)

	// Graphite endpoints.
	wrappedRouteFn(graphite.ReadURL, graphite.NewRenderHandler(h.options),
		graphite.ReadHTTPMethods...)
	wrappedRouteFn(graphite.FindURL, graphite.NewFindHandler(h.options),
		graphite.FindHTTPMethods...)

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
	wrappedRouteFn(xdebug.DebugURL, debugWriter.HTTPHandler())

	if clusterClient != nil {
		err = database.RegisterRoutes(wrappedRouteFn, clusterClient,
			h.options.Config(), h.options.EmbeddedDbCfg(),
			serviceOptionDefaults, instrumentOpts)
		if err != nil {
			return err
		}
		placement.RegisterRoutes(routeFn, serviceOptionDefaults, placementOpts)
		namespace.RegisterRoutes(wrappedRouteFn, clusterClient,
			h.options.Clusters(), serviceOptionDefaults, instrumentOpts)
		topic.RegisterRoutes(wrappedRouteFn, clusterClient, config, instrumentOpts)

		// Experimental endpoints.
		if config.Experimental.Enabled {
			experimentalAnnotatedWriteHandler := annotated.NewHandler(
				h.options.DownsamplerAndWriter(),
				h.options.TagOptions(),
				instrumentOpts.MetricsScope().
					Tagged(remoteSource).
					Tagged(experimentalAPIGroup),
			)
			wrappedRouteFn(annotated.WriteURL, experimentalAnnotatedWriteHandler,
				annotated.WriteHTTPMethod)
		}
	}

	h.registerHealthEndpoints()
	h.registerProfileEndpoints()
	h.registerRoutesEndpoint()

	// Register custom endpoints.
	for _, custom := range h.customHandlers {
		routeName := routeName(custom.Route(), custom.Methods())
		route := h.router.Get(routeName)
		var prevHandler http.Handler
		if route != nil {
			prevHandler = route.GetHandler()
		}
		customHandler, err := custom.Handler(nativeSourceOpts, prevHandler)
		if err != nil {
			return fmt.Errorf("failed to register custom handler with path %s: %w",
				routeName, err)
		}

		if route == nil {
			wrappedRouteFn(custom.Route(), customHandler, custom.Methods()...)
		} else {
			route.Handler(wrapped(customHandler))
		}
	}

	return nil
}

func (h *Handler) addRoute(router *mux.Router, path string, handler http.Handler, methods []string) {
	h.addRouteHandlerFn(router, path, handler.ServeHTTP, methods...)
}

func (h *Handler) addRouteHandlerFn(
	router *mux.Router,
	path string,
	handlerFn http.HandlerFunc,
	methods ...string,
) {
	routeName := routeName(path, methods)
	if previousRoute := router.Get(routeName); previousRoute != nil {
		h.logger.Warn("route already exists",
			zap.String("routeName", routeName))
		return
	}

	route := router.
		HandleFunc(path, handlerFn).
		Name(routeName)

	if len(methods) < 1 {
		h.logger.Warn("adding route without methods",
			zap.String("routeName", routeName))
		return
	}

	route.Methods(methods...)
}

func routeName(p string, methods []string) string {
	if len(methods) > 1 {
		distinctMethods := distinct(methods)
		return path.Join(p, strings.Join(distinctMethods, ":"))
	}

	return path.Join(p, strings.Join(methods, ":"))
}

func distinct(arr []string) []string {
	if len(arr) <= 1 {
		return arr
	}

	sort.Strings(arr)

	var prev string
	distinct := arr[:0]
	for i, entry := range arr {
		if i == 0 || prev != entry {
			distinct = append(distinct, entry)
		}

		prev = entry
	}

	return distinct
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
	h.addRouteHandlerFn(h.router, healthURL, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(struct {
			Uptime string `json:"uptime"`
		}{
			Uptime: time.Since(h.options.CreatedAt()).String(),
		})
	}, http.MethodGet)
}

// Endpoints useful for profiling the service.
func (h *Handler) registerProfileEndpoints() {
	h.router.
		PathPrefix("/debug/pprof/").
		Handler(http.DefaultServeMux).
		Name("/debug/pprof/")
}

// Endpoints useful for viewing routes directory.
func (h *Handler) registerRoutesEndpoint() {
	h.addRouteHandlerFn(h.router, routesURL, func(w http.ResponseWriter, r *http.Request) {
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
			xhttp.WriteError(w, err)
			return
		}
		json.NewEncoder(w).Encode(struct {
			Routes []string `json:"routes"`
		}{
			Routes: routes,
		})
	}, http.MethodGet)
}
