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
	"github.com/m3db/m3/src/x/headers"
	xhttp "github.com/m3db/m3/src/x/net/http"
	"github.com/m3db/m3/src/x/net/http/cors"

	"github.com/gorilla/mux"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/prometheus/util/httputil"
	"go.uber.org/zap"
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

		wrappedRouteFn = func(path string, handler http.Handler, methods ...string) error {
			return h.addRouteHandlerFn(h.router, path, wrapped(handler).ServeHTTP, methods...)
		}

		routeFn = func(path string, handler http.Handler, methods ...string) error {
			return h.addRouteHandlerFn(h.router, path, handler.ServeHTTP, methods...)
		}
	)

	if err := wrappedRouteFn(openapi.URL, openapi.NewDocHandler(instrumentOpts),
		openapi.HTTPMethod); err != nil {
		return err
	}

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

	promqlQueryHandler, err := prom.NewReadHandler(nativeSourceOpts,
		prom.WithEngine(h.options.PrometheusEngine()))
	if err != nil {
		return err
	}
	promqlInstantQueryHandler, err := prom.NewReadHandler(nativeSourceOpts,
		prom.WithInstantEngine(h.options.PrometheusEngine()))
	if err != nil {
		return err
	}
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

	if err := wrappedRouteFn(native.PromReadURL, h.options.QueryRouter(),
		native.PromReadHTTPMethods...); err != nil {
		return err
	}
	if err := wrappedRouteFn(native.PromReadInstantURL, h.options.InstantQueryRouter(),
		native.PromReadInstantHTTPMethods...); err != nil {
		return err
	}

	if err := wrappedRouteFn(native.PrometheusReadURL, promqlQueryHandler,
		native.PromReadHTTPMethods...); err != nil {
		return err
	}

	if err := wrappedRouteFn(native.PrometheusReadInstantURL, promqlInstantQueryHandler,
		native.PromReadInstantHTTPMethods...); err != nil {
		return err
	}

	if err := wrappedRouteFn(remote.PromReadURL, promRemoteReadHandler,
		remote.PromReadHTTPMethods...); err != nil {
		return err
	}
	if err := routeFn(remote.PromWriteURL, panicOnly(promRemoteWriteHandler),
		remote.PromWriteHTTPMethod); err != nil {
		return err
	}

	if err := wrappedRouteFn(native.M3QueryReadURL, nativePromReadHandler,
		native.PromReadHTTPMethods...); err != nil {
		return err
	}
	if err := wrappedRouteFn(native.M3QueryReadInstantURL, nativePromReadInstantHandler,
		native.PromReadInstantHTTPMethods...); err != nil {
		return err
	}

	// InfluxDB write endpoint.
	if err := wrappedRouteFn(influxdb.InfluxWriteURL, influxdb.NewInfluxWriterHandler(h.options),
		influxdb.InfluxWriteHTTPMethod); err != nil {
		return err
	}

	// Native M3 search and write endpoints.
	if err := wrappedRouteFn(handler.SearchURL, handler.NewSearchHandler(h.options),
		handler.SearchHTTPMethod); err != nil {
		return err
	}
	if err := wrappedRouteFn(m3json.WriteJSONURL, m3json.NewWriteJSONHandler(h.options),
		m3json.JSONWriteHTTPMethod); err != nil {
		return err
	}

	// Tag completion endpoints.
	if err := wrappedRouteFn(native.CompleteTagsURL, native.NewCompleteTagsHandler(h.options),
		native.CompleteTagsHTTPMethod); err != nil {
		return err
	}
	if err := wrappedRouteFn(remote.TagValuesURL, remote.NewTagValuesHandler(h.options),
		remote.TagValuesHTTPMethod); err != nil {
		return err
	}

	// List tag endpoints.
	if err := wrappedRouteFn(native.ListTagsURL, native.NewListTagsHandler(h.options),
		native.ListTagsHTTPMethods...); err != nil {
		return err
	}

	// Query parse endpoints.
	if err := wrappedRouteFn(native.PromParseURL, native.NewPromParseHandler(h.options),
		native.PromParseHTTPMethod); err != nil {
		return err
	}
	if err := wrappedRouteFn(native.PromThresholdURL, native.NewPromThresholdHandler(h.options),
		native.PromThresholdHTTPMethod); err != nil {
		return err
	}

	// Series match endpoints.
	if err := wrappedRouteFn(remote.PromSeriesMatchURL,
		remote.NewPromSeriesMatchHandler(h.options),
		remote.PromSeriesMatchHTTPMethods...); err != nil {
		return err
	}

	// Graphite endpoints.
	if err := wrappedRouteFn(graphite.ReadURL, graphite.NewRenderHandler(h.options),
		graphite.ReadHTTPMethods...); err != nil {
		return err
	}
	if err := wrappedRouteFn(graphite.FindURL, graphite.NewFindHandler(h.options),
		graphite.FindHTTPMethods...); err != nil {
		return err
	}

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
	if err := wrappedRouteFn(xdebug.DebugURL, debugWriter.HTTPHandler()); err != nil {
		return err
	}

	if clusterClient != nil {
		if err := database.RegisterRoutes(wrappedRouteFn, clusterClient,
			h.options.Config(), h.options.EmbeddedDbCfg(),
			serviceOptionDefaults, instrumentOpts); err != nil {
			return err
		}
		if err := placement.RegisterRoutes(routeFn, serviceOptionDefaults,
			placementOpts); err != nil {
			return err
		}
		if err := namespace.RegisterRoutes(wrappedRouteFn, clusterClient,
			h.options.Clusters(), serviceOptionDefaults, instrumentOpts); err != nil {
			return err
		}
		if err := topic.RegisterRoutes(wrappedRouteFn, clusterClient, config,
			instrumentOpts); err != nil {
			return err
		}

		// Experimental endpoints.
		if config.Experimental.Enabled {
			experimentalAnnotatedWriteHandler := annotated.NewHandler(
				h.options.DownsamplerAndWriter(),
				h.options.TagOptions(),
				instrumentOpts.MetricsScope().
					Tagged(remoteSource).
					Tagged(experimentalAPIGroup),
			)
			if err := wrappedRouteFn(annotated.WriteURL, experimentalAnnotatedWriteHandler,
				annotated.WriteHTTPMethod); err != nil {
				return err
			}
		}
	}

	if err := h.registerHealthEndpoints(); err != nil {
		return err
	}
	h.registerProfileEndpoints()
	if err := h.registerRoutesEndpoint(); err != nil {
		return err
	}

	// Register custom endpoints.
	for _, custom := range h.customHandlers {
		for _, method := range custom.Methods() {
			routeName := routeName(custom.Route(), method)
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
				if err := wrappedRouteFn(custom.Route(), customHandler, method); err != nil {
					return err
				}
			} else {
				route.Handler(wrapped(customHandler))
			}
		}
	}

	return nil
}

func (h *Handler) addRouteHandlerFn(
	router *mux.Router,
	path string,
	handlerFn http.HandlerFunc,
	methods ...string,
) error {
	for _, method := range methods {
		routeName := routeName(path, method)
		if previousRoute := router.Get(routeName); previousRoute != nil {
			return fmt.Errorf("route already exists: %s", routeName)
		}

		router.
			HandleFunc(path, handlerFn).
			Name(routeName).
			Methods(method)
	}

	return nil
}

func routeName(p string, method string) string {
	if method == "" {
		return p
	}
	return fmt.Sprintf("%s %s", p, method)
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
func (h *Handler) registerHealthEndpoints() error {
	return h.addRouteHandlerFn(h.router, healthURL, func(w http.ResponseWriter, r *http.Request) {
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
func (h *Handler) registerRoutesEndpoint() error {
	return h.addRouteHandlerFn(h.router, routesURL, func(w http.ResponseWriter, r *http.Request) {
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
