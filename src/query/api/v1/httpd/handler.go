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
	"github.com/m3db/m3/src/query/util/queryhttp"
	xdebug "github.com/m3db/m3/src/x/debug"
	"github.com/m3db/m3/src/x/headers"
	xhttp "github.com/m3db/m3/src/x/net/http"
	"github.com/m3db/m3/src/x/net/http/cors"

	"github.com/gorilla/mux"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
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
	registry       *queryhttp.EndpointRegistry
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

	instrumentOpts := handlerOptions.InstrumentOpts().SetMetricsScope(
		handlerOptions.InstrumentOpts().MetricsScope().SubScope("http_handler"))
	return &Handler{
		registry:       queryhttp.NewEndpointRegistry(r, instrumentOpts),
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
	instrumentOpts := h.options.InstrumentOpts()

	// OpenAPI.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    openapi.URL,
		Handler: openapi.NewDocHandler(instrumentOpts),
		Methods: methods(openapi.HTTPMethod),
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		PathPrefix: openapi.StaticURLPrefix,
		Handler:    openapi.StaticHandler(),
	}); err != nil {
		return err
	}

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

	// Query routable endpoints.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    native.PromReadURL,
		Handler: h.options.QueryRouter(),
		Methods: native.PromReadHTTPMethods,
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    native.PromReadInstantURL,
		Handler: h.options.InstantQueryRouter(),
		Methods: native.PromReadInstantHTTPMethods,
	}); err != nil {
		return err
	}

	// Prometheus endpoints.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    "/prometheus" + native.PromReadURL,
		Handler: promqlQueryHandler,
		Methods: native.PromReadHTTPMethods,
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    "/prometheus" + native.PromReadInstantURL,
		Handler: promqlInstantQueryHandler,
		Methods: native.PromReadInstantHTTPMethods,
	}); err != nil {
		return err
	}

	// M3Query endpoints.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    "/m3query" + native.PromReadURL,
		Handler: nativePromReadHandler,
		Methods: native.PromReadHTTPMethods,
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    "/m3query" + native.PromReadInstantURL,
		Handler: nativePromReadInstantHandler,
		Methods: native.PromReadInstantHTTPMethods,
	}); err != nil {
		return err
	}

	// Prometheus remote read and write endpoints.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    remote.PromReadURL,
		Handler: promRemoteReadHandler,
		Methods: remote.PromReadHTTPMethods,
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    remote.PromWriteURL,
		Handler: promRemoteWriteHandler,
		Methods: methods(remote.PromWriteHTTPMethod),
		// Register with no response logging for write calls since so frequent.
	}, logging.WithNoResponseLog()); err != nil {
		return err
	}

	// InfluxDB write endpoint.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    influxdb.InfluxWriteURL,
		Handler: influxdb.NewInfluxWriterHandler(h.options),
		Methods: methods(influxdb.InfluxWriteHTTPMethod),
		// Register with no response logging for write calls since so frequent.
	}, logging.WithNoResponseLog()); err != nil {
		return err
	}

	// Native M3 search and write endpoints.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    handler.SearchURL,
		Handler: handler.NewSearchHandler(h.options),
		Methods: methods(handler.SearchHTTPMethod),
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    m3json.WriteJSONURL,
		Handler: m3json.NewWriteJSONHandler(h.options),
		Methods: methods(m3json.JSONWriteHTTPMethod),
	}); err != nil {
		return err
	}

	// Readiness endpoint.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    handler.ReadyURL,
		Handler: handler.NewReadyHandler(h.options),
		Methods: methods(handler.ReadyHTTPMethod),
	}); err != nil {
		return err
	}

	// Tag completion endpoints.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    native.CompleteTagsURL,
		Handler: native.NewCompleteTagsHandler(h.options),
		Methods: methods(native.CompleteTagsHTTPMethod),
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    remote.TagValuesURL,
		Handler: remote.NewTagValuesHandler(h.options),
		Methods: methods(remote.TagValuesHTTPMethod),
	}); err != nil {
		return err
	}

	// List tag endpoints.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    native.ListTagsURL,
		Handler: native.NewListTagsHandler(h.options),
		Methods: native.ListTagsHTTPMethods,
	}); err != nil {
		return err
	}

	// Query parse endpoints.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    native.PromParseURL,
		Handler: native.NewPromParseHandler(h.options),
		Methods: methods(native.PromParseHTTPMethod),
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    native.PromThresholdURL,
		Handler: native.NewPromThresholdHandler(h.options),
		Methods: methods(native.PromThresholdHTTPMethod),
	}); err != nil {
		return err
	}

	// Series match endpoints.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    remote.PromSeriesMatchURL,
		Handler: remote.NewPromSeriesMatchHandler(h.options),
		Methods: remote.PromSeriesMatchHTTPMethods,
	}); err != nil {
		return err
	}

	// Graphite endpoints.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    graphite.ReadURL,
		Handler: graphite.NewRenderHandler(h.options),
		Methods: graphite.ReadHTTPMethods,
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    graphite.FindURL,
		Handler: graphite.NewFindHandler(h.options),
		Methods: graphite.FindHTTPMethods,
	}); err != nil {
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
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    xdebug.DebugURL,
		Handler: debugWriter.HTTPHandler(),
		Methods: methods(xdebug.DebugMethod),
	}); err != nil {
		return err
	}

	if clusterClient != nil {
		err = database.RegisterRoutes(h.registry, clusterClient,
			h.options.Config(), h.options.EmbeddedDbCfg(),
			serviceOptionDefaults, instrumentOpts, h.options.NamespaceValidator())
		if err != nil {
			return err
		}

		err = placement.RegisterRoutes(h.registry,
			serviceOptionDefaults, placementOpts)
		if err != nil {
			return err
		}

		err = namespace.RegisterRoutes(h.registry, clusterClient,
			h.options.Clusters(), serviceOptionDefaults, instrumentOpts,
			h.options.NamespaceValidator())
		if err != nil {
			return err
		}

		err = topic.RegisterRoutes(h.registry, clusterClient, config, instrumentOpts)
		if err != nil {
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
			if err := h.registry.Register(queryhttp.RegisterOptions{
				Path:    annotated.WriteURL,
				Handler: experimentalAnnotatedWriteHandler,
				Methods: methods(annotated.WriteHTTPMethod),
			}); err != nil {
				return err
			}
		}
	}

	if err := h.registerHealthEndpoints(); err != nil {
		return err
	}
	if err := h.registerProfileEndpoints(); err != nil {
		return err
	}
	if err := h.registerRoutesEndpoint(); err != nil {
		return err
	}

	// Register custom endpoints last to have these conflict with
	// any existing routes.
	for _, custom := range h.customHandlers {
		for _, method := range custom.Methods() {
			var prevHandler http.Handler
			route, prevRoute := h.registry.PathRoute(custom.Route(), method)
			if prevRoute {
				prevHandler = route.GetHandler()
			}

			handler, err := custom.Handler(nativeSourceOpts, prevHandler)
			if err != nil {
				return err
			}

			if !prevRoute {
				if err := h.registry.Register(queryhttp.RegisterOptions{
					Path:    custom.Route(),
					Handler: handler,
					Methods: methods(method),
				}); err != nil {
					return err
				}
			} else {
				// Do not re-instrument this route since the prev handler
				// is already instrumented.
				route.Handler(handler)
			}
		}
	}

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
func (h *Handler) registerHealthEndpoints() error {
	return h.registry.Register(queryhttp.RegisterOptions{
		Path: healthURL,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(struct {
				Uptime string `json:"uptime"`
			}{
				Uptime: time.Since(h.options.CreatedAt()).String(),
			})
		}),
		Methods: methods(http.MethodGet),
	})
}

// Endpoints useful for profiling the service.
func (h *Handler) registerProfileEndpoints() error {
	return h.registry.Register(queryhttp.RegisterOptions{
		PathPrefix: "/debug/pprof",
		Handler:    http.DefaultServeMux,
	})
}

// Endpoints useful for viewing routes directory.
func (h *Handler) registerRoutesEndpoint() error {
	return h.registry.Register(queryhttp.RegisterOptions{
		Path: routesURL,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var routes []string
			err := h.registry.Walk(
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
		}),
		Methods: methods(http.MethodGet),
	})
}

func methods(str ...string) []string {
	return str
}
