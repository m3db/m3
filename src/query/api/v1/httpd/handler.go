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

	// needed for pprof handler registration
	_ "net/http/pprof"
	"time"

	"github.com/gorilla/mux"
	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/placementhandler"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/database"
	"github.com/m3db/m3/src/query/api/v1/handler/graphite"
	"github.com/m3db/m3/src/query/api/v1/handler/influxdb"
	m3json "github.com/m3db/m3/src/query/api/v1/handler/json"
	"github.com/m3db/m3/src/query/api/v1/handler/namespace"
	"github.com/m3db/m3/src/query/api/v1/handler/openapi"
	"github.com/m3db/m3/src/query/api/v1/handler/prom"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote"
	"github.com/m3db/m3/src/query/api/v1/handler/topic"
	"github.com/m3db/m3/src/query/api/v1/middleware"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/util/queryhttp"
	xdebug "github.com/m3db/m3/src/x/debug"
	extdebug "github.com/m3db/m3/src/x/debug/ext"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

const (
	healthURL = "/health"
	routesURL = "/routes"

	// EngineURLParam defines query url parameter which is used to switch between
	// prometheus and m3query engines.
	EngineURLParam = "engine"
)

var (
	remoteSource = map[string]string{"source": "remote"}
	nativeSource = map[string]string{"source": "native"}

	v1APIGroup = map[string]string{"api_group": "v1"}
)

// Handler represents the top-level HTTP handler.
type Handler struct {
	registry         *queryhttp.EndpointRegistry
	handler          *mux.Router
	options          options.HandlerOptions
	customHandlers   []options.CustomHandler
	logger           *zap.Logger
	middlewareConfig config.MiddlewareConfiguration
}

// Router returns the http handler registered with all relevant routes for query.
func (h *Handler) Router() http.Handler {
	return h.handler
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(
	handlerOptions options.HandlerOptions,
	middlewareConfig config.MiddlewareConfiguration,
	customHandlers ...options.CustomHandler,
) *Handler {
	var (
		r        = mux.NewRouter()
		logger   = handlerOptions.InstrumentOpts().Logger()
		registry = queryhttp.NewEndpointRegistry(r)
	)
	return &Handler{
		registry:         registry,
		handler:          r,
		options:          handlerOptions,
		customHandlers:   customHandlers,
		logger:           logger,
		middlewareConfig: middlewareConfig,
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
		prom.WithEngine(h.options.PrometheusEngineFn()))
	if err != nil {
		return err
	}
	promqlInstantQueryHandler, err := prom.NewReadHandler(nativeSourceOpts,
		prom.WithInstantEngine(h.options.PrometheusEngineFn()))
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
		Path:               native.PromReadURL,
		Handler:            h.options.QueryRouter(),
		Methods:            native.PromReadHTTPMethods,
		MiddlewareOverride: native.WithRangeQueryParamsAndRangeRewriting,
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:               native.PromReadInstantURL,
		Handler:            h.options.InstantQueryRouter(),
		Methods:            native.PromReadInstantHTTPMethods,
		MiddlewareOverride: native.WithInstantQueryParamsAndRangeRewriting,
	}); err != nil {
		return err
	}

	// Prometheus endpoints.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:               "/prometheus" + native.PromReadURL,
		Handler:            promqlQueryHandler,
		Methods:            native.PromReadHTTPMethods,
		MiddlewareOverride: native.WithRangeQueryParamsAndRangeRewriting,
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:               "/prometheus" + native.PromReadInstantURL,
		Handler:            promqlInstantQueryHandler,
		Methods:            native.PromReadInstantHTTPMethods,
		MiddlewareOverride: native.WithInstantQueryParamsAndRangeRewriting,
	}); err != nil {
		return err
	}

	// M3Query endpoints.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:               "/m3query" + native.PromReadURL,
		Handler:            nativePromReadHandler,
		Methods:            native.PromReadHTTPMethods,
		MiddlewareOverride: native.WithRangeQueryParamsAndRangeRewriting,
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:               "/m3query" + native.PromReadInstantURL,
		Handler:            nativePromReadInstantHandler,
		Methods:            native.PromReadInstantHTTPMethods,
		MiddlewareOverride: native.WithInstantQueryParamsAndRangeRewriting,
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
		MiddlewareOverride: middleware.WithNoResponseLogging,
	}); err != nil {
		return err
	}

	// InfluxDB write endpoint.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    influxdb.InfluxWriteURL,
		Handler: influxdb.NewInfluxWriterHandler(h.options),
		Methods: methods(influxdb.InfluxWriteHTTPMethod),
		// Register with no response logging for write calls since so frequent.
		MiddlewareOverride: middleware.WithNoResponseLogging,
	}); err != nil {
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
		Path:               native.CompleteTagsURL,
		Handler:            native.NewCompleteTagsHandler(h.options),
		Methods:            methods(native.CompleteTagsHTTPMethod),
		MiddlewareOverride: native.WithQueryParams,
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:               remote.TagValuesURL,
		Handler:            remote.NewTagValuesHandler(h.options),
		Methods:            methods(remote.TagValuesHTTPMethod),
		MiddlewareOverride: native.WithQueryParams,
	}); err != nil {
		return err
	}

	// List tag endpoints.
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:               native.ListTagsURL,
		Handler:            native.NewListTagsHandler(h.options),
		Methods:            native.ListTagsHTTPMethods,
		MiddlewareOverride: native.WithQueryParams,
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
		Path:               route.SeriesMatchURL,
		Handler:            remote.NewPromSeriesMatchHandler(h.options),
		Methods:            remote.PromSeriesMatchHTTPMethods,
		MiddlewareOverride: native.WithQueryParams,
	}); err != nil {
		return err
	}

	// Graphite routable endpoints.
	h.options.GraphiteRenderRouter().Setup(options.GraphiteRenderRouterOptions{
		RenderHandler: graphite.NewRenderHandler(h.options).ServeHTTP,
	})
	h.options.GraphiteFindRouter().Setup(options.GraphiteFindRouterOptions{
		FindHandler: graphite.NewFindHandler(h.options).ServeHTTP,
	})
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    graphite.ReadURL,
		Handler: h.options.GraphiteRenderRouter(),
		Methods: graphite.ReadHTTPMethods,
	}); err != nil {
		return err
	}
	if err := h.registry.Register(queryhttp.RegisterOptions{
		Path:    graphite.FindURL,
		Handler: h.options.GraphiteFindRouter(),
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

	debugWriter, err := extdebug.NewPlacementAndNamespaceZipWriterWithDefaultSources(
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
			h.options.Config(), h.options.EmbeddedDBCfg(),
			serviceOptionDefaults, instrumentOpts,
			h.options.NamespaceValidator(), h.options.KVStoreProtoParser())
		if err != nil {
			return err
		}

		routes := placementhandler.MakeRoutes(serviceOptionDefaults, placementOpts)
		for _, route := range routes {
			err := h.registry.RegisterPaths(route.Paths, queryhttp.RegisterPathsOptions{
				Handler: route.Handler,
				Methods: route.Methods,
			})
			if err != nil {
				return err
			}
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

	customMiddle := make(map[*mux.Route]middleware.OverrideOptions)
	// Register custom endpoints last to have these conflict with
	// any existing routes.
	for _, custom := range h.customHandlers {
		for _, method := range custom.Methods() {
			var prevHandler http.Handler
			entry, prevRoute := h.registry.PathEntry(custom.Route(), method)
			if prevRoute {
				prevHandler = entry.GetHandler()
			}

			handler, err := custom.Handler(nativeSourceOpts, prevHandler)
			if err != nil {
				return err
			}

			if !prevRoute {
				if err := h.registry.Register(queryhttp.RegisterOptions{
					Path:               custom.Route(),
					Handler:            handler,
					Methods:            methods(method),
					MiddlewareOverride: custom.MiddlewareOverride(),
				}); err != nil {
					return err
				}
			} else {
				customMiddle[entry] = custom.MiddlewareOverride()
				entry.Handler(handler)
			}
		}
	}

	// NB: the double http_handler was accidentally introduced and now we are
	// stuck with it for backwards compatibility.
	middleIOpts := instrumentOpts.SetMetricsScope(
		h.options.InstrumentOpts().MetricsScope().SubScope("http_handler_http_handler"))

	// Apply middleware after the custom handlers have overridden the previous handlers so the middleware functions
	// are dispatched before the custom handler.
	// req -> middleware fns -> custom handler -> previous handler.
	err = h.registry.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		handler := route.GetHandler()
		opts := middleware.Options{
			InstrumentOpts: middleIOpts,
			Route:          route,
			Clock:          clockwork.NewRealClock(),
			Logging:        middleware.NewLoggingOptions(h.middlewareConfig.Logging),
			Metrics: middleware.MetricsOptions{
				Config: h.middlewareConfig.Metrics,
				ParseOptions: promql.NewParseOptions().
					SetRequireStartEndTime(h.options.Config().Query.RequireLabelsEndpointStartEndTime).
					SetNowFn(h.options.NowFn()),
			},
			PrometheusRangeRewrite: middleware.PrometheusRangeRewriteOptions{
				FetchOptionsBuilder:  h.options.FetchOptionsBuilder(),
				ResolutionMultiplier: h.middlewareConfig.Prometheus.ResolutionMultiplier,
				DefaultLookback:      h.options.DefaultLookback(),
				Storage:              h.options.Storage(),
				PrometheusEngineFn:   h.options.PrometheusEngineFn(),
			},
		}
		override := h.registry.MiddlewareOpts(route)
		if override != nil {
			opts = override(opts)
		}
		if customMiddle[route] != nil {
			opts = customMiddle[route](opts)
		}
		middle := h.options.RegisterMiddleware()(opts)

		// iterate through in reverse order so each Middleware fn gets the proper next handler to dispatch. this ensures the
		// Middleware is dispatched in the expected order (first -> last).
		for i := len(middle) - 1; i >= 0; i-- {
			handler = middle[i].Middleware(handler)
		}

		route.Handler(handler)
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (h *Handler) placementOpts() (placementhandler.HandlerOptions, error) {
	return placementhandler.NewHandlerOptions(
		h.options.ClusterClient(),
		h.options.Config().ClusterManagement.Placement,
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
				Now    string `json:"now"`
			}{
				Uptime: time.Since(h.options.CreatedAt()).String(),
				Now:    time.Now().String(),
			})
		}),
		Methods: methods(http.MethodGet),
	})
}

// Endpoints useful for profiling the service.
func (h *Handler) registerProfileEndpoints() error {
	debugHandler := http.NewServeMux()
	xdebug.RegisterPProfHandlers(debugHandler)

	return h.registry.Register(queryhttp.RegisterOptions{
		PathPrefix: "/debug/pprof",
		Handler:    debugHandler,
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
