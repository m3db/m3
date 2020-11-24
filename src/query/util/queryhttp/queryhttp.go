// Copyright (c) 2020 Uber Technologies, Inc.
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

package queryhttp

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/uber-go/tally"

	"github.com/gorilla/mux"
)

var (
	histogramTimerOptions = instrument.NewHistogramTimerOptions(
		instrument.HistogramTimerOptions{
			// Use sparse histogram timer buckets to not overload with latency metrics.
			HistogramBuckets: instrument.SparseHistogramTimerHistogramBuckets(),
		})
)

// NewEndpointRegistry returns a new endpoint registry.
func NewEndpointRegistry(
	router *mux.Router,
	instrumentOpts instrument.Options,
) *EndpointRegistry {
	return &EndpointRegistry{
		router: router,
		instrumentOpts: instrumentOpts.SetMetricsScope(
			instrumentOpts.MetricsScope().SubScope("http_handler")),
		registered: make(map[routeKey]*mux.Route),
	}
}

// EndpointRegistry is an endpoint registry that can register routes
// and instrument them.
type EndpointRegistry struct {
	router         *mux.Router
	instrumentOpts instrument.Options
	registered     map[routeKey]*mux.Route
}

type routeKey struct {
	path       string
	pathPrefix string
	method     string
}

// RegisterOptions are options for registering a handler.
type RegisterOptions struct {
	Path       string
	PathPrefix string
	Handler    http.Handler
	Methods    []string
}

// Register registers an endpoint.
func (r *EndpointRegistry) Register(
	opts RegisterOptions,
	middlewareOpts ...logging.MiddlewareOption,
) error {
	// Wrap requests with response time logging as well as panic recovery.
	var (
		route             *mux.Route
		metrics           = newRouteMetrics(r.instrumentOpts)
		middlewareOptions []logging.MiddlewareOption
	)
	postRequestOption := logging.WithPostRequestMiddleware(
		logging.PostRequestMiddleware(func(
			r *http.Request,
			meta logging.RequestMiddlewareMetadata,
		) {
			if !meta.WroteHeader {
				return
			}

			p, err := route.GetPathTemplate()
			if err != nil {
				p = "unknown"
			}

			counter, timer := metrics.metric(p, meta.StatusCode)
			counter.Inc(1)
			timer.Record(meta.Duration)
		}))
	middlewareOptions = append(middlewareOptions, postRequestOption)
	middlewareOptions = append(middlewareOptions, middlewareOpts...)

	wrapped := func(n http.Handler) http.Handler {
		return logging.WithResponseTimeAndPanicErrorLogging(n, r.instrumentOpts,
			middlewareOptions...)
	}

	handler := wrapped(opts.Handler)
	if p := opts.Path; p != "" && len(opts.Methods) > 0 {
		for _, method := range opts.Methods {
			key := routeKey{
				path:   p,
				method: method,
			}
			if _, ok := r.registered[key]; ok {
				return fmt.Errorf("route already exists: path=%s, method=%s",
					p, method)
			}

			route = r.router.HandleFunc(p, handler.ServeHTTP).Methods(method)
			r.registered[key] = route
		}
	} else if p := opts.PathPrefix; p != "" {
		key := routeKey{
			pathPrefix: p,
		}
		if _, ok := r.registered[key]; ok {
			return fmt.Errorf("route already exists: pathPrefix=%s", p)
		}
		route = r.router.PathPrefix(p).Handler(handler)
		r.registered[key] = route
	} else {
		return fmt.Errorf("no path and methods or path prefix set: +%v", opts)
	}

	return nil
}

// RegisterPathsOptions is options for registering multiple paths
// with the same handler.
type RegisterPathsOptions struct {
	Handler http.Handler
	Methods []string
}

// RegisterPaths registers multiple paths for the same handler.
func (r *EndpointRegistry) RegisterPaths(
	paths []string,
	opts RegisterPathsOptions,
	middlewareOpts ...logging.MiddlewareOption,
) error {
	for _, p := range paths {
		if err := r.Register(RegisterOptions{
			Path:    p,
			Handler: opts.Handler,
			Methods: opts.Methods,
		}); err != nil {
			return err
		}
	}
	return nil
}

// PathRoute resolves a registered route that was registered by path and method,
// not by path prefix.
func (r *EndpointRegistry) PathRoute(path, method string) (*mux.Route, bool) {
	key := routeKey{
		path:   path,
		method: method,
	}
	h, ok := r.registered[key]
	return h, ok
}

// PathPrefixRoute resolves a registered route that was registered by path
// prefix, not by path and method.
func (r *EndpointRegistry) PathPrefixRoute(pathPrefix string) (*mux.Route, bool) {
	key := routeKey{
		pathPrefix: pathPrefix,
	}
	h, ok := r.registered[key]
	return h, ok
}

// Walk walks the router and all its sub-routers, calling walkFn for each route
// in the tree. The routes are walked in the order they were added. Sub-routers
// are explored depth-first.
func (r *EndpointRegistry) Walk(walkFn mux.WalkFunc) error {
	return r.router.Walk(walkFn)
}

func routeName(p string, method string) string {
	if method == "" {
		return p
	}
	return fmt.Sprintf("%s %s", p, method)
}

type routeMetrics struct {
	sync.RWMutex
	instrumentOpts instrument.Options
	metrics        map[routeMetricKey]routeMetric
	timers         map[string]tally.Timer
}

type routeMetricKey struct {
	path   string
	status int
}

type routeMetric struct {
	status tally.Counter
}

func newRouteMetrics(instrumentOpts instrument.Options) *routeMetrics {
	return &routeMetrics{
		instrumentOpts: instrumentOpts,
		metrics:        make(map[routeMetricKey]routeMetric),
		timers:         make(map[string]tally.Timer),
	}
}

func (m *routeMetrics) metric(path string, status int) (tally.Counter, tally.Timer) {
	key := routeMetricKey{
		path:   path,
		status: status,
	}
	m.RLock()
	metric, ok1 := m.metrics[key]
	timer, ok2 := m.timers[path]
	m.RUnlock()
	if ok1 && ok2 {
		return metric.status, timer
	}

	m.Lock()
	defer m.Unlock()

	metric, ok1 = m.metrics[key]
	timer, ok2 = m.timers[path]
	if ok1 && ok2 {
		return metric.status, timer
	}

	scopePath := m.instrumentOpts.MetricsScope().Tagged(map[string]string{
		"path": path,
	})

	scopePathAndStatus := scopePath.Tagged(map[string]string{
		"status": strconv.Itoa(status),
	})

	if !ok1 {
		metric = routeMetric{
			status: scopePathAndStatus.Counter("request"),
		}
		m.metrics[key] = metric
	}
	if !ok2 {
		timer = instrument.NewTimer(scopePath, "latency", histogramTimerOptions)
		m.timers[path] = timer
	}

	return metric.status, timer
}
