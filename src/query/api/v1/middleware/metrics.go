// Copyright (c) 2021 Uber Technologies, Inc.
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

package middleware

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/x/headers"
	xhttp "github.com/m3db/m3/src/x/http"
	"github.com/m3db/m3/src/x/instrument"
)

var histogramTimerOptions = instrument.NewHistogramTimerOptions(
	instrument.HistogramTimerOptions{
		// Use sparse histogram timer buckets to not overload with latency metrics.
		HistogramBuckets: instrument.SparseHistogramTimerHistogramBuckets(),
	})

// ResponseMetrics records metrics for the http response.
func ResponseMetrics(opts options.MiddlewareOptions) mux.MiddlewareFunc {
	var (
		iOpts = opts.InstrumentOpts
		route = opts.Route
		cfg   = opts.Config
	)

	scope := tally.NoopScope
	if iOpts != nil {
		scope = iOpts.MetricsScope()
	}

	queryMetrics := newQueryInspectionMetrics(scope)
	metrics := newRouteMetrics(scope)

	custom := newCustomMetrics(iOpts)

	return func(base http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			statusCodeTracking := &xhttp.StatusCodeTracker{ResponseWriter: w}
			w = statusCodeTracking.WrappedResponseWriter()

			start := time.Now()
			base.ServeHTTP(w, r)
			d := time.Since(start)

			if !statusCodeTracking.WroteHeader {
				return
			}

			path, err := route.GetPathTemplate()
			if err != nil {
				path = "unknown"
			}

			customScopeName := r.Header.Get(headers.CustomResponseMetricsScope)
			if customScopeName != "" {
				m := custom.getOrCreate(customScopeName)
				queryMetrics = m.query
				metrics = m.route
			}

			querySize := inspectQuerySize(w, r, path, queryMetrics, cfg)

			addLatencyStatus := false
			if cfg != nil && cfg.AddStatusToLatencies {
				addLatencyStatus = true
			}

			counter, timer := metrics.metric(
				path, statusCodeTracking.Status, addLatencyStatus, querySize)
			counter.Inc(1)
			timer.Record(d)
		})
	}
}

type responseMetrics struct {
	route *routeMetrics
	query *queryInspectionMetrics
}

type customMetrics struct {
	sync.Mutex
	metrics        map[string]responseMetrics
	instrumentOpts instrument.Options
}

func newCustomMetrics(instrumentOpts instrument.Options) *customMetrics {
	return &customMetrics{
		metrics:        make(map[string]responseMetrics),
		instrumentOpts: instrumentOpts,
	}
}

func (c *customMetrics) getOrCreate(name string) *responseMetrics {
	c.Lock()
	defer c.Unlock()

	if m, ok := c.metrics[name]; ok {
		return &m
	}

	subscope := c.instrumentOpts.MetricsScope().SubScope(name)
	m := responseMetrics{
		route: newRouteMetrics(subscope),
		query: newQueryInspectionMetrics(subscope),
	}
	c.metrics[name] = m
	return &m
}

type routeMetrics struct {
	sync.RWMutex
	scope   tally.Scope
	metrics map[routeMetricKey]routeMetric
	timers  map[routeMetricKey]tally.Timer
}

type routeMetricKey struct {
	path   string
	size   string
	status int
}

type routeMetric struct {
	status tally.Counter
}

func newRouteMetrics(scope tally.Scope) *routeMetrics {
	return &routeMetrics{
		scope:   scope,
		metrics: make(map[routeMetricKey]routeMetric),
		timers:  make(map[routeMetricKey]tally.Timer),
	}
}

func (m *routeMetrics) metric(
	path string,
	status int,
	addLatencyStatus bool,
	querySize querySize,
) (tally.Counter, tally.Timer) {
	querySize.toTags()
	metricKey := querySize.toRouteMetricKey(path, status)
	// NB: use 0 as the status for all latency operations unless status should be
	// explicitly included in written metrics.
	latencyStatus := 0
	if addLatencyStatus {
		latencyStatus = status
	}

	timerKey := querySize.toRouteMetricKey(path, latencyStatus)
	m.RLock()
	metric, ok1 := m.metrics[metricKey]
	timer, ok2 := m.timers[timerKey]
	m.RUnlock()
	if ok1 && ok2 {
		return metric.status, timer
	}

	m.Lock()
	defer m.Unlock()

	metric, ok1 = m.metrics[metricKey]
	timer, ok2 = m.timers[timerKey]
	if ok1 && ok2 {
		return metric.status, timer
	}

	tags := querySize.toTags()
	tags["path"] = path
	scopePath := m.scope.Tagged(tags)
	scopePathAndStatus := scopePath.Tagged(map[string]string{
		"status": strconv.Itoa(status),
	})

	if !ok1 {
		metric = routeMetric{
			status: scopePathAndStatus.Counter("request"),
		}
		m.metrics[metricKey] = metric
	}
	if !ok2 {
		scope := scopePath
		if addLatencyStatus {
			scope = scopePathAndStatus
		}

		timer = instrument.NewTimer(scope, "latency", histogramTimerOptions)
		m.timers[timerKey] = timer
	}

	return metric.status, timer
}
