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

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/x/headers"
	xhttp "github.com/m3db/m3/src/x/http"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	metricsTypeTagName         = "type"
	metricsTypeTagDefaultValue = "coordinator"
)

var histogramTimerOptions = instrument.NewHistogramTimerOptions(
	instrument.HistogramTimerOptions{
		// Use sparse histogram timer buckets to not overload with latency metrics.
		HistogramBuckets: instrument.SparseHistogramTimerHistogramBuckets(),
	})

// MetricsOptions are the options for the metrics middleware.
type MetricsOptions struct {
	Config           config.MetricsMiddlewareConfiguration
	ParseQueryParams ParseQueryParams
	ParseOptions     promql.ParseOptions
}

// ResponseMetrics records metrics for the http response.
func ResponseMetrics(opts Options) mux.MiddlewareFunc {
	var (
		iOpts = opts.InstrumentOpts
		route = opts.Route
		cfg   = opts.Metrics.Config
	)

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

			metricsType := r.Header.Get(headers.CustomResponseMetricsType)
			if len(metricsType) == 0 {
				metricsType = metricsTypeTagDefaultValue
			}

			m := custom.getOrCreate(metricsType)
			classificationMetrics := m.classification
			metrics := m.route

			var tags classificationTags
			if cfg.LabelEndpointsClassification.Enabled() || cfg.QueryEndpointsClassification.Enabled() {
				if statusCodeTracking.Status == 200 {
					tags = classifyRequest(w, r, classificationMetrics, opts, start, path)
				} else {
					// NB(nate): Don't attempt to classify failed requests since they won't have a number of
					// series/metadata fetched and would skew the results of the smallest bucket if attempted,
					// as a missing "result" is considered a 0.
					tags = newClassificationTags()
				}
			}

			addLatencyStatus := false
			if cfg.AddStatusToLatencies {
				addLatencyStatus = true
			}

			counter, timer := metrics.metric(path, statusCodeTracking.Status, addLatencyStatus, tags)
			counter.Inc(1)
			timer.Record(d)
		})
	}
}

type responseMetrics struct {
	route          *routeMetrics
	classification *classificationMetrics
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

func (c *customMetrics) getOrCreate(value string) *responseMetrics {
	c.Lock()
	defer c.Unlock()

	if m, ok := c.metrics[value]; ok {
		return &m
	}

	subscope := c.instrumentOpts.MetricsScope().Tagged(map[string]string{
		metricsTypeTagName: value,
	})
	m := responseMetrics{
		route:          newRouteMetrics(subscope),
		classification: newClassificationMetrics(subscope),
	}

	c.metrics[value] = m
	return &m
}

type routeMetrics struct {
	sync.RWMutex
	scope   tally.Scope
	metrics map[routeMetricKey]routeMetric
	timers  map[routeMetricKey]tally.Timer
}

type routeMetricKey struct {
	path                   string
	status                 int
	resultsClassification  string
	durationClassification string
}

func newRouteMetricKey(
	path string,
	status int,
	tags classificationTags,
) routeMetricKey {
	return routeMetricKey{
		path:                   path,
		status:                 status,
		resultsClassification:  tags[resultsClassification],
		durationClassification: tags[durationClassification],
	}
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
	tags classificationTags,
) (tally.Counter, tally.Timer) {
	metricKey := newRouteMetricKey(path, status, tags)
	// NB: use 0 as the status for all latency operations unless status should be
	// explicitly included in written metrics.
	latencyStatus := 0
	if addLatencyStatus {
		latencyStatus = status
	}

	timerKey := newRouteMetricKey(path, latencyStatus, tags)
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

	allTags := make(map[string]string)
	for k, v := range tags {
		allTags[k] = v
	}
	allTags["path"] = path

	scopePath := m.scope.Tagged(allTags)
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
