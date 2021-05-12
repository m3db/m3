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

	xhttp "github.com/m3db/m3/src/x/http"
	"github.com/m3db/m3/src/x/instrument"
)

var histogramTimerOptions = instrument.NewHistogramTimerOptions(
	instrument.HistogramTimerOptions{
		// Use sparse histogram timer buckets to not overload with latency metrics.
		HistogramBuckets: instrument.SparseHistogramTimerHistogramBuckets(),
	})

// ResponseMetrics records metrics for the http response.
func ResponseMetrics(iOpts instrument.Options) mux.MiddlewareFunc {
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

			metrics := newRouteMetrics(iOpts)
			counter, timer := metrics.metric(r.URL.RequestURI(), statusCodeTracking.Status)
			counter.Inc(1)
			timer.Record(d)
		})
	}
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
