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
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/x/headers"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/tallytest"
)

func TestResponseMetrics(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	iOpts := instrument.NewOptions().SetMetricsScope(scope)

	r := mux.NewRouter()
	route := r.NewRoute()
	opts := Options{
		InstrumentOpts: iOpts,
		Route:          route,
	}

	h := ResponseMetrics(opts).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	route.Path("/test").Handler(h)

	server := httptest.NewServer(r)
	defer server.Close()

	resp, err := server.Client().Get(server.URL + "/test?foo=bar") //nolint: noctx
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	snapshot := scope.Snapshot()
	tallytest.AssertCounterValue(t, 1, snapshot, "request", map[string]string{
		"path":   "/test",
		"status": "200",
		"type":   "coordinator",
	})

	hist := snapshot.Histograms()
	require.True(t, len(hist) == 1)
	for _, h := range hist {
		require.Equal(t, "latency", h.Name())
		require.Equal(t, map[string]string{
			"path": "/test",
			"type": "coordinator",
		}, h.Tags())
	}
}

func TestResponseMetricsCustomMetricType(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	iOpts := instrument.NewOptions().SetMetricsScope(scope)

	r := mux.NewRouter()
	route := r.NewRoute()
	opts := Options{
		InstrumentOpts: iOpts,
		Route:          route,
	}

	h := ResponseMetrics(opts).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set(headers.CustomResponseMetricsType, "foo")
		w.WriteHeader(200)
	}))
	route.Path("/test").Handler(h)

	server := httptest.NewServer(r)
	defer server.Close()

	resp, err := server.Client().Get(server.URL + "/test?foo=bar") //nolint: noctx
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	snapshot := scope.Snapshot()
	tallytest.AssertCounterValue(t, 1, snapshot, "request", map[string]string{
		"path":   "/test",
		"status": "200",
		"type":   "foo",
	})

	hist := snapshot.Histograms()
	require.True(t, len(hist) == 1)
	for _, h := range hist {
		require.Equal(t, "latency", h.Name())
		require.Equal(t, map[string]string{
			"path": "/test",
			"type": "foo",
		}, h.Tags())
	}
}

var parseQueryParams ParseQueryParams = func(r *http.Request, _ time.Time) (QueryParams, error) {
	if err := r.ParseForm(); err != nil {
		return QueryParams{}, err
	}
	params := QueryParams{
		Query: r.FormValue("query"),
	}
	if s := r.FormValue("start"); s != "" {
		start, err := strconv.Atoi(r.FormValue("start"))
		if err != nil {
			return QueryParams{}, err
		}
		params.Start = time.Unix(int64(start), 0)
	}

	if s := r.FormValue("end"); s != "" {
		end, err := strconv.Atoi(r.FormValue("end"))
		if err != nil {
			return QueryParams{}, err
		}
		params.End = time.Unix(int64(end), 0)
	}
	return params, nil
}

func TestLargeResponseMetrics(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	iOpts := instrument.NewOptions().SetMetricsScope(scope)

	r := mux.NewRouter()
	route := r.NewRoute()
	opts := Options{
		InstrumentOpts: iOpts,
		Route:          route,
		Metrics: MetricsOptions{
			Config: config.MetricsMiddlewareConfiguration{
				QueryEndpointsClassification: config.QueryClassificationConfig{
					ResultsBuckets:  []int{1, 10},
					DurationBuckets: []time.Duration{1 * time.Minute, 15 * time.Minute},
				},
			},
			ParseQueryParams: parseQueryParams,
		},
	}

	h := ResponseMetrics(opts).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add(headers.FetchedSeriesCount, "15")
		w.WriteHeader(200)
	}))
	route.Path("/api/v1/query").Handler(h)

	server := httptest.NewServer(r)
	defer server.Close()

	resp, err := server.Client().Get(server.URL + "/api/v1/query?query=rate(up[20m])") //nolint: noctx
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	snapshot := scope.Snapshot()
	tallytest.AssertCounterValue(t, 1, snapshot, "request", map[string]string{
		"path":                 "/api/v1/query",
		"status":               "200",
		"type":                 "coordinator",
		resultsClassification:  "10",
		durationClassification: "15m0s",
	})

	hist := snapshot.Histograms()
	require.True(t, len(hist) == 1)
	for _, h := range hist {
		require.Equal(t, "latency", h.Name())
		require.Equal(t, map[string]string{
			"path":                 "/api/v1/query",
			"type":                 "coordinator",
			resultsClassification:  "10",
			durationClassification: "15m0s",
		}, h.Tags())
	}
}

func TestMultipleLargeResponseMetricsWithLatencyStatus(t *testing.T) {
	testMultipleLargeResponseMetrics(t, true)
}

func TestMultipleLargeResponseMetricsWithoutLatencyStatus(t *testing.T) {
	testMultipleLargeResponseMetrics(t, false)
}

func TestCustomMetricsRepeatedGets(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	iOpts := instrument.NewOptions().SetMetricsScope(scope)

	cm := newCustomMetrics(iOpts)

	_ = cm.getOrCreate("foo")
	_ = cm.getOrCreate("foo")
	require.Equal(t, len(cm.metrics), 1)

	_ = cm.getOrCreate("foo2")
	require.Equal(t, len(cm.metrics), 2)
}

func testMultipleLargeResponseMetrics(t *testing.T, addStatus bool) {
	scope := tally.NewTestScope("", nil)
	iOpts := instrument.NewOptions().SetMetricsScope(scope)

	r := mux.NewRouter()
	route := r.NewRoute()
	opts := Options{
		InstrumentOpts: iOpts,
		Route:          route,
		Metrics: MetricsOptions{
			Config: config.MetricsMiddlewareConfiguration{
				QueryEndpointsClassification: config.QueryClassificationConfig{
					ResultsBuckets:  []int{1, 10},
					DurationBuckets: []time.Duration{1 * time.Minute, 15 * time.Minute},
				},
				AddStatusToLatencies: addStatus,
			},
			ParseQueryParams: parseQueryParams,
		},
	}

	// NB: pass expected seriesCount from qs to the test.
	seriesCount := "series_count"
	responseCode := "response_code"
	h := ResponseMetrics(opts).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := r.URL.Query().Get(seriesCount)
		w.Header().Add(headers.FetchedSeriesCount, count)
		code := r.URL.Query().Get(responseCode)
		c, err := strconv.Atoi(code)
		require.NoError(t, err)
		w.WriteHeader(c)
	}))

	route.Path("/api/v1/query_range").Handler(h)
	server := httptest.NewServer(r)
	defer server.Close()

	urls := []string{
		"query=rate(up[20m])&series_count=15&response_code=200&start=1&end=1",
		"query=rate(up[10m])&series_count=15&response_code=200&start=1&end=1",
		"query=rate(up[10m])&series_count=5&response_code=200&start=1&end=1",
		"query=rate(up[20m])&series_count=15&response_code=300&start=1&end=1",
		// NB: this should be large since the end-start + query duration is 15m.
		"query=rate(up[14m])&series_count=15&response_code=200&start=1621458000&end=1621458060",
	}

	for _, url := range urls {
		resp, err := server.Client().Get(server.URL + "/api/v1/query_range?" + url) //nolint: noctx
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}

	snapshot := scope.Snapshot()
	tallytest.AssertCounterValue(t, 2, snapshot, "request", map[string]string{
		"path":                 "/api/v1/query_range",
		"status":               "200",
		"type":                 "coordinator",
		resultsClassification:  "10",
		durationClassification: "15m0s",
	})

	tallytest.AssertCounterValue(t, 1, snapshot, "request", map[string]string{
		"path":                 "/api/v1/query_range",
		"status":               "300",
		"type":                 "coordinator",
		resultsClassification:  "10",
		durationClassification: "15m0s",
	})

	tallytest.AssertCounterValue(t, 1, snapshot, "request", map[string]string{
		"path":                 "/api/v1/query_range",
		"status":               "200",
		"type":                 "coordinator",
		resultsClassification:  "1",
		durationClassification: "1m0s",
	})

	tallytest.AssertCounterValue(t, 5, snapshot, "count", map[string]string{
		"type":   "coordinator",
		"status": "classified_duration",
	})
	tallytest.AssertCounterValue(t, 5, snapshot, "count", map[string]string{
		"type":   "coordinator",
		"status": "classified_result",
	})

	var (
		hist       = snapshot.Histograms()
		exHistLen  = 3
		exTagCount = 4
	)

	if addStatus {
		// NB: if status is added, we expect to see three histogram entries,
		// since they will also include the status code in the tags list; otherwise
		// code:200 and code:300 queries are expected to go to the same histogram
		// metric.
		exHistLen++
		exTagCount++
	}

	require.Equal(t, exHistLen, len(hist))
	buckets := map[string]map[string]int{
		resultsClassification: {
			"1":  0,
			"10": 0,
		},
		durationClassification: {
			"1m0s":  0,
			"15m0s": 0,
		},
	}
	statuses := map[string]int{}
	for _, h := range hist {
		require.Equal(t, "latency", h.Name())

		tags := h.Tags()
		require.Equal(t, exTagCount, len(tags))
		require.Equal(t, "/api/v1/query_range", tags["path"])
		require.Equal(t, metricsTypeTagDefaultValue, tags[metricsTypeTagName])

		buckets[resultsClassification][tags[resultsClassification]]++
		buckets[durationClassification][tags[durationClassification]]++
		if addStatus {
			statuses[tags["status"]]++
		}
	}

	expectedBuckets := map[string]map[string]int{
		resultsClassification: {
			"1":  1,
			"10": 2,
		},
		durationClassification: {
			"1m0s":  2,
			"15m0s": 1,
		},
	}
	if addStatus {
		require.Equal(t, map[string]int{"200": 3, "300": 1}, statuses)
		expectedBuckets[durationClassification]["15m0s"]++
		expectedBuckets[resultsClassification]["10"]++
	}

	require.Equal(t, expectedBuckets, buckets)
}

func TestRequestClassificationByEndpoints(t *testing.T) {
	defaultConfig := config.MetricsMiddlewareConfiguration{
		QueryEndpointsClassification: config.QueryClassificationConfig{
			ResultsBuckets:  []int{1, 10, 100, 1000},
			DurationBuckets: []time.Duration{1 * time.Minute, 10 * time.Minute, 100 * time.Minute},
		},
		LabelEndpointsClassification: config.QueryClassificationConfig{
			ResultsBuckets:  []int{2, 20, 200, 2000},
			DurationBuckets: []time.Duration{2 * time.Minute, 20 * time.Minute, 200 * time.Minute},
		},
	}
	tests := []struct {
		name             string
		path             string
		config           config.MetricsMiddlewareConfiguration
		isQueryEndpoint  bool
		query            string
		fetchedResult    string
		expectedResult   string
		expectedDuration string
	}{
		{
			name:             "query_range",
			path:             route.QueryRangeURL,
			config:           defaultConfig,
			isQueryEndpoint:  true,
			query:            "query=sum(rate(coordinator_http_handler_http_handler_request[1m]))&start=1&end=660",
			fetchedResult:    "25",
			expectedResult:   "10",
			expectedDuration: "10m0s",
		},
		{
			name:             "query",
			path:             route.QueryURL,
			config:           defaultConfig,
			isQueryEndpoint:  true,
			query:            "query=sum(rate(coordinator_http_handler_http_handler_request[1m]))&time=1630611461",
			fetchedResult:    "1",
			expectedResult:   "1",
			expectedDuration: "1m0s",
		},
		{
			name:             "label_values",
			path:             route.Prefix + "/label/__name__/values",
			config:           defaultConfig,
			isQueryEndpoint:  false,
			query:            "start=0&end=1800",
			fetchedResult:    "30000",
			expectedResult:   "2000",
			expectedDuration: "20m0s",
		},
		{
			name:             "label_values -- max time",
			path:             route.Prefix + "/label/__name__/values",
			config:           defaultConfig,
			isQueryEndpoint:  false,
			fetchedResult:    "30000",
			expectedResult:   "2000",
			expectedDuration: "3h20m0s",
		},
		{
			name:             "label_names",
			path:             route.LabelNamesURL,
			config:           defaultConfig,
			isQueryEndpoint:  false,
			query:            "start=0&end=21600",
			fetchedResult:    "300",
			expectedResult:   "200",
			expectedDuration: "3h20m0s",
		},
		{
			name:             "non-classifiable endpoint",
			path:             "/foo",
			config:           defaultConfig,
			isQueryEndpoint:  false,
			query:            "start=0&end=21600",
			expectedResult:   "unclassified",
			expectedDuration: "unclassified",
		},
		{
			name:            "disabled",
			path:            "/foo",
			config:          config.MetricsMiddlewareConfiguration{},
			isQueryEndpoint: false,
			query:           "start=0&end=21600",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope := tally.NewTestScope("", nil)
			iOpts := instrument.NewOptions().SetMetricsScope(scope)

			r := mux.NewRouter()
			route := r.NewRoute()
			opts := Options{
				InstrumentOpts: iOpts,
				Route:          route,
				Metrics: MetricsOptions{
					Config:           tt.config,
					ParseQueryParams: parseQueryParams,
					ParseOptions: promql.NewParseOptions().
						SetRequireStartEndTime(false).
						SetNowFn(time.Now),
				},
			}

			h := ResponseMetrics(opts).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.isQueryEndpoint {
					w.Header().Add(headers.FetchedSeriesCount, tt.fetchedResult)
				} else {
					w.Header().Add(headers.FetchedMetadataCount, tt.fetchedResult)
				}
				w.WriteHeader(200)
			}))

			route.Path(tt.path).Handler(h)
			server := httptest.NewServer(r)
			defer server.Close()

			resp, err := server.Client().Get(server.URL + tt.path + "?" + tt.query) //nolint: noctx
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			snapshot := scope.Snapshot()
			// Validate request counter
			tags := map[string]string{
				"path":                 tt.path,
				"status":               "200",
				"type":                 "coordinator",
				resultsClassification:  tt.expectedResult,
				durationClassification: tt.expectedDuration,
			}
			if tt.expectedResult == "" {
				delete(tags, resultsClassification)
			}
			if tt.expectedDuration == "" {
				delete(tags, durationClassification)
			}
			tallytest.AssertCounterValue(t, 1, snapshot, "request", tags)

			// Validate latency histogram
			require.Equal(t, 1, len(snapshot.Histograms()))
			var hist tally.HistogramSnapshot
			for _, hist = range snapshot.Histograms() {
			}

			if !tt.config.AddStatusToLatencies {
				delete(tags, "status")
			}
			require.Equal(t, tags, hist.Tags())

			count := int64(0)
			for _, value := range hist.Durations() {
				count += value
			}
			require.Equal(t, int64(1), count)
		})
	}
}
