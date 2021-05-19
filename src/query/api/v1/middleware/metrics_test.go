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
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/x/headers"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/tallytest"
)

func TestResponseMetrics(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	iOpts := instrument.NewOptions().SetMetricsScope(scope)

	r := mux.NewRouter()
	route := r.NewRoute()
	opts := options.MiddlewareOptions{
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
		"path":            "/test",
		"status":          "200",
		"size":            "small",
		"count_threshold": "0",
		"range_threshold": "0",
	})

	hist := snapshot.Histograms()
	require.True(t, len(hist) == 1)
	for _, h := range hist {
		require.Equal(t, "latency", h.Name())
		require.Equal(t, map[string]string{
			"path":            "/test",
			"size":            "small",
			"count_threshold": "0",
			"range_threshold": "0",
		}, h.Tags())
	}
}

func TestLargeResponseMetrics(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	iOpts := instrument.NewOptions().SetMetricsScope(scope)

	r := mux.NewRouter()
	route := r.NewRoute()
	opts := options.MiddlewareOptions{
		InstrumentOpts: iOpts,
		Route:          route,
		Config: &config.MiddlewareConfiguration{
			InspectQuerySize:          true,
			LargeSeriesCountThreshold: 10,
			LargeSeriesRangeThreshold: time.Minute * 15,
		},
	}

	h := ResponseMetrics(opts).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add(headers.FetchedSeriesCount, "15")
		w.WriteHeader(200)
	}))
	route.Path("/api/v1/query_range").Handler(h)

	server := httptest.NewServer(r)
	defer server.Close()

	resp, err := server.Client().Get(server.URL + "/api/v1/query_range?query=rate(up[20m])") //nolint: noctx
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	snapshot := scope.Snapshot()
	tallytest.AssertCounterValue(t, 1, snapshot, "request", map[string]string{
		"path":            "/api/v1/query_range",
		"status":          "200",
		"size":            "large",
		"count_threshold": "10",
		"range_threshold": "15m0s",
	})

	hist := snapshot.Histograms()
	require.True(t, len(hist) == 1)
	for _, h := range hist {
		require.Equal(t, "latency", h.Name())
		require.Equal(t, map[string]string{
			"path":            "/api/v1/query_range",
			"size":            "large",
			"count_threshold": "10",
			"range_threshold": "15m0s",
		}, h.Tags())
	}
}

func TestMultipleLargeResponseMetricsWithLatencyStatus(t *testing.T) {
	testMultipleLargeResponseMetrics(t, true)
}

func TestMultipleLargeResponseMetricsWithoutLatencyStatus(t *testing.T) {
	testMultipleLargeResponseMetrics(t, false)
}

func testMultipleLargeResponseMetrics(t *testing.T, addStatus bool) {
	scope := tally.NewTestScope("", nil)
	iOpts := instrument.NewOptions().SetMetricsScope(scope)

	r := mux.NewRouter()
	route := r.NewRoute()
	opts := options.MiddlewareOptions{
		InstrumentOpts: iOpts,
		Route:          route,
		Config: &config.MiddlewareConfiguration{
			InspectQuerySize:          true,
			LargeSeriesCountThreshold: 10,
			LargeSeriesRangeThreshold: time.Minute * 15,
			AddStatusToLatencies:      addStatus,
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
		"query_range?query=rate(up[20m])&series_count=15&response_code=200&start=1&end=1",
		"query_range?query=rate(up[10m])&series_count=15&response_code=200&start=1&end=1",
		"query_range?query=rate(up[10m])&series_count=5&response_code=200&start=1&end=1",
		"query_range?query=rate(up[20m])&series_count=15&response_code=300&start=1&end=1",
		// NB: this should be large since the end-start + query duration is 15m.
		"query_range?query=rate(up[14m])&series_count=15&response_code=200&start=1621458000&end=1621458060",
	}

	for _, url := range urls {
		resp, err := server.Client().Get(server.URL + "/api/v1/" + url) //nolint: noctx
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}

	snapshot := scope.Snapshot()
	tallytest.AssertCounterValue(t, 2, snapshot, "request", map[string]string{
		"path":            "/api/v1/query_range",
		"status":          "200",
		"size":            "large",
		"count_threshold": "10",
		"range_threshold": "15m0s",
	})

	tallytest.AssertCounterValue(t, 1, snapshot, "request", map[string]string{
		"path":            "/api/v1/query_range",
		"status":          "300",
		"size":            "large",
		"count_threshold": "10",
		"range_threshold": "15m0s",
	})

	tallytest.AssertCounterValue(t, 2, snapshot, "request", map[string]string{
		"path":            "/api/v1/query_range",
		"status":          "200",
		"size":            "small",
		"count_threshold": "10",
		"range_threshold": "15m0s",
	})

	tallytest.AssertCounterValue(t, 1, snapshot, "count", map[string]string{
		"status": "below_range_threshold",
	})
	tallytest.AssertCounterValue(t, 1, snapshot, "count", map[string]string{
		"status": "below_count_threshold",
	})
	tallytest.AssertCounterValue(t, 3, snapshot, "count", map[string]string{
		"status": "large_query",
	})

	var (
		hist       = snapshot.Histograms()
		exHistLen  = 2
		exLarge    = 1
		exTagCount = 4
	)

	if addStatus {
		// NB: if status is added, we expect to see three histogram entries,
		// since they will also include the status code in the tags list; otherwise
		// code:200 and code:300 queries are expected to go to the same histogram
		// metric.
		exHistLen++
		exLarge++
		exTagCount++
	}

	require.True(t, len(hist) == exHistLen)
	sizes := map[string]int{}
	statuses := map[string]int{}
	for _, h := range hist {
		require.Equal(t, "latency", h.Name())

		tags := h.Tags()
		require.Equal(t, exTagCount, len(tags))
		require.Equal(t, "/api/v1/query_range", tags["path"])
		require.Equal(t, "10", tags["count_threshold"])
		require.Equal(t, "15m0s", tags["range_threshold"])

		sizes[tags["size"]]++
		if addStatus {
			statuses[tags["status"]]++
		}
	}

	if addStatus {
		require.Equal(t, map[string]int{"200": 2, "300": 1}, statuses)
	}

	require.Equal(t, map[string]int{"small": 1, "large": exLarge}, sizes)
}
