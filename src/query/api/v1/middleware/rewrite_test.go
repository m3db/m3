// Copyright (c) 2021  Uber Technologies, Inc.
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
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	kitlogzap "github.com/go-kit/kit/log/zap"
	"github.com/gorilla/mux"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/x/instrument"
)

func TestPrometheusRangeRewrite(t *testing.T) {
	// nolint:maligned
	queryTests := []struct {
		name     string
		attrs    []storagemetadata.Attributes
		enabled  bool
		mult     int
		query    string
		instant  bool
		lookback *time.Duration

		expectedQuery    string
		expectedLookback *time.Duration
		usePromEngine    bool
	}{
		{
			name:    "query with range to unagg",
			attrs:   unaggregatedAttrs(),
			enabled: true,
			mult:    2,
			query:   "rate(foo[1m])",

			expectedQuery: "rate(foo[1m])",
		},
		{
			name:    "query with no range",
			attrs:   unaggregatedAttrs(),
			enabled: true,
			mult:    2,
			query:   "foo",

			expectedQuery: "foo",
		},
		{
			name:    "query with rewriteable range",
			attrs:   aggregatedAttrs(5 * time.Minute),
			enabled: true,
			mult:    2,
			query:   "rate(foo[30s])",

			expectedQuery:    "rate(foo[10m])",
			expectedLookback: durationPtr(10 * time.Minute),
		},
		{
			name:    "query with range to agg; no rewrite",
			attrs:   aggregatedAttrs(1 * time.Minute),
			enabled: true,
			mult:    2,
			query:   "rate(foo[5m])",

			expectedQuery: "rate(foo[5m])",
		},
		{
			name:    "query with rewriteable range; disabled",
			attrs:   aggregatedAttrs(5 * time.Minute),
			enabled: false,
			mult:    2,
			query:   "rate(foo[30s])",

			expectedQuery: "rate(foo[30s])",
		},
		{
			name:    "query with rewriteable range; zero multiplier",
			attrs:   aggregatedAttrs(5 * time.Minute),
			enabled: false,
			mult:    0,
			query:   "rate(foo[30s])",

			expectedQuery: "rate(foo[30s])",
		},
		{
			name:    "instant query; no rewrite",
			attrs:   unaggregatedAttrs(),
			enabled: true,
			mult:    3,
			instant: true,
			query:   "rate(foo[1m])",

			expectedQuery: "rate(foo[1m])",
		},
		{
			name:    "instant query; rewrite",
			attrs:   aggregatedAttrs(5 * time.Minute),
			enabled: true,
			mult:    3,
			instant: true,
			query:   "rate(foo[30s])",

			expectedQuery:    "rate(foo[15m])",
			expectedLookback: durationPtr(15 * time.Minute),
		},
		{
			name:    "range with lookback not set; keep default lookback",
			attrs:   aggregatedAttrs(1 * time.Minute),
			enabled: true,
			mult:    2,
			query:   "foo",

			expectedQuery: "foo",
		},
		{
			name:    "instant with lookback not set; keep default lookback",
			attrs:   aggregatedAttrs(1 * time.Minute),
			enabled: true,
			mult:    2,
			instant: true,
			query:   "foo",

			expectedQuery: "foo",
		},
		{
			name:    "range with lookback not set; rewrite lookback to higher",
			attrs:   aggregatedAttrs(3 * time.Minute),
			enabled: true,
			mult:    3,
			query:   "foo",

			expectedQuery:    "foo",
			expectedLookback: durationPtr(9 * time.Minute),
		},
		{
			name:    "instant with lookback not set; rewrite lookback to higher",
			attrs:   aggregatedAttrs(4 * time.Minute),
			enabled: true,
			mult:    3,
			instant: true,
			query:   "foo",

			expectedQuery:    "foo",
			expectedLookback: durationPtr(12 * time.Minute),
		},
		{
			name:     "range with lookback already set; keep existing lookback",
			attrs:    aggregatedAttrs(5 * time.Minute),
			enabled:  true,
			mult:     2,
			query:    "foo",
			lookback: durationPtr(11 * time.Minute),

			expectedQuery:    "foo",
			expectedLookback: durationPtr(11 * time.Minute),
		},
		{
			name:     "instant with lookback already set; keep existing lookback",
			attrs:    aggregatedAttrs(4 * time.Minute),
			enabled:  true,
			mult:     3,
			instant:  true,
			query:    "foo",
			lookback: durationPtr(13 * time.Minute),

			expectedQuery:    "foo",
			expectedLookback: durationPtr(13 * time.Minute),
		},
		{
			name:     "range with lookback already set; rewrite lookback to higher",
			attrs:    aggregatedAttrs(5 * time.Minute),
			enabled:  true,
			mult:     3,
			query:    "foo",
			lookback: durationPtr(11 * time.Minute),

			expectedQuery:    "foo",
			expectedLookback: durationPtr(15 * time.Minute),
		},
		{
			name:     "instant with lookback already set; rewrite lookback to higher",
			attrs:    aggregatedAttrs(5 * time.Minute),
			enabled:  true,
			mult:     3,
			instant:  true,
			query:    "foo",
			lookback: durationPtr(13 * time.Minute),

			expectedQuery:    "foo",
			expectedLookback: durationPtr(15 * time.Minute),
		},
		{
			name:          "instant query; rewrite w/ prom engine",
			attrs:         aggregatedAttrs(5 * time.Minute),
			enabled:       true,
			mult:          3,
			instant:       true,
			usePromEngine: true,
			query:         "rate(foo[30s])",
			lookback:      durationPtr(15 * time.Minute),

			expectedQuery:    "rate(foo[15m])",
			expectedLookback: durationPtr(15 * time.Minute),
		},
		{
			name: "instant query; rewrite w/ prom engine & offset",
			// Just testing the parsing code paths since this is a fake storage
			attrs:         aggregatedAttrs(5 * time.Minute),
			enabled:       true,
			usePromEngine: true,
			mult:          3,
			instant:       true,
			query:         "rate(foo[30s] offset 1w)",
			lookback:      durationPtr(15 * time.Minute),

			expectedQuery:    "rate(foo[15m] offset 1w)",
			expectedLookback: durationPtr(15 * time.Minute),
		},
		{
			name: "range query; rewrite w/ prom engine & offset",
			// Just testing the parsing code paths since this is a fake storage
			attrs:         aggregatedAttrs(5 * time.Minute),
			enabled:       true,
			usePromEngine: true,
			mult:          3,
			instant:       false,
			query:         "rate(foo[30s] offset 1w)",
			lookback:      durationPtr(15 * time.Minute),

			expectedQuery:    "rate(foo[15m] offset 1w)",
			expectedLookback: durationPtr(15 * time.Minute),
		},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			r := mux.NewRouter()

			opts := makeBaseOpts(t, r, tt.usePromEngine)

			store := opts.PrometheusRangeRewrite.Storage.(mock.Storage)
			store.SetQueryStorageMetadataAttributesResult(tt.attrs, nil)

			opts.PrometheusRangeRewrite.Enabled = tt.enabled
			opts.PrometheusRangeRewrite.ResolutionMultiplier = tt.mult
			opts.PrometheusRangeRewrite.Instant = tt.instant

			params := url.Values{}
			params.Add("step", (time.Duration(3600) * time.Second).String())
			if tt.instant {
				params.Add("now", "1600000000")
			} else {
				params.Add(startParam, "1600000000")
				params.Add(endParam, "1600000001")
			}
			params.Add(queryParam, tt.query)
			if tt.lookback != nil {
				params.Add(lookbackParam, tt.lookback.String())
			}
			encodedParams := params.Encode()

			h := PrometheusRangeRewrite(opts).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, r.FormValue(queryParam), tt.expectedQuery)
				if tt.expectedLookback != nil {
					require.Equal(t, r.FormValue(lookbackParam), tt.expectedLookback.String())
				}

				enabled := tt.enabled && tt.mult > 0
				if enabled && r.Method == "POST" {
					params.Set("query", tt.expectedQuery)
					if tt.expectedLookback != nil {
						params.Set(lookbackParam, tt.expectedLookback.String())
					}

					body, err := ioutil.ReadAll(r.Body)
					require.NoError(t, err)
					// request body should be exactly the same with the exception of an updated
					// query, potentially.
					require.Equal(t, params.Encode(), string(body))
				}

				if r.Method == "GET" {
					require.Equal(t, http.NoBody, r.Body)
				}
			}))
			path := "/query_range"
			if tt.instant {
				path = "/query"
			}
			opts.Route.Path(path).Handler(h)

			server := httptest.NewServer(r)
			defer server.Close()

			var (
				resp *http.Response
				err  error
			)

			// Validate as GET
			// nolint: noctx
			resp, err = server.Client().Get(
				server.URL + path + "?" + encodedParams,
			)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, 200, resp.StatusCode)

			// Validate as POST
			// nolint: noctx
			resp, err = server.Client().Post(
				server.URL+path,
				"application/x-www-form-urlencoded",
				bytes.NewReader([]byte(encodedParams)),
			)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, 200, resp.StatusCode)
		})
	}
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}

func makeBaseOpts(t *testing.T, r *mux.Router, addPromEngine bool) Options {
	var (
		instrumentOpts = instrument.NewOptions()
		kitLogger      = kitlogzap.NewZapSugarLogger(instrumentOpts.Logger(), zapcore.InfoLevel)
		engineOpts     = promql.EngineOpts{
			Logger:     log.With(kitLogger, "component", "query engine"),
			MaxSamples: 100,
			Timeout:    1 * time.Minute,
			NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 {
				return durationMilliseconds(1 * time.Minute)
			},
		}
	)
	engine := promql.NewEngine(engineOpts)
	route := r.NewRoute()

	mockStorage := mock.NewMockStorage()

	fetchOptsBuilderCfg := handleroptions.FetchOptionsBuilderOptions{
		Timeout: 15 * time.Second,
	}
	fetchOptsBuilder, err := handleroptions.NewFetchOptionsBuilder(fetchOptsBuilderCfg)
	require.NoError(t, err)

	opts := Options{
		InstrumentOpts: instrument.NewOptions(),
		Route:          route,
		PrometheusRangeRewrite: PrometheusRangeRewriteOptions{
			Enabled:              true,
			FetchOptionsBuilder:  fetchOptsBuilder,
			ResolutionMultiplier: 2,
			DefaultLookback:      5 * time.Minute,
			Storage:              mockStorage,
		},
	}
	if addPromEngine {
		opts.PrometheusRangeRewrite.PrometheusEngineFn = func(duration time.Duration) (*promql.Engine, error) {
			return engine, nil
		}
	}
	return opts
}

func unaggregatedAttrs() []storagemetadata.Attributes {
	return []storagemetadata.Attributes{
		{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
		},
	}
}

func aggregatedAttrs(resolution time.Duration) []storagemetadata.Attributes {
	return []storagemetadata.Attributes{
		{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Resolution:  resolution,
		},
	}
}

func durationPtr(duration time.Duration) *time.Duration {
	return &duration
}
