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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/x/instrument"
)

func TestPrometheusRangeRewrite(t *testing.T) {
	queryTests := []struct {
		name     string
		attrs    []storagemetadata.Attributes
		enabled  bool
		mult     int
		query    string
		expected string
	}{
		{
			name: "query with range to unagg",
			attrs: []storagemetadata.Attributes{
				{
					MetricsType: storagemetadata.UnaggregatedMetricsType,
					Retention:   7 * 24 * time.Hour,
				},
			},
			enabled:  true,
			mult:     2,
			query:    "rate(foo[1m])",
			expected: "rate(foo[1m])",
		},
		{
			name: "query with no range",
			attrs: []storagemetadata.Attributes{
				{
					MetricsType: storagemetadata.UnaggregatedMetricsType,
					Retention:   7 * 24 * time.Hour,
				},
			},
			enabled:  true,
			mult:     2,
			query:    "foo",
			expected: "foo",
		},
		{
			name: "query with rewriteable range",
			attrs: []storagemetadata.Attributes{
				{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Resolution:  5 * time.Minute,
					Retention:   90 * 24 * time.Hour,
				},
			},
			enabled:  true,
			mult:     2,
			query:    "rate(foo[30s])",
			expected: "rate(foo[10m])",
		},
		{
			name: "query with range to agg; no rewrite",
			attrs: []storagemetadata.Attributes{
				{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Retention:   30 * 24 * time.Hour,
					Resolution:  1 * time.Minute,
				},
			},
			enabled:  true,
			mult:     2,
			query:    "rate(foo[5m])",
			expected: "rate(foo[5m])",
		},
		{
			name: "query with rewriteable range; disabled",
			attrs: []storagemetadata.Attributes{
				{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Resolution:  5 * time.Minute,
					Retention:   90 * 24 * time.Hour,
				},
			},
			enabled:  false,
			mult:     2,
			query:    "rate(foo[30s])",
			expected: "rate(foo[30s])",
		},
		{
			name: "query with rewriteable range; zero multiplier",
			attrs: []storagemetadata.Attributes{
				{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Resolution:  5 * time.Minute,
					Retention:   90 * 24 * time.Hour,
				},
			},
			enabled:  false,
			mult:     0,
			query:    "rate(foo[30s])",
			expected: "rate(foo[30s])",
		},
		{
			name: "query with range to multiple aggs; rewrite to largest",
			attrs: []storagemetadata.Attributes{
				{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Retention:   30 * 24 * time.Hour,
					Resolution:  2 * time.Minute,
				},
				{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Retention:   60 * 24 * time.Hour,
					Resolution:  4 * time.Minute,
				},
			},
			enabled:  true,
			mult:     3,
			query:    "rate(foo[1m])",
			expected: "rate(foo[12m])",
		},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			r := mux.NewRouter()

			opts := makeBaseOpts(t, r)

			store := opts.PrometheusRangeRewrite.Storage.(mock.Storage)
			store.SetQueryStorageMetadataAttributesResult(tt.attrs, nil)

			opts.PrometheusRangeRewrite.Enabled = tt.enabled
			opts.PrometheusRangeRewrite.ResolutionMultiplier = tt.mult

			h := PrometheusRangeRewrite(opts).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, r.FormValue(queryParam), tt.expected)
			}))
			opts.Route.Path("/query_range").Handler(h)

			server := httptest.NewServer(r)
			defer server.Close()

			var (
				resp *http.Response
				err  error
			)

			// Validate as GET
			args := "step=3600&start=1614882294&end=1625250298&query=" + tt.query
			resp, err = server.Client().Get(server.URL + "/query_range?" + args) //nolint: noctx
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			// Validate as POST
			// nolint: noctx
			resp, err = server.Client().Post(
				server.URL+"/query_range",
				"application/x-www-form-urlencoded",
				bytes.NewReader([]byte(args)),
			)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
		})
	}
}

func makeBaseOpts(t *testing.T, r *mux.Router) Options {
	route := r.NewRoute()

	mockStorage := mock.NewMockStorage()

	fetchOptsBuilderCfg := handleroptions.FetchOptionsBuilderOptions{
		Timeout: 15 * time.Second,
	}
	fetchOptsBuilder, err := handleroptions.NewFetchOptionsBuilder(fetchOptsBuilderCfg)
	require.NoError(t, err)

	return Options{
		InstrumentOpts: instrument.NewOptions(),
		Route:          route,
		PrometheusRangeRewrite: PrometheusRangeRewriteOptions{
			Enabled:              true,
			FetchOptionsBuilder:  fetchOptsBuilder,
			ResolutionMultiplier: 2,
			Storage:              mockStorage,
		},
	}
}
