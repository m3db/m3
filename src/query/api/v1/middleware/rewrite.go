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
	"net/url"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/prometheus/promql/parser"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/storage"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

// PrometheusRangeRewriteOptions are the options for the prometheus range rewriting middleware.
type PrometheusRangeRewriteOptions struct {
	Enabled              bool
	FetchOptionsBuilder  handleroptions.FetchOptionsBuilder
	Instant              bool
	ResolutionMultiplier int
	Storage              storage.Storage
}

// PrometheusRangeRewrite is middleware that, when enabled, will rewrite the query parameter
// on the request (url and body, if present) if it's determined that the query contains a
// range duration that is less than the resolution that will be used to serve the request.
// With this middleware disabled, queries like this will return no data. When enabled, the
// range is updated to be the namespace resolution * a configurable multiple which should allow
// for data to be returned.
func PrometheusRangeRewrite(opts Options) mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var (
				mwOpts   = opts.PrometheusRangeRewrite
				mult     = mwOpts.ResolutionMultiplier
				disabled = !mwOpts.Enabled
			)
			if disabled || mult == 0 {
				base.ServeHTTP(w, r)
				return
			}

			logger := opts.InstrumentOpts.Logger()
			if err := rewriteRangeDuration(r, mwOpts, logger); err != nil {
				logger.Error("could not rewrite range", zap.Error(err))
				xhttp.WriteError(w, err)
				return
			}
			base.ServeHTTP(w, r)
		})
	}
}

const queryParam = "query"

func rewriteRangeDuration(
	r *http.Request,
	opts PrometheusRangeRewriteOptions,
	logger *zap.Logger,
) error {
	// Extract relevant query params
	query, start, end, err := extractParams(r, opts.Instant)
	if err != nil {
		return err
	}

	// Query for namespace metadata of namespaces used to service the request
	store := opts.Storage
	ctx, fetchOpts, err := opts.FetchOptionsBuilder.NewFetchOptions(r.Context(), r)
	if err != nil {
		return err
	}

	attrs, err := store.QueryStorageMetadataAttributes(ctx, start, end, fetchOpts)
	if err != nil {
		return err
	}

	// Find the largest resolution
	var res time.Duration
	for _, attr := range attrs {
		if attr.Resolution > res {
			res = attr.Resolution
		}
	}

	// Largest resolution is 0 which means we're routing to the unaggregated namespace.
	// Unaggregated namespace can service all requests, so return.
	if res == 0 {
		return nil
	}

	// Rewrite ranges within the query, if necessary
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return err
	}
	updated := rewriteRangeInQuery(expr, res, opts.ResolutionMultiplier)
	if !updated {
		return nil
	}

	// Add updated query to the request where necessary
	updatedQuery := expr.String()

	// Update query param in URL, if present
	urlQueryValues, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return err
	}
	if urlQueryValues.Get(queryParam) != "" {
		urlQueryValues.Set(queryParam, updatedQuery)
	}
	updatedURL, err := url.Parse(r.URL.String())
	if err != nil {
		return err
	}
	updatedURL.RawQuery = urlQueryValues.Encode()
	r.URL = updatedURL

	// Update query param in the request body, if present
	if r.Form.Get(queryParam) != "" {
		r.Form.Set(queryParam, updatedQuery)
	}

	// Reset the body on the request for any handlers that may want access to the raw body.
	updatedBody := r.Form.Encode()
	r.Body = ioutil.NopCloser(bytes.NewBufferString(updatedBody))

	logger.Debug("rewrote range duration value within query",
		zap.String("originalQuery", query),
		zap.String("updatedQuery", updatedQuery))

	return nil
}

func extractParams(r *http.Request, instant bool) (string, time.Time, time.Time, error) {
	if err := r.ParseForm(); err != nil {
		return "", time.Time{}, time.Time{}, err
	}

	query := r.FormValue(queryParam)

	if instant {
		prometheus.SetDefaultStartEndParamsForInstant(r)
	}

	timeParams, err := prometheus.ParseTimeParams(r)
	if err != nil {
		return "", time.Time{}, time.Time{}, err
	}

	return query, timeParams.Start, timeParams.End, nil
}

func rewriteRangeInQuery(expr parser.Node, res time.Duration, multiplier int) bool {
	updated := false
	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		// nolint:gocritic
		switch n := node.(type) {
		case *parser.MatrixSelector:
			if n.Range <= res {
				n.Range = res * time.Duration(multiplier)
				updated = true
			}
		}
		return nil
	})

	return updated
}
