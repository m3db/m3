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
type PrometheusRangeRewriteOptions struct { // nolint:maligned
	Enabled              bool
	FetchOptionsBuilder  handleroptions.FetchOptionsBuilder
	Instant              bool
	ResolutionMultiplier int
	DefaultLookback      time.Duration
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

const (
	queryParam    = "query"
	startParam    = "start"
	endParam      = "end"
	lookbackParam = handleroptions.LookbackParam
)

func rewriteRangeDuration(
	r *http.Request,
	opts PrometheusRangeRewriteOptions,
	logger *zap.Logger,
) error {
	// Extract relevant query params
	params, err := extractParams(r, opts.Instant)
	if err != nil {
		return err
	}
	defer func() {
		// Reset the body on the request for any handlers that may want access to the raw body.
		if opts.Instant {
			// NB(nate): shared time param parsing logic modifies the form on the request to set these
			// to the "now" string to aid in parsing. Remove these before resetting the body.
			r.Form.Del(startParam)
			r.Form.Del(endParam)
		}

		if r.Method == "GET" {
			return
		}

		body := r.Form.Encode()
		r.Body = ioutil.NopCloser(bytes.NewBufferString(body))
	}()

	// Query for namespace metadata of namespaces used to service the request
	store := opts.Storage
	ctx, fetchOpts, err := opts.FetchOptionsBuilder.NewFetchOptions(r.Context(), r)
	if err != nil {
		return err
	}

	attrs, err := store.QueryStorageMetadataAttributes(ctx, params.start, params.end, fetchOpts)
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
	expr, err := parser.ParseExpr(params.query)
	if err != nil {
		return err
	}

	updateQuery, updatedQuery := maybeRewriteRangeInQuery(params.query, expr, res, opts.ResolutionMultiplier)
	updateLookback, updatedLookback := maybeUpdateLookback(params, res, opts)

	if !updateQuery && !updateLookback {
		return nil
	}

	// Update query and lookback params in URL, if present and needed.
	urlQueryValues, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return err
	}
	if urlQueryValues.Get(queryParam) != "" {
		if updateQuery {
			urlQueryValues.Set(queryParam, updatedQuery)
		}
		if updateLookback {
			urlQueryValues.Set(lookbackParam, updatedLookback.String())
		}
	}
	updatedURL, err := url.Parse(r.URL.String())
	if err != nil {
		return err
	}
	updatedURL.RawQuery = urlQueryValues.Encode()
	r.URL = updatedURL

	// Update query and lookback params in the request body, if present and needed.
	if r.Form.Get(queryParam) != "" {
		if updateQuery {
			r.Form.Set(queryParam, updatedQuery)
		}
		if updateLookback {
			r.Form.Set(lookbackParam, updatedLookback.String())
		}
	}

	logger.Debug("rewrote duration values in request",
		zap.String("originalQuery", params.query),
		zap.String("updatedQuery", updatedQuery),
		zap.Duration("originalLookback", params.lookback),
		zap.Duration("updatedLookback", updatedLookback))

	return nil
}

type params struct {
	query         string
	start, end    time.Time
	lookback      time.Duration
	isLookbackSet bool
}

func extractParams(r *http.Request, instant bool) (params, error) {
	if err := r.ParseForm(); err != nil {
		return params{}, err
	}

	query := r.FormValue(queryParam)

	if instant {
		prometheus.SetDefaultStartEndParamsForInstant(r)
	}

	timeParams, err := prometheus.ParseTimeParams(r)
	if err != nil {
		return params{}, err
	}

	lookback, isLookbackSet, err := handleroptions.ParseLookbackDuration(r)
	if err != nil {
		return params{}, err
	}

	return params{
		query:         query,
		start:         timeParams.Start,
		end:           timeParams.End,
		lookback:      lookback,
		isLookbackSet: isLookbackSet,
	}, nil
}

func maybeRewriteRangeInQuery(query string, expr parser.Node, res time.Duration, multiplier int) (bool, string) {
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

	if updated {
		return true, expr.String()
	}
	return false, query
}

func maybeUpdateLookback(
	params params,
	maxResolution time.Duration,
	opts PrometheusRangeRewriteOptions,
) (bool, time.Duration) {
	resolutionBasedLookback := maxResolution * time.Duration(opts.ResolutionMultiplier) // nolint: durationcheck
	if params.isLookbackSet && params.lookback < resolutionBasedLookback {
		return true, resolutionBasedLookback
	}
	if !params.isLookbackSet && opts.DefaultLookback < resolutionBasedLookback {
		return true, resolutionBasedLookback
	}
	return false, params.lookback
}
