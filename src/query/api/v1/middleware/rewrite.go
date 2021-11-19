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
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	promstorage "github.com/prometheus/prometheus/storage"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/storage"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xtime "github.com/m3db/m3/src/x/time"
)

var errIgnorableQuerierError = errors.New("ignorable error")

// PrometheusRangeRewriteOptions are the options for the prometheus range rewriting middleware.
type PrometheusRangeRewriteOptions struct { // nolint:maligned
	Enabled              bool
	FetchOptionsBuilder  handleroptions.FetchOptionsBuilder
	Instant              bool
	ResolutionMultiplier int
	DefaultLookback      time.Duration
	Storage              storage.Storage

	// TODO(marcus): There's a conversation with Prometheus about supporting dynamic lookback.
	//  We can replace this with a single engine reference if that work is ever completed.
	//   https://groups.google.com/g/prometheus-developers/c/9wzuobfLMV8
	PrometheusEngineFn func(time.Duration) (*promql.Engine, error)
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
			if err := RewriteRangeDuration(r, mwOpts, logger); err != nil {
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

// RewriteRangeDuration is the driver function for the PrometheusRangeRewrite middleware
func RewriteRangeDuration(
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

	// Get the appropriate time range before updating the lookback
	// This is necessary to cover things like the offset and `@` modifiers.
	startTime, endTime := getQueryBounds(opts, params, fetchOpts, logger)
	res, err := findLargestQueryResolution(ctx, store, fetchOpts, startTime, endTime)
	if err != nil {
		return err
	}
	// Largest resolution is 0 which means we're routing to the unaggregated namespace.
	// Unaggregated namespace can service all requests, so return.
	if res == 0 {
		return nil
	}

	updatedLookback, updateLookback := maybeUpdateLookback(params, res, opts)
	originalLookback := params.lookback

	// We use the lookback as a part of bounds calculation
	// If the lookback had changed, we need to recalculate the bounds
	if updateLookback {
		params.lookback = updatedLookback
		startTime, endTime = getQueryBounds(opts, params, fetchOpts, logger)
		res, err = findLargestQueryResolution(ctx, store, fetchOpts, startTime, endTime)
		if err != nil {
			return err
		}
	}

	// parse the query so that we can manipulate it
	expr, err := parser.ParseExpr(params.query)
	if err != nil {
		return err
	}
	updatedQuery, updateQuery := maybeRewriteRangeInQuery(params.query, expr, res, opts.ResolutionMultiplier)

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
		zap.Duration("originalLookback", originalLookback),
		zap.Duration("updatedLookback", updatedLookback))

	return nil
}

func findLargestQueryResolution(ctx context.Context,
	store storage.Storage,
	fetchOpts *storage.FetchOptions,
	startTime time.Time,
	endTime time.Time,
) (time.Duration, error) {
	attrs, err := store.QueryStorageMetadataAttributes(ctx, startTime, endTime, fetchOpts)
	if err != nil {
		return 0, err
	}

	// Find the largest resolution
	var res time.Duration
	for _, attr := range attrs {
		if attr.Resolution > res {
			res = attr.Resolution
		}
	}
	return res, nil
}

// Using the prometheus engine in this way should be considered
// optional and best effort. Fall back to the frequently accurate logic
// of using the start and end time in the request
func getQueryBounds(
	opts PrometheusRangeRewriteOptions,
	params params,
	fetchOpts *storage.FetchOptions,
	logger *zap.Logger,
) (start time.Time, end time.Time) {
	start = params.start
	end = params.end
	if opts.PrometheusEngineFn == nil {
		return start, end
	}

	lookback := opts.DefaultLookback
	if params.isLookbackSet {
		lookback = params.lookback
	}
	engine, err := opts.PrometheusEngineFn(lookback)
	if err != nil {
		logger.Debug("Found an error when getting a Prom engine to "+
			"calculate start/end time for query rewriting. Falling back to request start/end time",
			zap.String("originalQuery", params.query),
			zap.Duration("lookbackDuration", lookback))
		return start, end
	}

	queryable := fakeQueryable{
		engine:  engine,
		instant: opts.Instant,
	}
	err = queryable.calculateQueryBounds(params.query, params.start, params.end, fetchOpts.Step)
	if err != nil {
		logger.Debug("Found an error when using the Prom engine to "+
			"calculate start/end time for query rewriting. Falling back to request start/end time",
			zap.String("originalQuery", params.query))
		return start, end
	}
	// calculates the query boundaries in roughly the same way as prometheus
	start, end = queryable.getQueryBounds()
	return start, end
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

func maybeRewriteRangeInQuery(query string, expr parser.Node, res time.Duration, multiplier int) (string, bool) {
	updated := false // nolint: ifshort
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
		return expr.String(), true
	}
	return query, false
}

func maybeUpdateLookback(
	params params,
	maxResolution time.Duration,
	opts PrometheusRangeRewriteOptions,
) (time.Duration, bool) {
	var (
		lookback                = params.lookback
		resolutionBasedLookback = maxResolution * time.Duration(opts.ResolutionMultiplier) // nolint: durationcheck
	)
	if !params.isLookbackSet {
		lookback = opts.DefaultLookback
	}
	if lookback < resolutionBasedLookback {
		return resolutionBasedLookback, true
	}
	return lookback, false
}

type fakeQueryable struct {
	engine              *promql.Engine
	instant             bool
	calculatedStartTime time.Time
	calculatedEndTime   time.Time
}

func (f *fakeQueryable) Querier(ctx context.Context, mint, maxt int64) (promstorage.Querier, error) {
	f.calculatedStartTime = xtime.FromUnixMillis(mint)
	f.calculatedEndTime = xtime.FromUnixMillis(maxt)
	// fail here to cause prometheus to give up on query execution
	return nil, errIgnorableQuerierError
}

func (f *fakeQueryable) calculateQueryBounds(
	q string,
	start time.Time,
	end time.Time,
	step time.Duration,
) (err error) {
	var query promql.Query
	if f.instant {
		// startTime and endTime are the same for instant queries
		query, err = f.engine.NewInstantQuery(f, q, start)
	} else {
		query, err = f.engine.NewRangeQuery(f, q, start, end, step)
	}
	if err != nil {
		return err
	}
	// The result returned by Exec will be an error, but that's expected
	if res := query.Exec(context.Background()); !errors.Is(res.Err, errIgnorableQuerierError) {
		return err
	}
	return nil
}

func (f *fakeQueryable) getQueryBounds() (startTime time.Time, endTime time.Time) {
	return f.calculatedStartTime, f.calculatedEndTime
}
