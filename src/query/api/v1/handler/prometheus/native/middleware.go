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

// Package native handles the prometheus api endpoints using m3.
package native

import (
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/m3db/m3/src/query/api/v1/middleware"
)

// WithQueryParams adds the query request parameters to the middleware options.
var WithQueryParams middleware.OverrideOptions = func(opts middleware.Options) middleware.Options {
	opts.Logging.Fields = opts.Logging.Fields.Append(func(r *http.Request, start time.Time) []zap.Field {
		params, err := middlewareParseParams(r, start)
		if err != nil {
			opts.InstrumentOpts.Logger().Warn("failed to parse query params for response logging", zap.Error(err))
			return nil
		}
		return []zap.Field{
			zap.String("query", params.Query),
			zap.Time("start", params.Start),
			zap.Time("end", params.End),
			zap.Duration("queryRange", params.Range()),
		}
	})
	opts.Metrics.ParseQueryParams = middlewareParseParams
	return opts
}

var middlewareParseParams middleware.ParseQueryParams = func(r *http.Request, requestStart time.Time) (
	middleware.QueryParams, error) {
	query := r.FormValue(queryParam)
	if query == "" {
		return middleware.QueryParams{}, nil
	}
	// N.B - instant queries set startParam/endParam to "now" if not set. ParseTime can handle this special
	// "now" value. Use when this middleware ran as the approximate now value.
	start, err := ParseTime(r, startParam, requestStart)
	if err != nil {
		return middleware.QueryParams{}, err
	}

	end, err := ParseTime(r, endParam, requestStart)
	if err != nil {
		return middleware.QueryParams{}, err
	}
	return middleware.QueryParams{
		Query: query,
		Start: start,
		End:   end,
	}, nil
}
