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

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
)

// QueryResponse logs the query response time if the request took longer than the configured threshold.
func QueryResponse(threshold time.Duration, iOpts instrument.Options) mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startTime := time.Now()
			base.ServeHTTP(w, r)
			d := time.Since(startTime)
			if threshold > 0 && d >= threshold {
				logger := logging.WithContext(r.Context(), iOpts)
				query, err := parseQuery(r)
				if err != nil {
					logger.Warn("failed to parse query for response logging", zap.Error(err))
				}
				startTime, err := parseTime(r, startParam, time.Now())
				if err != nil {
					logger.Warn("failed to parse startTime for response logging", zap.Error(err))
				}
				endTime, err := parseTime(r, endParam, time.Now())
				if err != nil {
					logger.Warn("failed to parse endTime for response logging", zap.Error(err))
				}
				logger.Info("finished handling query request",
					zap.Duration("duration", d),
					zap.String("url", r.URL.RequestURI()),
					zap.String("query", query),
					zap.Time("startTime", startTime),
					zap.Time("endTime", endTime),
					zap.Duration("queryRange", endTime.Sub(startTime)),
				)
			}
		})
	}
}
