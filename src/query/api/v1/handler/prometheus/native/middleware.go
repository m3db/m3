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
	"strings"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/query/util"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/headers"
	xhttp "github.com/m3db/m3/src/x/http"
	"github.com/m3db/m3/src/x/instrument"
)

// QueryResponse logs the query response time if the request took longer than the configured threshold.
func QueryResponse(threshold time.Duration, iOpts instrument.Options) mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			statusCodeTracking := &xhttp.StatusCodeTracker{ResponseWriter: w}
			w = statusCodeTracking.WrappedResponseWriter()
			base.ServeHTTP(w, r)
			d := time.Since(start)
			if threshold > 0 && d >= threshold {
				logger := logging.WithContext(r.Context(), iOpts)
				query := r.FormValue(queryParam)
				startTime, err := util.ParseTimeStringWithDefault(r.FormValue(startParam), time.Time{})
				if err != nil {
					logger.Warn("failed to parse start for response logging", zap.Error(err))
				}
				endTime, err := util.ParseTimeStringWithDefault(r.FormValue(endParam), startTime)
				if err != nil {
					logger.Warn("failed to parse end for response logging", zap.Error(err))
				}
				fields := []zap.Field{
					zap.Duration("duration", d),
					zap.String("url", r.URL.RequestURI()),
					zap.Int("status", statusCodeTracking.Status),
					zap.String("query", query),
					zap.Time("start", startTime),
					zap.Time("end", endTime),
					zap.Duration("queryRange", endTime.Sub(startTime)),
				}
				for k, v := range r.Header {
					if strings.HasPrefix(k, headers.M3HeaderPrefix) {
						if len(v) == 1 {
							fields = append(fields, zap.String(k, v[0]))
						} else {
							fields = append(fields, zap.Strings(k, v))
						}
					}
				}
				logger.Info("finished handling query request", fields...)
			}
		})
	}
}
