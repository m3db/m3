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
	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/headers"
	xhttp "github.com/m3db/m3/src/x/http"
	"github.com/m3db/m3/src/x/instrument"
)

// QueryResponseOptions are the options to construct the QueryResponse middleware func.
type QueryResponseOptions struct {
	Threshold      time.Duration
	InstrumentOpts instrument.Options
	Clock          clockwork.Clock
}

// QueryResponse logs the query response time if the request took longer than the configured threshold.
func QueryResponse(opts QueryResponseOptions) mux.MiddlewareFunc {
	if opts.Clock == nil {
		opts.Clock = clockwork.NewRealClock()
	}
	return func(base http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := opts.Clock.Now()
			statusCodeTracking := &xhttp.StatusCodeTracker{ResponseWriter: w, TrackError: true}
			w = statusCodeTracking.WrappedResponseWriter()
			base.ServeHTTP(w, r)
			d := opts.Clock.Now().Sub(start)
			if opts.Threshold > 0 && d >= opts.Threshold {
				logger := logging.WithContext(r.Context(), opts.InstrumentOpts)
				query := r.FormValue(queryParam)
				// N.B - instant queries set startParam/endParam to "now" if not set. ParseTime can handle this special
				// "now" value. Use when this middleware ran as the approximate now value.
				startTime, err := ParseTime(r, startParam, start)
				if err != nil {
					logger.Warn("failed to parse start for response logging",
						zap.Error(err),
						zap.String("start", r.FormValue(startParam)))
				}
				endTime, err := ParseTime(r, endParam, start)
				if err != nil {
					logger.Warn("failed to parse end for response logging",
						zap.Error(err),
						zap.String("end", r.FormValue(endParam)))
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
				if statusCodeTracking.ErrMsg != "" {
					fields = append(fields, zap.String("error", statusCodeTracking.ErrMsg))
				}
				fields = appendM3Headers(r.Header, fields)
				fields = appendM3Headers(w.Header(), fields)
				logger.Info("finished handling query request", fields...)
			}
		})
	}
}

func appendM3Headers(h http.Header, fields []zap.Field) []zap.Field {
	for k, v := range h {
		if strings.HasPrefix(k, headers.M3HeaderPrefix) {
			if len(v) == 1 {
				fields = append(fields, zap.String(k, v[0]))
			} else {
				fields = append(fields, zap.Strings(k, v))
			}
		}
	}
	return fields
}
