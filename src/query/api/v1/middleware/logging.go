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
	"strings"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/headers"
	xhttp "github.com/m3db/m3/src/x/http"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/gorilla/mux"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
)

// RequestID populates the request scoped logger with a unique request ID.
func RequestID(iOpts instrument.Options) mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rqCtx := logging.NewContextWithGeneratedID(r.Context(), iOpts)

			sp := opentracing.SpanFromContext(rqCtx)
			if sp != nil {
				rqID := logging.ReadContextID(rqCtx)
				sp.SetTag("rqID", rqID)
			}

			// Propagate the context with the reqId
			base.ServeHTTP(w, r.WithContext(rqCtx))
		})
	}
}

// LoggingOptions are the options for the logging middleware.
type LoggingOptions struct {
	Threshold time.Duration
	Disabled  bool
	Fields    Fields
}

// NewLoggingOptions returns new options from the config.
func NewLoggingOptions(c config.LoggingMiddlewareConfiguration) LoggingOptions {
	if c.Threshold == 0 {
		c.Threshold = time.Second
	}
	return LoggingOptions{
		Threshold: c.Threshold,
		Disabled:  c.Disabled,
	}
}

// Append the provided Fields to these Fields.
func (f Fields) Append(other Fields) Fields {
	return func(r *http.Request, start time.Time) []zap.Field {
		var fields []zap.Field
		if f != nil {
			fields = f(r, start)
		}
		if other != nil {
			fields = append(fields, other(r, start)...)
		}
		return fields
	}
}

// WithNoResponseLogging disables response logging for a route.
var WithNoResponseLogging = func(opts Options) Options {
	opts.Logging.Disabled = true
	return opts
}

// Fields returns additional logging fields to add to the response log message.
type Fields func(r *http.Request, start time.Time) []zap.Field

// ResponseLogging logs the response time if the request took longer than the configured threshold.
func ResponseLogging(opts Options) mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := opts.Clock.Now()
			statusCodeTracking := &xhttp.StatusCodeTracker{ResponseWriter: w, TrackError: true}
			w = statusCodeTracking.WrappedResponseWriter()
			base.ServeHTTP(w, r)
			d := opts.Clock.Now().Sub(start)
			if !opts.Logging.Disabled && d >= opts.Logging.Threshold {
				logger := logging.WithContext(r.Context(), opts.InstrumentOpts)
				fields := []zap.Field{
					zap.Duration("duration", d),
					zap.String("url", r.URL.RequestURI()),
					zap.Int("status", statusCodeTracking.Status),
				}
				if statusCodeTracking.ErrMsg != "" {
					fields = append(fields, zap.String("error", statusCodeTracking.ErrMsg))
				}
				fields = appendM3Headers(r.Header, fields)
				fields = appendM3Headers(w.Header(), fields)
				if opts.Logging.Fields != nil {
					fields = append(fields, opts.Logging.Fields(r, start)...)
				}
				logger.Info("finished handling request", fields...)
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
