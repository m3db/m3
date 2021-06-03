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

// Package middleware contains HTTP middleware functions.
package middleware

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/jonboulle/clockwork"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/prometheus/util/httputil"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/net/http/cors"
)

// Register is a func to build the set of middleware functions.
type Register func(opts Options) []mux.MiddlewareFunc

// Options is the set of parameters passed to the Register function.
type Options struct {
	// Common options for all middleware functions.
	InstrumentOpts instrument.Options
	Clock          clockwork.Clock
	Route          *mux.Route

	// Specific options for middleware functions.
	Logging LoggingOptions
	Metrics MetricsOptions
	Source  SourceOptions
}

// OverrideOptions is a function that returns new Options from the provided Options.
type OverrideOptions func(opts Options) Options

// Default is the default list of middleware functions applied if no middleware functions are set in the
// HandlerOptions.
func Default(opts Options) []mux.MiddlewareFunc {
	// The order of middleware is important. Be very careful when reordering existing middleware.
	return []mux.MiddlewareFunc{
		Cors(),
		// install tracing before logging so the trace_id is available for response logging.
		Tracing(opentracing.GlobalTracer(), opts.InstrumentOpts),
		// install source before logging so the source is available for response logging.
		Source(opts),
		RequestID(opts.InstrumentOpts),
		ResponseLogging(opts),
		ResponseMetrics(opts),
		// install panic handler after any middleware that adds extra useful information to the context logger.
		Panic(opts.InstrumentOpts),
		Compression(),
	}
}

// Cors adds CORS headers will be added to all responses.
func Cors() mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return &cors.Handler{
			Handler: base,
			Info: &cors.Info{
				"*": true,
			},
		}
	}
}

// Compression adds suitable response compression based on the client's Accept-Encoding headers.
func Compression() mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return httputil.CompressionHandler{
			Handler: base,
		}
	}
}
