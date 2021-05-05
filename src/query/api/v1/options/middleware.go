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

package options

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/prometheus/util/httputil"

	"github.com/m3db/m3/src/x/net/http/cors"
)

// DefaultMiddleware is the default list of middleware functions applied if no middleware functions are set in the
// HandlerOptions.
func DefaultMiddleware() []mux.MiddlewareFunc {
	return []mux.MiddlewareFunc{
		CorsMiddleware(),
		TracingMiddleware(opentracing.GlobalTracer()),
		CompressionMiddleware(),
	}
}

// TracingMiddleware applies OpenTracing compatible middleware, which will start a span
// for each incoming request.
func TracingMiddleware(tracer opentracing.Tracer) mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return nethttp.Middleware(tracer, base,
			nethttp.OperationNameFunc(func(r *http.Request) string {
				return fmt.Sprintf("%s %s", r.Method, r.URL.Path)
			}))
	}
}

// CorsMiddleware adds CORS headers will be added to all responses.
func CorsMiddleware() mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return &cors.Handler{
			Handler: base,
			Info: &cors.Info{
				"*": true,
			},
		}
	}
}

// CompressionMiddleware adds suitable response compression based on the client's Accept-Encoding headers.
func CompressionMiddleware() mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return httputil.CompressionHandler{
			Handler: base,
		}
	}
}
