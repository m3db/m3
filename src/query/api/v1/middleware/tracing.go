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
	"fmt"
	"net/http"
	"strconv"

	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/gorilla/mux"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/uber/jaeger-client-go"
	"go.uber.org/zap"
)

// Tracing applies OpenTracing compatible middleware, which will start a span
// for each incoming request. Additionally if iOpts is non-nil the trace_id and span_id are added as fields to the
// request scoped logger.
func Tracing(tracer opentracing.Tracer, iOpts instrument.Options) mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return nethttp.MiddlewareFunc(
			tracer,
			func(w http.ResponseWriter, r *http.Request) {
				span := opentracing.SpanFromContext(r.Context())
				if span != nil && iOpts != nil {
					var (
						traceID string
						spanID  string
					)
					switch sCtx := span.Context().(type) {
					case mocktracer.MockSpanContext:
						traceID = strconv.Itoa(sCtx.TraceID)
						spanID = strconv.Itoa(sCtx.SpanID)
					case jaeger.SpanContext:
						traceID = sCtx.TraceID().String()
						spanID = sCtx.SpanID().String()
					}
					if spanID != "" && traceID != "" {
						r = r.WithContext(logging.NewContext(r.Context(), iOpts,
							zap.String("trace_id", traceID),
							zap.String("span_id", spanID)))
					}
				}
				base.ServeHTTP(w, r)
			},
			nethttp.OperationNameFunc(func(r *http.Request) string {
				return fmt.Sprintf("%s %s", r.Method, r.URL.Path)
			}))
	}
}
