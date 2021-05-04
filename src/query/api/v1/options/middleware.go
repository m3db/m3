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
var DefaultMiddleware = []mux.MiddlewareFunc{
	CorsMiddleware,
	NewTracingMiddleware(opentracing.GlobalTracer()),
	CompressionMiddleware,
}

// NewTracingMiddleware applies OpenTracing compatible middleware, which will start a span
// for each incoming request.
func NewTracingMiddleware(tracer opentracing.Tracer) mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return nethttp.Middleware(tracer, base,
			nethttp.OperationNameFunc(func(r *http.Request) string {
				return fmt.Sprintf("%s %s", r.Method, r.URL.Path)
			}))
	}
}

// CorsMiddleware adds CORS headers will be added to all responses.
var CorsMiddleware = func(base http.Handler) http.Handler {
	return &cors.Handler{
		Handler: base,
		Info: &cors.Info{
			"*": true,
		},
	}
}

// CompressionMiddleware adds suitable response compression based on the client's Accept-Encoding headers.
var CompressionMiddleware = func(base http.Handler) http.Handler {
	return httputil.CompressionHandler{
		Handler: base,
	}
}
