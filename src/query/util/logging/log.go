// Copyright (c) 2018 Uber Technologies, Inc.
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

package logging

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pborman/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type loggerKeyType int

const (
	loggerKey loggerKeyType = iota
	rqIDKey

	undefinedID = "undefined"
)

var (
	highPriority = zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel
	})
	lowPriority = zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl < zapcore.ErrorLevel
	})
)

// NewContext returns a context has a zap logger with the extra fields added.
func NewContext(
	ctx context.Context,
	instrumentOpts instrument.Options,
	fields ...zapcore.Field,
) context.Context {
	return context.WithValue(ctx, loggerKey,
		WithContext(ctx, instrumentOpts).With(fields...))
}

// NewContextWithGeneratedID returns a context with a generated id with a zap
// logger and an id field.
func NewContextWithGeneratedID(
	ctx context.Context,
	instrumentOpts instrument.Options,
) context.Context {
	// Attach a rqID with all logs so that its simple to trace the whole call stack
	rqID := uuid.NewRandom()
	return NewContextWithID(ctx, rqID.String(), instrumentOpts)
}

// NewContextWithID returns a context which has a zap logger and an id field.
func NewContextWithID(
	ctx context.Context,
	id string,
	instrumentOpts instrument.Options,
) context.Context {
	ctxWithID := context.WithValue(ctx, rqIDKey, id)
	return context.WithValue(ctxWithID, loggerKey,
		WithContext(ctx, instrumentOpts).With(zap.String("rqID", id)))
}

// ReadContextID returns the context's id or "undefined".
func ReadContextID(ctx context.Context) string {
	if ctxID, ok := ctx.Value(rqIDKey).(string); ok {
		return ctxID
	}

	return undefinedID
}

// WithContext returns a zap logger with as much context as possible.
func WithContext(ctx context.Context, instrumentOpts instrument.Options) *zap.Logger {
	if ctx == nil {
		return instrumentOpts.Logger()
	}
	if ctxLogger, ok := ctx.Value(loggerKey).(*zap.Logger); ok {
		return ctxLogger
	}

	return instrumentOpts.Logger()
}

// withResponseTimeLogging wraps around the given handler, providing response time
// logging.
func withResponseTimeLogging(
	next http.Handler,
	instrumentOpts instrument.Options,
) http.Handler {
	return withResponseTimeLoggingFunc(next.ServeHTTP, instrumentOpts)
}

// withResponseTimeLoggingFunc wraps around the http request handler function,
// providing response time logging.
func withResponseTimeLoggingFunc(
	next func(w http.ResponseWriter, r *http.Request),
	instrumentOpts instrument.Options,
	opts ...MiddlewareOption,
) http.HandlerFunc {
	middlewareOpts := defaultMiddlewareOptions
	for _, opt := range opts {
		opt(&middlewareOpts)
	}

	threshold := middlewareOpts.responseLogThreshold
	return func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		for _, fn := range middlewareOpts.preHooks {
			fn(r)
		}

		// Track status code.
		statusCodeTracking := &statusCodeTracker{ResponseWriter: w}
		w = statusCodeTracking.wrappedResponseWriter()

		rqCtx := NewContextWithGeneratedID(r.Context(), instrumentOpts)
		logger := WithContext(rqCtx, instrumentOpts)

		sp := opentracing.SpanFromContext(rqCtx)
		if sp != nil {
			rqID := ReadContextID(rqCtx)
			sp.SetTag("rqID", rqID)
		}

		// Propagate the context with the reqId
		next(w, r.WithContext(rqCtx))
		endTime := time.Now()
		d := endTime.Sub(startTime)
		if threshold > 0 && d >= threshold {
			logger.Info("finished handling request", zap.Time("time", endTime),
				zap.Duration("response", d), zap.String("url", r.URL.RequestURI()))
		}

		for _, fn := range middlewareOpts.postHooks {
			fn(r, RequestMiddlewareMetadata{
				Start:       startTime,
				End:         endTime,
				Duration:    d,
				WroteHeader: statusCodeTracking.wroteHeader,
				StatusCode:  statusCodeTracking.status,
			})
		}
	}
}

// WithPanicErrorResponder wraps around the given handler,
// providing panic recovery and logging.
func WithPanicErrorResponder(
	next http.Handler,
	instrumentOpts instrument.Options,
) http.Handler {
	return withPanicErrorResponderFunc(next.ServeHTTP, instrumentOpts)
}

func withPanicErrorResponderFunc(
	next func(w http.ResponseWriter, r *http.Request),
	instrumentOpts instrument.Options,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeCheckWriter := &responseWrittenResponseWriter{writer: w}
		w = writeCheckWriter

		defer func() {
			if err := recover(); err != nil {
				logger := WithContext(r.Context(), instrumentOpts).
					WithOptions(zap.AddStacktrace(highPriority))
				logger.Error("panic captured", zap.Any("stack", err))

				if !writeCheckWriter.Written() {
					xhttp.WriteError(w, fmt.Errorf("caught panic: %v", err))
					return
				}

				// cannot write the error back to the caller, some contents already written.
				logger.Warn("cannot write error for request; already written")
			}
		}()

		next(w, r)
	}
}

type responseWrittenResponseWriter struct {
	sync.RWMutex
	writer  http.ResponseWriter
	written bool
}

func (w *responseWrittenResponseWriter) Written() bool {
	w.RLock()
	v := w.written
	w.RUnlock()
	return v
}

func (w *responseWrittenResponseWriter) setWritten() {
	w.RLock()
	if w.written {
		return
	}
	w.RUnlock()

	w.Lock()
	w.written = true
	w.Unlock()
}

func (w *responseWrittenResponseWriter) Header() http.Header {
	return w.writer.Header()
}

func (w *responseWrittenResponseWriter) Write(d []byte) (int, error) {
	w.setWritten()
	return w.writer.Write(d)
}

func (w *responseWrittenResponseWriter) WriteHeader(statusCode int) {
	w.setWritten()
	w.writer.WriteHeader(statusCode)
}

// WithResponseTimeAndPanicErrorLogging wraps around the given handler,
// providing panic recovery and response time logging.
func WithResponseTimeAndPanicErrorLogging(
	next http.Handler,
	instrumentOpts instrument.Options,
	opts ...MiddlewareOption,
) http.Handler {
	return WithResponseTimeAndPanicErrorLoggingFunc(next.ServeHTTP,
		instrumentOpts, opts...)
}

// WithResponseTimeAndPanicErrorLoggingFunc wraps around the http request
// handler function, providing panic recovery and response time logging.
func WithResponseTimeAndPanicErrorLoggingFunc(
	next func(w http.ResponseWriter, r *http.Request),
	instrumentOpts instrument.Options,
	opts ...MiddlewareOption,
) http.Handler {
	// Wrap panic first, to be able to capture slow requests that panic in the
	// logs.
	return withResponseTimeLoggingFunc(
		withPanicErrorResponderFunc(next, instrumentOpts),
		instrumentOpts, opts...)
}

type middlewareOptions struct {
	responseLogThreshold time.Duration
	preHooks             []PreRequestMiddleware
	postHooks            []PostRequestMiddleware
}

var (
	defaultMiddlewareOptions = middlewareOptions{
		responseLogThreshold: time.Second,
	}
)

// PreRequestMiddleware is middleware that runs before a request.
type PreRequestMiddleware func(req *http.Request)

// RequestMiddlewareMetadata is metadata available to middleware about a request.
type RequestMiddlewareMetadata struct {
	Start       time.Time
	End         time.Time
	Duration    time.Duration
	WroteHeader bool
	StatusCode  int
}

// PostRequestMiddleware is middleware that runs before a request.
type PostRequestMiddleware func(req *http.Request, meta RequestMiddlewareMetadata)

// MiddlewareOption is an option to pass to a middleware.
type MiddlewareOption func(*middlewareOptions)

// WithResponseLogThreshold is a middleware option to set response log threshold.
func WithResponseLogThreshold(threshold time.Duration) MiddlewareOption {
	return func(opts *middlewareOptions) {
		opts.responseLogThreshold = threshold
	}
}

// WithNoResponseLog is a middleware option to disable response logging.
func WithNoResponseLog() MiddlewareOption {
	return func(opts *middlewareOptions) {
		opts.responseLogThreshold = 0
	}
}

// WithPreRequestMiddleware is a middleware option to set pre-request middleware.
func WithPreRequestMiddleware(m PreRequestMiddleware) MiddlewareOption {
	return func(opts *middlewareOptions) {
		opts.preHooks = append(opts.preHooks, m)
	}
}

// WithPostRequestMiddleware is a middleware option to set post-request middleware.
func WithPostRequestMiddleware(m PostRequestMiddleware) MiddlewareOption {
	return func(opts *middlewareOptions) {
		opts.postHooks = append(opts.postHooks, m)
	}
}

type statusCodeTracker struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusCodeTracker) WriteHeader(status int) {
	w.status = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusCodeTracker) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
		w.status = 200
	}
	return w.ResponseWriter.Write(b)
}

// wrappedResponseWriter returns a wrapped version of the original
// ResponseWriter and only implements the same combination of additional
// interfaces as the original.  This implementation is based on
// https://github.com/felixge/httpsnoop.
func (w *statusCodeTracker) wrappedResponseWriter() http.ResponseWriter {
	var (
		hj, i0 = w.ResponseWriter.(http.Hijacker)
		cn, i1 = w.ResponseWriter.(http.CloseNotifier)
		pu, i2 = w.ResponseWriter.(http.Pusher)
		fl, i3 = w.ResponseWriter.(http.Flusher)
		rf, i4 = w.ResponseWriter.(io.ReaderFrom)
	)

	switch {
	case !i0 && !i1 && !i2 && !i3 && !i4:
		return struct {
			http.ResponseWriter
		}{w}
	case !i0 && !i1 && !i2 && !i3 && i4:
		return struct {
			http.ResponseWriter
			io.ReaderFrom
		}{w, rf}
	case !i0 && !i1 && !i2 && i3 && !i4:
		return struct {
			http.ResponseWriter
			http.Flusher
		}{w, fl}
	case !i0 && !i1 && !i2 && i3 && i4:
		return struct {
			http.ResponseWriter
			http.Flusher
			io.ReaderFrom
		}{w, fl, rf}
	case !i0 && !i1 && i2 && !i3 && !i4:
		return struct {
			http.ResponseWriter
			http.Pusher
		}{w, pu}
	case !i0 && !i1 && i2 && !i3 && i4:
		return struct {
			http.ResponseWriter
			http.Pusher
			io.ReaderFrom
		}{w, pu, rf}
	case !i0 && !i1 && i2 && i3 && !i4:
		return struct {
			http.ResponseWriter
			http.Pusher
			http.Flusher
		}{w, pu, fl}
	case !i0 && !i1 && i2 && i3 && i4:
		return struct {
			http.ResponseWriter
			http.Pusher
			http.Flusher
			io.ReaderFrom
		}{w, pu, fl, rf}
	case !i0 && i1 && !i2 && !i3 && !i4:
		return struct {
			http.ResponseWriter
			http.CloseNotifier
		}{w, cn}
	case !i0 && i1 && !i2 && !i3 && i4:
		return struct {
			http.ResponseWriter
			http.CloseNotifier
			io.ReaderFrom
		}{w, cn, rf}
	case !i0 && i1 && !i2 && i3 && !i4:
		return struct {
			http.ResponseWriter
			http.CloseNotifier
			http.Flusher
		}{w, cn, fl}
	case !i0 && i1 && !i2 && i3 && i4:
		return struct {
			http.ResponseWriter
			http.CloseNotifier
			http.Flusher
			io.ReaderFrom
		}{w, cn, fl, rf}
	case !i0 && i1 && i2 && !i3 && !i4:
		return struct {
			http.ResponseWriter
			http.CloseNotifier
			http.Pusher
		}{w, cn, pu}
	case !i0 && i1 && i2 && !i3 && i4:
		return struct {
			http.ResponseWriter
			http.CloseNotifier
			http.Pusher
			io.ReaderFrom
		}{w, cn, pu, rf}
	case !i0 && i1 && i2 && i3 && !i4:
		return struct {
			http.ResponseWriter
			http.CloseNotifier
			http.Pusher
			http.Flusher
		}{w, cn, pu, fl}
	case !i0 && i1 && i2 && i3 && i4:
		return struct {
			http.ResponseWriter
			http.CloseNotifier
			http.Pusher
			http.Flusher
			io.ReaderFrom
		}{w, cn, pu, fl, rf}
	case i0 && !i1 && !i2 && !i3 && !i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
		}{w, hj}
	case i0 && !i1 && !i2 && !i3 && i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			io.ReaderFrom
		}{w, hj, rf}
	case i0 && !i1 && !i2 && i3 && !i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.Flusher
		}{w, hj, fl}
	case i0 && !i1 && !i2 && i3 && i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.Flusher
			io.ReaderFrom
		}{w, hj, fl, rf}
	case i0 && !i1 && i2 && !i3 && !i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.Pusher
		}{w, hj, pu}
	case i0 && !i1 && i2 && !i3 && i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.Pusher
			io.ReaderFrom
		}{w, hj, pu, rf}
	case i0 && !i1 && i2 && i3 && !i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.Pusher
			http.Flusher
		}{w, hj, pu, fl}
	case i0 && !i1 && i2 && i3 && i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.Pusher
			http.Flusher
			io.ReaderFrom
		}{w, hj, pu, fl, rf}
	case i0 && i1 && !i2 && !i3 && !i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.CloseNotifier
		}{w, hj, cn}
	case i0 && i1 && !i2 && !i3 && i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.CloseNotifier
			io.ReaderFrom
		}{w, hj, cn, rf}
	case i0 && i1 && !i2 && i3 && !i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.CloseNotifier
			http.Flusher
		}{w, hj, cn, fl}
	case i0 && i1 && !i2 && i3 && i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.CloseNotifier
			http.Flusher
			io.ReaderFrom
		}{w, hj, cn, fl, rf}
	case i0 && i1 && i2 && !i3 && !i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.CloseNotifier
			http.Pusher
		}{w, hj, cn, pu}
	case i0 && i1 && i2 && !i3 && i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.CloseNotifier
			http.Pusher
			io.ReaderFrom
		}{w, hj, cn, pu, rf}
	case i0 && i1 && i2 && i3 && !i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.CloseNotifier
			http.Pusher
			http.Flusher
		}{w, hj, cn, pu, fl}
	case i0 && i1 && i2 && i3 && i4:
		return struct {
			http.ResponseWriter
			http.Hijacker
			http.CloseNotifier
			http.Pusher
			http.Flusher
			io.ReaderFrom
		}{w, hj, cn, pu, fl, rf}
	default:
		return struct {
			http.ResponseWriter
		}{w}
	}
}
