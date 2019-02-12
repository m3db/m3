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
	"net/http"
	"os"
	"sync"
	"time"

	xhttp "github.com/m3db/m3/src/x/net/http"

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

var logger *zap.Logger

// InitWithCores is used to set up a new logger.
func InitWithCores(cores []zapcore.Core) {
	consoleEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())

	highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel
	})
	lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl < zapcore.ErrorLevel
	})

	consoleErrors := zapcore.Lock(os.Stderr)
	consoleDebugging := zapcore.Lock(os.Stdout)

	if cores == nil {
		cores = make([]zapcore.Core, 0, 2)
	}

	cores = append(cores, zapcore.NewCore(consoleEncoder, consoleErrors, highPriority))
	cores = append(cores, zapcore.NewCore(consoleEncoder, consoleDebugging, lowPriority))

	core := zapcore.NewTee(cores...)

	logger = zap.New(core).WithOptions(zap.AddStacktrace(highPriority))
	defer logger.Sync()
}

// NewContext returns a context has a zap logger with the extra fields added.
func NewContext(ctx context.Context, fields ...zapcore.Field) context.Context {
	return context.WithValue(ctx, loggerKey, WithContext(ctx).With(fields...))
}

// NewContextWithGeneratedID returns a context with a generated id with a zap
// logger and an id field.
func NewContextWithGeneratedID(ctx context.Context) context.Context {
	// Attach a rqID with all logs so that its simple to trace the whole call stack
	rqID := uuid.NewRandom()
	return NewContextWithID(ctx, rqID.String())
}

// NewContextWithID returns a context which has a zap logger and an id field.
func NewContextWithID(ctx context.Context, id string) context.Context {
	ctxWithID := context.WithValue(ctx, rqIDKey, id)
	return context.WithValue(ctxWithID, loggerKey, WithContext(ctx).With(zap.String("rqID", id)))
}

// ReadContextID returns the context's id or "undefined".
func ReadContextID(ctx context.Context) string {
	if ctxID, ok := ctx.Value(rqIDKey).(string); ok {
		return ctxID
	}

	return undefinedID
}

// WithContext returns a zap logger with as much context as possible.
func WithContext(ctx context.Context) *zap.Logger {
	if ctx == nil {
		return logger
	}
	if ctxLogger, ok := ctx.Value(loggerKey).(*zap.Logger); ok {
		return ctxLogger
	}

	return logger
}

// withResponseTimeLogging wraps around the given handler, providing response time
// logging.
func withResponseTimeLogging(next http.Handler) http.Handler {
	return withResponseTimeLoggingFunc(next.ServeHTTP)
}

// withResponseTimeLoggingFunc wraps around the http request handler function,
// providing response time logging.
func withResponseTimeLoggingFunc(
	next func(w http.ResponseWriter, r *http.Request),
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		rqCtx := NewContextWithGeneratedID(r.Context())
		logger := WithContext(rqCtx)

		// Propagate the context with the reqId
		next(w, r.WithContext(rqCtx))
		endTime := time.Now()
		d := endTime.Sub(startTime)
		if d > time.Second {
			logger.Info("finished handling request", zap.Time("time", endTime),
				zap.Duration("response", d), zap.String("url", r.URL.RequestURI()))
		}
	}
}

// WithPanicErrorResponder wraps around the given handler,
// providing panic recovery and logging.
func WithPanicErrorResponder(next http.Handler) http.Handler {
	return withPanicErrorResponderFunc(next.ServeHTTP)
}

func withPanicErrorResponderFunc(
	next func(w http.ResponseWriter, r *http.Request),
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeCheckWriter := &responseWrittenResponseWriter{writer: w}
		w = writeCheckWriter

		defer func() {
			if err := recover(); err != nil {
				logger := WithContext(r.Context())
				logger.Error("panic captured", zap.Any("stack", err))

				if !writeCheckWriter.Written() {
					xhttp.Error(w, fmt.Errorf("caught panic: %v", err),
						http.StatusInternalServerError)
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
func WithResponseTimeAndPanicErrorLogging(next http.Handler) http.Handler {
	return WithResponseTimeAndPanicErrorLoggingFunc(next.ServeHTTP)
}

// WithResponseTimeAndPanicErrorLoggingFunc wraps around the http request
// handler function, providing panic recovery and response time logging.
func WithResponseTimeAndPanicErrorLoggingFunc(
	next func(w http.ResponseWriter, r *http.Request),
) http.Handler {
	// Wrap panic first, to be able to capture slow requests that panic in the
	// logs.
	return withResponseTimeLoggingFunc(withPanicErrorResponderFunc(next))
}
