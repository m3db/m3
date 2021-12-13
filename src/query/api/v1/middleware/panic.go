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
	"sync"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/m3db/m3/src/x/instrument"
)

var highPriority = zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
	return lvl >= zapcore.ErrorLevel
})

// Panic recovers from panics and logs the error/stacktrace if possible.
func Panic(_ instrument.Options) mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			base.ServeHTTP(w, r)
		})
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
