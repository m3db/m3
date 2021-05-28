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
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
)

func TestTracing(t *testing.T) {
	core, recorded := observer.New(zapcore.InfoLevel)
	iOpts := instrument.NewOptions().SetLogger(zap.New(core))
	mtr := mocktracer.New()
	r := mux.NewRouter()
	r.Use(Tracing(mtr, iOpts))
	r.HandleFunc(testRoute, func(w http.ResponseWriter, r *http.Request) {
		logging.WithContext(r.Context(), iOpts).Info("test")
	})

	req := httptest.NewRequest("GET", testRoute, nil)
	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)

	spans := mtr.FinishedSpans()
	require.Len(t, spans, 1)
	require.Equal(t, fmt.Sprintf("GET %s", testRoute), spans[0].OperationName)
	require.Len(t, recorded.All(), 1)
	entry := recorded.All()[0]
	require.Equal(t, "test", entry.Message)
	fields := entry.ContextMap()
	require.Len(t, fields, 2)
	require.NotEqual(t, "", fields["trace_id"])
	require.NotEqual(t, "", fields["span_id"])
}

func TestCompression(t *testing.T) {
	router := mux.NewRouter()
	setupTestRouteRouter(router)

	router.Use(Compression())

	req := httptest.NewRequest("GET", testRoute, nil)
	req.Header.Add("Accept-Encoding", "gzip")
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)

	enc, found := res.Header()["Content-Encoding"]
	require.True(t, found)
	require.Equal(t, 1, len(enc))
	assert.Equal(t, "gzip", enc[0])

	cr, err := gzip.NewReader(res.Body)
	require.NoError(t, err)
	body, err := ioutil.ReadAll(cr)
	require.NoError(t, err)
	assert.Equal(t, "hello!", string(body))
}

func TestCors(t *testing.T) {
	router := mux.NewRouter()
	setupTestRouteRouter(router)

	router.Use(Cors())

	req := httptest.NewRequest("GET", testRoute, nil)
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)

	assert.Equal(t, "hello!", res.Body.String())
	assert.Equal(t, "*", res.Header().Get("Access-Control-Allow-Origin"))
}

// TestDefaults is a quick check to see if a new MiddlewareFunc was added to all the appropriate default sets.
func TestDefaults(t *testing.T) {
	noResponse := NoResponseLogging(options.MiddlewareOptions{})
	defaultSet := Default(options.MiddlewareOptions{})
	query := Query(options.MiddlewareOptions{})
	promQuery := PromQuery(options.MiddlewareOptions{})

	// If these checks fail and you're adding a new MiddlewareFunc you need to either:
	// 1. Add the new MiddlewareFunc to all the appropriate default sets
	// 2. Or, update these constraints if they no longer hold.
	require.Equal(t, len(noResponse), len(defaultSet)-1,
		"size of NoResponseLogging is not one less than Default. Did you add a new MiddlewareFunc?")
	require.Equal(t, len(query), len(promQuery), "size of Query and PromQuery should be the same")
	require.Equal(t, len(defaultSet), len(query)-1,
		"size of Query is not one more than Default. Did you add a new MiddlewareFunc?")
}

const testRoute = "/foobar"

func setupTestRouteRouter(r *mux.Router) {
	r.HandleFunc(testRoute, func(writer http.ResponseWriter, r *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("hello!"))
	})
}
