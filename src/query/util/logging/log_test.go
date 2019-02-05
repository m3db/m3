// Copyright (c) 2019 Uber Technologies, Inc.
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
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type httpWriter struct {
	written []string
	status  int
}

func (w *httpWriter) Header() http.Header {
	return make(http.Header)
}

func (w *httpWriter) WriteHeader(statusCode int) {
	w.status = statusCode
}

func (w *httpWriter) Write(b []byte) (int, error) {
	w.written = append(w.written, string(b))
	return len(b), nil
}

type panicHandler struct{}

func (h panicHandler) ServeHTTP(_ http.ResponseWriter, _ *http.Request) {
	panic("beef")
}

func setup(t *testing.T, ctxLogger bool) (*os.File, *os.File, *http.Request) {
	stdout, err := ioutil.TempFile("", "temp-log-file-out")
	require.NoError(t, err, "Couldn't create a temporary out file for test.")
	defer os.Remove(stdout.Name())

	stderr, err := ioutil.TempFile("", "temp-log-file-err")
	require.NoError(t, err, "Couldn't create a temporary err file for test.")
	defer os.Remove(stderr.Name())

	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = []string{stdout.Name()}
	cfg.ErrorOutputPaths = []string{stderr.Name()}

	templogger, err := cfg.Build()
	require.NoError(t, err)

	req, err := http.NewRequest("GET", "cool", nil)
	require.NoError(t, err)

	if ctxLogger {
		ctx := context.WithValue(context.TODO(), loggerKey, templogger)
		ctx = NewContext(ctx, zap.String("foo", "bar"))
		req = req.WithContext(ctx)
	} else {
		core := templogger.Core()
		InitWithCores([]zapcore.Core{core})
	}

	return stdout, stderr, req
}

func TestPanicErrorReporter(t *testing.T) {
	stdout, stderr, req := setup(t, true)
	defer os.Remove(stdout.Name())
	defer os.Remove(stderr.Name())

	writer := &httpWriter{written: make([]string, 0, 10)}
	deadbeef := WithPanicErrorResponder(panicHandler{})
	deadbeef.ServeHTTP(writer, req)

	assert.Equal(t, 500, writer.status)
	require.Equal(t, 1, len(writer.written))
	assert.Equal(t, "{\"error\":\"beef\"}\n", writer.written[0])

	b, err := ioutil.ReadAll(stderr)
	require.NoError(t, err)
	assert.Equal(t, 0, len(b))

	b, err = ioutil.ReadAll(stdout)
	require.NoError(t, err)
	outstrs := strings.Split(string(b), "\n")

	assert.True(t, strings.Contains(outstrs[0], "ERROR"))
	assert.True(t, strings.Contains(outstrs[0], "panic captured"))
	assert.True(t, strings.Contains(outstrs[0], `"stack": "beef"`))
	assert.True(t, strings.Contains(outstrs[0], `"foo": "bar"`))

	// Assert that stack trace is written
	// `log.go` should appear in the output four times, once for the error log
	// method, once for the error log location, once for the call to `next(..)`,
	// and once for the panicHandler
	count := 0
	for _, s := range outstrs {
		if strings.Contains(s, "log.go") {
			count++
		}
	}

	assert.Equal(t, 4, count)

	// `log_test` should appear in the output twice, once for the call in the
	// deadbeef method, and once for the ServeHttp call.
	count = 0
	for _, s := range outstrs {
		if strings.Contains(s, "log_test.go") {
			count++
		}
	}
}

type delayHandler struct {
	delay time.Duration
}

func (h delayHandler) ServeHTTP(_ http.ResponseWriter, _ *http.Request) {
	time.Sleep(h.delay)
}

func TestWithResponseTimeLogging(t *testing.T) {
	slowHandler := WithResponseTimeLogging(delayHandler{delay: time.Second})
	fastHandler := WithResponseTimeLogging(delayHandler{delay: time.Duration(0)})

	stdout, stderr, req := setup(t, false)
	defer os.Remove(stdout.Name())
	defer os.Remove(stderr.Name())

	writer := &httpWriter{written: make([]string, 0, 10)}

	slowHandler.ServeHTTP(writer, req)
	fastHandler.ServeHTTP(writer, req)

	require.Equal(t, 0, len(writer.written))
	b, err := ioutil.ReadAll(stderr)
	require.NoError(t, err)
	assert.Equal(t, 0, len(b))

	b, err = ioutil.ReadAll(stdout)
	require.NoError(t, err)
	outstrs := strings.Split(string(b), "\n")

	require.Equal(t, 2, len(outstrs))
	out := outstrs[0]

	assert.True(t, strings.Contains(out, "INFO"))
	assert.True(t, strings.Contains(out, "finished handling request"))
	assert.True(t, strings.Contains(out, `"url": "cool"`))
	assert.True(t, strings.Contains(out, `response": "1.`))
}
