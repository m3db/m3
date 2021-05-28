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

	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
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

type cleanupFn func()

func setup(t *testing.T, ctxLogger bool) (
	*os.File,
	*os.File,
	*http.Request,
	instrument.Options,
	cleanupFn,
) {
	success := false

	stdout, err := ioutil.TempFile("", "temp-log-file-out")
	require.NoError(t, err, "Couldn't create a temporary out file for test.")
	defer func() {
		if success {
			return
		}
		_ = os.Remove(stdout.Name())
	}()

	stderr, err := ioutil.TempFile("", "temp-log-file-err")
	require.NoError(t, err, "Couldn't create a temporary err file for test.")
	defer func() {
		if success {
			return
		}
		_ = os.Remove(stderr.Name())
	}()

	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = []string{stdout.Name()}
	cfg.ErrorOutputPaths = []string{stderr.Name()}

	templogger, err := cfg.Build()
	require.NoError(t, err)

	instrumentOpts := instrument.NewOptions().
		SetLogger(templogger)

	req, err := http.NewRequest("GET", "cool", nil)
	require.NoError(t, err)

	if ctxLogger {
		// Only stacktrace `error` priority and higher
		highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
			return lvl >= zapcore.ErrorLevel
		})

		templogger = templogger.WithOptions(zap.AddStacktrace(highPriority)).With(zap.String("foo", "bar"))
		req = req.WithContext(logging.NewContextWithLogger(context.TODO(), templogger))
	}

	// Success to true to avoid removing log files
	success = true
	cleanup := func() {
		_ = os.Remove(stdout.Name())
		_ = os.Remove(stderr.Name())
	}
	return stdout, stderr, req, instrumentOpts, cleanup
}

func TestPanicErrorResponder(t *testing.T) {
	stdout, stderr, req, instrumentOpts, cleanup := setup(t, true)
	defer cleanup()

	writer := &httpWriter{written: make([]string, 0, 10)}
	deadbeef := Panic(instrumentOpts).Middleware(panicHandler{})
	deadbeef.ServeHTTP(writer, req)

	assert.Equal(t, 500, writer.status)
	require.Equal(t, 1, len(writer.written))
	assert.JSONEq(t, `{"status":"error","error":"caught panic: beef"}`, writer.written[0])

	assertNoErrorLogs(t, stderr)
	b, err := ioutil.ReadAll(stdout)
	require.NoError(t, err)
	outstrs := strings.Split(string(b), "\n")

	assert.True(t, strings.Contains(outstrs[0], "ERROR"))
	assert.True(t, strings.Contains(outstrs[0], "panic captured"))
	assert.True(t, strings.Contains(outstrs[0], `"stack": "beef"`))
	assert.True(t, strings.Contains(outstrs[0], `"foo": "bar"`))

	// Assert that stack trace is written
	count := 0
	for _, s := range outstrs {
		if strings.Contains(s, "runtime.gopanic") {
			count++
		}
	}

	assert.True(t, count > 0)
}

func assertNoErrorLogs(t *testing.T, stderr *os.File) {
	b, err := ioutil.ReadAll(stderr)
	require.NoError(t, err)
	assert.Equal(t, 0, len(b))
}

func assertPanicLogsWritten(t *testing.T, stdout, stderr *os.File) {
	assertNoErrorLogs(t, stderr)
	b, err := ioutil.ReadAll(stdout)
	require.NoError(t, err)
	outstrs := strings.Split(string(b), "\n")

	assert.True(t, strings.Contains(outstrs[0], "ERROR"))
	assert.True(t, strings.Contains(outstrs[0], "panic captured"))
	assert.True(t, strings.Contains(outstrs[0], `"stack": "err"`))
	assert.True(t, strings.Contains(outstrs[0], `"foo": "bar"`))

	// Assert that stack trace is written
	count := 0
	for _, s := range outstrs {
		if strings.Contains(s, "runtime.gopanic") {
			count++
		}
	}
	assert.True(t, count > 0)

	count = 0
	for _, s := range outstrs {
		if strings.Contains(s, "cannot write error for request; already written\t{\"foo\": \"bar\"}") {
			count++
		}
	}
	assert.True(t, count > 0)
}

func TestPanicErrorResponderOnlyIfNotWrittenRequest(t *testing.T) {
	stdout, stderr, req, instrumentOpts, cleanup := setup(t, true)
	defer cleanup()

	writer := &httpWriter{written: make([]string, 0, 10)}
	deadbeef := Panic(instrumentOpts).Middleware(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("foo"))
			panic("err")
		}))
	deadbeef.ServeHTTP(writer, req)

	assert.Equal(t, 0, writer.status)
	require.Equal(t, 1, len(writer.written))
	assert.Equal(t, "foo", writer.written[0])

	// Verify that panic capture is still logged.
	assertPanicLogsWritten(t, stdout, stderr)
}

func TestPanicErrorResponderOnlyIfNotWrittenHeader(t *testing.T) {
	stdout, stderr, req, instrumentOpts, cleanup := setup(t, true)
	defer cleanup()

	writer := &httpWriter{written: make([]string, 0, 10)}
	deadbeef := Panic(instrumentOpts).Middleware(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(404)
			panic("err")
		}))
	deadbeef.ServeHTTP(writer, req)

	assert.Equal(t, 404, writer.status)
	require.Equal(t, 0, len(writer.written))

	// Verify that panic capture is still logged.
	assertPanicLogsWritten(t, stdout, stderr)
}

type delayHandler struct {
	delay time.Duration
}

func (h delayHandler) ServeHTTP(_ http.ResponseWriter, _ *http.Request) {
	time.Sleep(h.delay)
}

func TestWithResponseTimeLogging(t *testing.T) {
	stdout, stderr, req, instrumentOpts, cleanup := setup(t, false)
	defer cleanup()

	m := ResponseLogging(time.Second, instrumentOpts)
	slowHandler := m.Middleware(&delayHandler{delay: time.Second})
	fastHandler := m.Middleware(&delayHandler{delay: time.Duration(0)})

	writer := &httpWriter{written: make([]string, 0, 10)}

	slowHandler.ServeHTTP(writer, req)
	fastHandler.ServeHTTP(writer, req)

	require.Equal(t, 0, len(writer.written))
	assertNoErrorLogs(t, stderr)

	b, err := ioutil.ReadAll(stdout)
	require.NoError(t, err)
	outstrs := strings.Split(string(b), "\n")

	require.Equal(t, 2, len(outstrs))
	out := outstrs[0]

	assert.True(t, strings.Contains(out, "INFO"))
	assert.True(t, strings.Contains(out, "finished handling request"))
	assert.True(t, strings.Contains(out, `"url": "cool"`))
	assert.True(t, strings.Contains(out, `duration": "1.`))
}

func TestWithResponseTimeAndPanicErrorLoggingFunc(t *testing.T) {
	stdout, stderr, req, instrumentOpts, cleanup := setup(t, true)
	defer cleanup()

	writer := &httpWriter{written: make([]string, 0, 10)}
	slowPanic := ResponseLogging(time.Second, instrumentOpts).Middleware(
		Panic(instrumentOpts).Middleware(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(time.Second)
				panic("err")
			})))

	slowPanic.ServeHTTP(writer, req)

	assert.Equal(t, 500, writer.status)
	require.Equal(t, 1, len(writer.written))
	assert.JSONEq(t, `{"status":"error","error":"caught panic: err"}`, writer.written[0])

	assertNoErrorLogs(t, stderr)

	b, err := ioutil.ReadAll(stdout)
	require.NoError(t, err)
	outstrs := strings.Split(string(b), "\n")
	assert.True(t, strings.Contains(outstrs[0], "ERROR"))
	assert.True(t, strings.Contains(outstrs[0], "panic captured"))
	assert.True(t, strings.Contains(outstrs[0], `"stack": "err"`))
	assert.True(t, strings.Contains(outstrs[0], `"foo": "bar"`))

	// Assert that stack trace is written
	count := 0
	for _, s := range outstrs {
		if strings.Contains(s, "runtime.gopanic") {
			count++
		}
	}

	assert.True(t, count > 0)

	// assert that the second last line of the log captures response time.
	last := outstrs[len(outstrs)-2]

	assert.True(t, strings.Contains(last, "INFO"))
	assert.True(t, strings.Contains(last, "finished handling request"))
	assert.True(t, strings.Contains(last, `"url": "cool"`))
	assert.True(t, strings.Contains(last, `duration": "1.`))
}
