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

	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestContextWithID(t *testing.T) {
	ctx := context.TODO()
	assert.Equal(t, undefinedID, ReadContextID(ctx))

	id := "cool id"
	ctx = NewContextWithID(ctx, id, instrument.NewOptions())
	assert.Equal(t, id, ReadContextID(ctx))
}

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
		os.Remove(stdout.Name())
	}()

	stderr, err := ioutil.TempFile("", "temp-log-file-err")
	require.NoError(t, err, "Couldn't create a temporary err file for test.")
	defer func() {
		if success {
			return
		}
		os.Remove(stderr.Name())
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

		templogger = templogger.WithOptions(zap.AddStacktrace(highPriority))
		ctx := context.WithValue(context.TODO(), loggerKey, templogger)
		ctx = NewContext(ctx, instrumentOpts, zap.String("foo", "bar"))
		req = req.WithContext(ctx)
	}

	// Success to true to avoid removing log files
	success = true
	cleanup := func() {
		os.Remove(stdout.Name())
		os.Remove(stderr.Name())
	}
	return stdout, stderr, req, instrumentOpts, cleanup
}

func TestPanicErrorResponder(t *testing.T) {
	stdout, stderr, req, instrumentOpts, cleanup := setup(t, true)
	defer cleanup()

	writer := &httpWriter{written: make([]string, 0, 10)}
	deadbeef := WithPanicErrorResponder(panicHandler{},
		instrumentOpts)
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
	// `log.go` should appear in the output four times, once for the error log
	// method, once for the error log location, once for the call to `next(..)`,
	// and once for the panicHandler
	count := 0
	for _, s := range outstrs {
		if strings.Contains(s, "log.go") {
			count++
		}
	}

	assert.True(t, count > 1)

	// `log_test` should appear in the output twice, once for the call in the
	// deadbeef method, and once for the ServeHttp call.
	count = 0
	for _, s := range outstrs {
		if strings.Contains(s, "log_test.go") {
			count++
		}
	}
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
	// `log.go` should appear in the output five times, once for the error log
	// method, once for the error log location, once for the call to `next(..)`,
	// once for the panicHandler, and once for the info message about failing
	// the write.
	count := 0
	for _, s := range outstrs {
		if strings.Contains(s, "log.go") {
			count++
			// This should be the last INFO message
			if count == 5 {
				assert.True(t, strings.Contains(s, "WARN"))
				assert.True(t, strings.Contains(s,
					`cannot write error for request; already written	{"foo": "bar"}`))
			}
		}
	}

	assert.True(t, count > 1)

	// `log_test` should appear in the output twice, once for the call in the
	// deadbeef method, and once for the ServeHttp call.
	count = 0
	for _, s := range outstrs {
		if strings.Contains(s, "log_test.go") {
			count++
		}
	}

	assert.Equal(t, 2, count)
}

func TestPanicErrorResponderOnlyIfNotWrittenRequest(t *testing.T) {
	stdout, stderr, req, instrumentOpts, cleanup := setup(t, true)
	defer cleanup()

	writer := &httpWriter{written: make([]string, 0, 10)}
	deadbeef := WithPanicErrorResponder(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("foo"))
			panic("err")
		}),
		instrumentOpts)
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
	deadbeef := WithPanicErrorResponder(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(404)
			panic("err")
		}),
		instrumentOpts)
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

	slowHandler := withResponseTimeLogging(delayHandler{delay: time.Second},
		instrumentOpts)
	fastHandler := withResponseTimeLogging(delayHandler{delay: time.Duration(0)},
		instrumentOpts)

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
	assert.True(t, strings.Contains(out, `response": "1.`))
}

func TestWithResponseTimeAndPanicErrorLoggingFunc(t *testing.T) {
	stdout, stderr, req, instrumentOpts, cleanup := setup(t, true)
	defer cleanup()

	writer := &httpWriter{written: make([]string, 0, 10)}
	slowPanic := WithResponseTimeAndPanicErrorLoggingFunc(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(time.Second)
			panic("err")
		}),
		instrumentOpts)

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
		if strings.Contains(s, "log.go") {
			count++
		}
	}

	assert.True(t, count > 1)

	// `log_test` should appear in the output twice, once for the call in the
	// deadbeef method, and once for the ServeHttp call.
	count = 0
	for _, s := range outstrs {
		if strings.Contains(s, "log_test.go") {
			count++
		}
	}

	// assert that the second last line of the log captures response time.
	last := outstrs[len(outstrs)-2]

	assert.True(t, strings.Contains(last, "INFO"))
	assert.True(t, strings.Contains(last, "finished handling request"))
	assert.True(t, strings.Contains(last, `"url": "cool"`))
	assert.True(t, strings.Contains(last, `response": "1.`))
}
