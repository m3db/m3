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
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type httpWriter struct {
	written []string
	status  int
	header  http.Header
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

func TestPanicErrorReporter(t *testing.T) {
	stdout, err := ioutil.TempFile("", "temp-log-file-out")
	require.NoError(t, err, "Couldn't create a temporary out file for test.")
	defer os.Remove(stdout.Name())

	stderr, err := ioutil.TempFile("", "temp-log-file-err")
	require.NoError(t, err, "Couldn't create a temporary err file for test.")
	defer os.Remove(stderr.Name())

	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = []string{stdout.Name()}
	cfg.ErrorOutputPaths = []string{stderr.Name()}
	logger, err := cfg.Build()
	require.NoError(t, err)

	deadbeef := func(w http.ResponseWriter, _ *http.Request) {
		panic("beef")
	}

	writer := &httpWriter{written: make([]string, 0, 10)}
	handler := WithPanicErrorResponderFunc(deadbeef, logger)
	handler.ServeHTTP(writer, nil)

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
	assert.True(t, strings.Contains(outstrs[0], `{"stack": "beef"}`))

	// Assert that stack trace is written
	// `log.go` should appear in the output thrice, once for the error log method,
	// once for the error log location, and once for the call to `next(..)`, and once
	count := 0
	for _, s := range outstrs {
		if strings.Contains(s, "log.go") {
			count++
		}
	}

	assert.Equal(t, 3, count)

	// `log_test` should appear in the output twice, once for the call in the
	// deadbeef method, and once for the ServeHttp call.
	count = 0
	for _, s := range outstrs {
		if strings.Contains(s, "log_test.go") {
			count++
		}
	}
}
