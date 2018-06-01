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

package native

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/m3db/m3db/src/coordinator/executor"
	"github.com/m3db/m3db/src/coordinator/api/v1/handler/prometheus"
	"github.com/m3db/m3db/src/coordinator/test/local"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	promQuery = `http_requests_total{job="prometheus",group="canary"}`
	target    = "?target="
)

func TestPromReadParsing(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	storage, _ := local.NewStorageAndSession(ctrl)
	promRead := &PromReadHandler{engine: executor.NewEngine(storage)}

	req, _ := http.NewRequest("GET", createURL().String(), nil)

	r, err := promRead.parseRequest(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, promQuery, r)
}

func TestPromReadNotImplemented(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	storage, _ := local.NewStorageAndSession(ctrl)
	promRead := &PromReadHandler{engine: executor.NewEngine(storage)}
	req, _ := http.NewRequest("GET", createURL().String(), nil)

	r, parseErr := promRead.parseRequest(req)
	require.Nil(t, parseErr, "unable to parse request")
	_, err := promRead.read(context.TODO(), httptest.NewRecorder(), r, &prometheus.RequestParams{Timeout: time.Hour})
	require.NotNil(t, err, "{\"error\":\"not implemented\"}\n")
}

// NB(braskin): will replace this test once the server actually returns something
func TestPromReadEndpoint(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	// No calls expected on session object
	req, _ := http.NewRequest("GET", createURL().String(), nil)
	res := httptest.NewRecorder()
	storage, _ := local.NewStorageAndSession(ctrl)
	engine := executor.NewEngine(storage)
	promRead := &PromReadHandler{engine: engine}

	promRead.ServeHTTP(res, req)
	require.Equal(t, "{\"error\":\"not implemented\"}\n", res.Body.String())
}

func createURL() *bytes.Buffer {
	var buffer bytes.Buffer

	buffer.WriteString(PromReadURL)
	buffer.WriteString(target)
	buffer.WriteString(url.QueryEscape(promQuery))

	return &buffer
}
