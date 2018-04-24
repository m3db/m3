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

package httpd

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/executor"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/services/m3coordinator/config"
	"github.com/m3db/m3coordinator/services/m3coordinator/handler"
	"github.com/m3db/m3coordinator/storage/local"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestPromReadGet(t *testing.T) {
	logging.InitWithCores(nil)

	req, _ := http.NewRequest("GET", handler.PromReadURL, nil)
	res := httptest.NewRecorder()
	storage := local.NewStorage(nil, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))

	h, err := NewHandler(storage, executor.NewEngine(storage), nil, config.Configuration{})
	require.NoError(t, err, "unable to setup handler")
	h.RegisterRoutes()
	h.Router.ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusMethodNotAllowed, "GET method not defined")
}

func TestPromReadPost(t *testing.T) {
	logging.InitWithCores(nil)

	req, _ := http.NewRequest("POST", handler.PromReadURL, nil)
	res := httptest.NewRecorder()
	storage := local.NewStorage(nil, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))

	h, err := NewHandler(storage, executor.NewEngine(storage), nil, config.Configuration{})
	require.NoError(t, err, "unable to setup handler")
	h.RegisterRoutes()
	h.Router.ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusBadRequest, "Empty request")
}
