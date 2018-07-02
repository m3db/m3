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

	"github.com/m3db/m3db/src/cmd/services/m3coordinator/config"
	dbconfig "github.com/m3db/m3db/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3db/src/coordinator/api/v1/handler/prometheus/native"
	"github.com/m3db/m3db/src/coordinator/api/v1/handler/prometheus/remote"
	"github.com/m3db/m3db/src/coordinator/executor"
	"github.com/m3db/m3db/src/coordinator/test/local"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestPromRemoteReadGet(t *testing.T) {
	logging.InitWithCores(nil)

	req, _ := http.NewRequest("GET", remote.PromReadURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := local.NewStorageAndSession(ctrl)

	h, err := NewHandler(storage, executor.NewEngine(storage), nil, config.Configuration{}, dbconfig.DBConfiguration{}, tally.NewTestScope("", nil))
	require.NoError(t, err, "unable to setup handler")
	err = h.RegisterRoutes()
	require.NoError(t, err, "unable to register routes")
	h.Router.ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusMethodNotAllowed, "GET method not defined")
}

func TestPromRemoteReadPost(t *testing.T) {
	logging.InitWithCores(nil)

	req, _ := http.NewRequest("POST", remote.PromReadURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := local.NewStorageAndSession(ctrl)

	h, err := NewHandler(storage, executor.NewEngine(storage), nil, config.Configuration{}, dbconfig.DBConfiguration{}, tally.NewTestScope("", nil))
	require.NoError(t, err, "unable to setup handler")
	err = h.RegisterRoutes()
	require.NoError(t, err, "unable to register routes")
	h.Router.ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusBadRequest, "Empty request")
}

func TestPromNativeReadGet(t *testing.T) {
	logging.InitWithCores(nil)

	req, _ := http.NewRequest("GET", native.PromReadURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := local.NewStorageAndSession(ctrl)

	h, err := NewHandler(storage, executor.NewEngine(storage), nil, config.Configuration{}, dbconfig.DBConfiguration{}, tally.NewTestScope("", nil))
	require.NoError(t, err, "unable to setup handler")
	h.RegisterRoutes()
	h.Router.ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusBadRequest, "Empty request")
}

func TestPromNativeReadPost(t *testing.T) {
	logging.InitWithCores(nil)

	req, _ := http.NewRequest("POST", native.PromReadURL, nil)
	res := httptest.NewRecorder()
	ctrl := gomock.NewController(t)
	storage, _ := local.NewStorageAndSession(ctrl)

	h, err := NewHandler(storage, executor.NewEngine(storage), nil, config.Configuration{}, dbconfig.DBConfiguration{}, tally.NewTestScope("", nil))
	require.NoError(t, err, "unable to setup handler")
	h.RegisterRoutes()
	h.Router.ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusMethodNotAllowed, "POST method not defined")
}
