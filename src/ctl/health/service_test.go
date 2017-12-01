// Copyright (c) 2017 Uber Technologies, Inc.
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
// THE SOFTWARE

package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gorilla/mux"
	"github.com/m3db/m3x/instrument"
	"github.com/stretchr/testify/require"
)

func TestHostName(t *testing.T) {
	expectedName, err := os.Hostname()
	require.NoError(t, err, "Failed to get system hostname")
	actualName := hostName()

	require.Equal(t, expectedName, actualName)
}

func TestHealthCheck(t *testing.T) {
	rr := httptest.NewRecorder()
	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.
	req, err := http.NewRequest("GET", "/health", nil)
	require.NoError(t, err)

	opts := instrument.NewOptions()
	service := NewService(opts)
	mux := mux.NewRouter().PathPrefix(service.URLPrefix()).Subrouter()
	service.RegisterHandlers(mux)

	mux.ServeHTTP(rr, req)

	rawResult := make([]byte, rr.Body.Len())
	_, err = rr.Body.Read(rawResult)
	require.NoError(t, err, "Encountered error parsing response")

	var actualResult healthCheckResult
	json.Unmarshal(rawResult, &actualResult)

	name, _ := os.Hostname()

	require.Equal(t, name, actualResult.Host)
	require.Equal(t, ok, actualResult.Status)
}
