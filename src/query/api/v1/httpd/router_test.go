// Copyright (c) 2020 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/x/headers"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHanlerSwitch(t *testing.T) {
	promqlCalled := 0
	promqlHandler := func(w http.ResponseWriter, req *http.Request) {
		promqlCalled++
	}

	m3qCalled := 0
	m3qHandler := func(w http.ResponseWriter, req *http.Request) {
		m3qCalled++
	}

	router := NewQueryRouter()
	router.Setup(options.QueryRouterOptions{
		DefaultQueryEngine: "prometheus",
		PromqlHandler:      promqlHandler,
		M3QueryHandler:     m3qHandler,
	})
	rr := httptest.NewRecorder()

	req, err := http.NewRequest("GET", "/query?query=sum(metric)", nil)
	require.NoError(t, err)
	router.ServeHTTP(rr, req)
	assert.Equal(t, 1, promqlCalled)
	assert.Equal(t, 0, m3qCalled)

	req, err = http.NewRequest("GET", "/query?query=sum(metric)&engine=m3query", nil)
	require.NoError(t, err)
	router.ServeHTTP(rr, req)
	assert.Equal(t, 1, promqlCalled)
	assert.Equal(t, 1, m3qCalled)

	req, err = http.NewRequest("GET", "/query?query=sum(metric)", nil)
	req.Header.Add(headers.EngineHeaderName, "m3query")
	require.NoError(t, err)
	router.ServeHTTP(rr, req)
	assert.Equal(t, 1, promqlCalled)
	assert.Equal(t, 2, m3qCalled)

	req, err = http.NewRequest("GET", "/query?query=sum(metric)", nil)
	req.Header.Add(headers.EngineHeaderName, "M3QUERY")
	require.NoError(t, err)
	router.ServeHTTP(rr, req)
	assert.Equal(t, 1, promqlCalled)
	assert.Equal(t, 3, m3qCalled)

	req, err = http.NewRequest("GET", "/query?query=sum(metric)", nil)
	req.Header.Add(headers.EngineHeaderName, "prometheus")
	require.NoError(t, err)
	router.ServeHTTP(rr, req)
	assert.Equal(t, 2, promqlCalled)
	assert.Equal(t, 3, m3qCalled)
}
