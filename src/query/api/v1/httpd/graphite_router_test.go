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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/query/api/v1/options"
)

func TestGraphiteRenderHandler(t *testing.T) {
	called := 0
	renderHandler := func(w http.ResponseWriter, req *http.Request) {
		called++
	}

	router := NewGraphiteRenderRouter()
	router.Setup(options.GraphiteRenderRouterOptions{
		RenderHandler: renderHandler,
	})
	rr := httptest.NewRecorder()

	req, err := http.NewRequestWithContext(context.Background(), "GET", "/render?target=sum(metric)", nil)
	require.NoError(t, err)
	router.ServeHTTP(rr, req)
	assert.Equal(t, 1, called)
}

func TestGraphiteFindHandler(t *testing.T) {
	called := 0
	findHandler := func(w http.ResponseWriter, req *http.Request) {
		called++
	}

	router := NewGraphiteFindRouter()
	router.Setup(options.GraphiteFindRouterOptions{
		FindHandler: findHandler,
	})
	rr := httptest.NewRecorder()

	req, err := http.NewRequestWithContext(context.Background(), "GET", "/find?target=sum(metric)", nil)
	require.NoError(t, err)
	router.ServeHTTP(rr, req)
	assert.Equal(t, 1, called)
}
