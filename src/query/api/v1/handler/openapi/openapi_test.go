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

package openapi

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocHandler(t *testing.T) {
	w := httptest.NewRecorder()

	req := httptest.NewRequest("GET", "/api/v1/docs", nil)
	require.NotNil(t, req)

	docHandler := DocHandler{}
	require.NotNil(t, docHandler)
	docHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "<!DOCTYPE html>\n<!--\nNOTE: Run `make asset-gen-coordinator` if you make any changes to this file!\n-->\n<html>\n  <head>\n    <title>M3DB ReDoc</title>\n    <meta charset=\"utf-8\"/>\n    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n    <link href=\"https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700\" rel=\"stylesheet\">\n    <style>\n      body {\n        margin: 0;\n        padding: 0;\n      }\n    </style>\n  </head>\n  <body>\n    <redoc spec-url='openapi/static/spec.yml'></redoc>\n    <script src=\"https://cdn.jsdelivr.net/npm/redoc@latest/bundles/redoc.standalone.js\"> </script>\n  </body>\n</html>\n", string(body))
}

func TestStaticHandler(t *testing.T) {
	assert.NotNil(t, StaticHandler())
}
