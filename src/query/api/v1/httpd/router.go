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

// Package httpd contains http routers.
package httpd

import (
	"net/http"
	"strings"

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/x/headers"
)

type router struct {
	promqlHandler      func(http.ResponseWriter, *http.Request)
	m3QueryHandler     func(http.ResponseWriter, *http.Request)
	defaultQueryEngine options.QueryEngine
}

func NewQueryRouter() options.QueryRouter {
	return &router{}
}

func (r *router) Setup(opts options.QueryRouterOptions) {
	defaultEngine := opts.DefaultQueryEngine
	if defaultEngine != options.PrometheusEngine && defaultEngine != options.M3QueryEngine {
		defaultEngine = options.PrometheusEngine
	}

	r.defaultQueryEngine = defaultEngine
	r.promqlHandler = opts.PromqlHandler
	r.m3QueryHandler = opts.M3QueryHandler
}

func (r *router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	engine := strings.ToLower(req.Header.Get(headers.EngineHeaderName))
	urlParam := req.URL.Query().Get(EngineURLParam)

	if len(urlParam) > 0 {
		engine = strings.ToLower(urlParam)
	}

	if !options.IsQueryEngineSet(engine) {
		engine = string(r.defaultQueryEngine)
	}

	w.Header().Add(headers.EngineHeaderName, engine)

	if engine == string(options.M3QueryEngine) {
		r.m3QueryHandler(w, req)
		return
	}

	r.promqlHandler(w, req)
}
