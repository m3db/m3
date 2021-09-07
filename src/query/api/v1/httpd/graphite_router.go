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

	"github.com/m3db/m3/src/query/api/v1/options"
)

type renderRouter struct {
	renderHandler func(http.ResponseWriter, *http.Request)
}

// NewGraphiteRenderRouter returns a new graphite render router.
func NewGraphiteRenderRouter() options.GraphiteRenderRouter {
	return &renderRouter{}
}

func (r *renderRouter) Setup(opts options.GraphiteRenderRouterOptions) {
	r.renderHandler = opts.RenderHandler
}

func (r *renderRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.renderHandler(w, req)
}

type findRouter struct {
	findHandler func(http.ResponseWriter, *http.Request)
}

// NewGraphiteFindRouter returns a new graphite find router.
func NewGraphiteFindRouter() options.GraphiteFindRouter {
	return &findRouter{}
}

func (r *findRouter) Setup(opts options.GraphiteFindRouterOptions) {
	r.findHandler = opts.FindHandler
}

func (r *findRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.findHandler(w, req)
}
