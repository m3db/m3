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
	"html/template"
	"net/http"

	"github.com/m3db/m3db/src/cmd/services/m3coordinator/handler"
	"github.com/m3db/m3db/src/coordinator/util/logging"
	"go.uber.org/zap"
)

const (
	// URL is the url for the OpenAPI handler.
	URL = handler.RoutePrefix + "/docs"

	// HTTPMethod is the HTTP method used with this resource.
	HTTPMethod = "GET"

	// Dir is the openapi directory.
	Dir       = "openapi/"
	staticDir = Dir + "/static/"

	docTitle = "M3DB Documentation"
)

var (
	// StaticURLPrefix is the url prefix for openapi specs.
	StaticURLPrefix = URL + "/static/"
)

// TemplateHandler handles serving the OpenAPI template.
type TemplateHandler struct {
	template string
	spec     string
	title    string
}

// NewTemplateHandler returns a new instance of a namespace add handler.
func NewTemplateHandler(template, spec, title string) http.Handler {
	return &TemplateHandler{
		template: template,
		spec:     spec,
		title:    title,
	}
}

// doc is a OpenAPI doc.
type doc struct {
	Title   string
	Spec    string
	RedocJS string
}

// ServeHTTP serves the OpenAPI template. This template is what renders the OpenAPI
// doc. It dynamically loads a spec (served by the StaticHandler below), which contains
// all the metadata about endpoints.
func (h *TemplateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	t, err := template.ParseFiles(h.template)
	if err != nil {
		logger.Error("unable generate OpenAPI docs", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	doc := &doc{
		Title:   h.title,
		Spec:    StaticURLPrefix + h.spec,
		RedocJS: StaticURLPrefix + "redoc.js",
	}

	err = t.Execute(w, doc)
	if err != nil {
		logger.Error("unable generate OpenAPI docs", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}
}

// StaticHandler is the handler for serving static assets (including OpenAPI specs).
func StaticHandler() http.Handler {
	return http.StripPrefix(StaticURLPrefix, http.FileServer(http.Dir(staticDir)))
}
