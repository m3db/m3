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
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/route"
	assets "github.com/m3db/m3/src/query/generated/assets/openapi"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// URL is the url for the OpenAPI handler.
	URL = route.Prefix + "/openapi"

	// HTTPMethod is the HTTP method used with this resource.
	HTTPMethod = http.MethodGet

	docPath = "/index.html"
)

var (
	// StaticURLPrefix is the url prefix for openapi specs.
	StaticURLPrefix = URL + "/static/"
)

// DocHandler handles serving the OpenAPI doc.
type DocHandler struct {
	instrumentOpts instrument.Options
}

// NewDocHandler returns a new doc handler.
func NewDocHandler(
	instrumentOpts instrument.Options,
) http.Handler {
	return &DocHandler{
		instrumentOpts: instrumentOpts,
	}
}

// ServeHTTP serves the OpenAPI doc.
func (h *DocHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx, h.instrumentOpts)

	doc, err := assets.FSByte(false, docPath)

	if err != nil {
		logger.Error("unable to load doc", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeHTMLUTF8)
	w.Write(doc)
}

// StaticHandler is the handler for serving static assets (including OpenAPI specs).
func StaticHandler() http.Handler {
	return http.StripPrefix(StaticURLPrefix, http.FileServer(assets.FS(false)))
}
