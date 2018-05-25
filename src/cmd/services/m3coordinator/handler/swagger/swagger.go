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

package swagger

import (
	"html/template"
	"net/http"

	"github.com/m3db/m3db/src/cmd/services/m3coordinator/handler"
	"github.com/m3db/m3db/src/coordinator/util/logging"
	"go.uber.org/zap"
)

const (
	// URL is the url for the swagger handler.
	URL = handler.RoutePrefix + "/swagger"

	// HTTPMethod is the HTTP method used with this resource.
	HTTPMethod = "GET"

	swaggerSpecDir = "swagger/spec/"

	swaggerTitle = "M3DB Documentation"
)

var (
	// SpecURLPrefix is the url prefix for swagger specs.
	SpecURLPrefix = URL + "/spec/"

	swaggerSpec = SpecURLPrefix + "coordinator.yml"
)

// TemplateHandler handles serving the swagger template.
type TemplateHandler struct{}

// Doc is a Swagger doc.
type Doc struct {
	Title string
	Spec  string
}

// ServeHTTP serves the swagger template.
func (*TemplateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	t, err := template.ParseFiles("swagger/template.html")
	if err != nil {
		logger.Error("unable generate Swagger docs", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	doc := &Doc{
		Title: swaggerTitle,
		Spec:  swaggerSpec,
	}

	t.Execute(w, doc)
}

// SpecHandler is the handler for serving swagger specs.
func SpecHandler() http.Handler {
	return http.StripPrefix(SpecURLPrefix, http.FileServer(http.Dir(swaggerSpecDir)))
}
