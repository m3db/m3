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

package remote

import (
	"context"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// TagValuesURL is the url for tag values.
	TagValuesURL = handler.RoutePrefixV1 +
		"/label/{" + prometheus.NameReplace + "}/values"

	// TagValuesHTTPMethod is the HTTP method used with this resource.
	TagValuesHTTPMethod = http.MethodGet
)

// TagValuesHandler represents a handler for search tags endpoint.
type TagValuesHandler struct {
	storage storage.Storage
}

// TagValuesResponse is the response that gets returned to the user
type TagValuesResponse struct {
	Results storage.CompleteTagsResult `json:"results,omitempty"`
}

// NewTagValuesHandler returns a new instance of handler.
func NewTagValuesHandler(
	storage storage.Storage,
) http.Handler {
	return &TagValuesHandler{
		storage: storage,
	}
}

func (h *TagValuesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx)
	w.Header().Set("Content-Type", "application/json")

	query, err := prometheus.ParseTagValuesToQuery(r)
	if err != nil {
		logger.Error("unable to parse tag values to query", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	opts := storage.NewFetchOptions()
	result, err := h.storage.CompleteTags(ctx, query, opts)
	if err != nil {
		logger.Error("unable to get tag values", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	// TODO: Support multiple result types
	err = prometheus.RenderTagValuesResultsJSON(w, result)
	if err != nil {
		logger.Error("unable to render tag values", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
	}
}
