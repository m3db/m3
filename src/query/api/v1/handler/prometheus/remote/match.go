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
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// PromSeriesMatchURL is the url for remote prom series matcher handler.
	PromSeriesMatchURL = handler.RoutePrefixV1 + "/series"

	// PromSeriesMatchHTTPMethod is the HTTP method used with this resource.
	PromSeriesMatchHTTPMethod = http.MethodGet
)

// PromSeriesMatchHandler represents a handler for prometheus series matcher endpoint.
type PromSeriesMatchHandler struct {
	tagOptions models.TagOptions
	storage    storage.Storage
}

// NewPromSeriesMatchHandler returns a new instance of handler.
func NewPromSeriesMatchHandler(
	storage storage.Storage,
	tagOptions models.TagOptions,
) http.Handler {
	return &PromSeriesMatchHandler{
		tagOptions: tagOptions,
		storage:    storage,
	}
}

func (h *PromSeriesMatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	query, err := prometheus.ParseSeriesMatchQuery(r, h.tagOptions)
	if err != nil {
		logger.Error("unable to parse series match values to query", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	opts := storage.NewFetchOptions()
	matchers := query.TagMatchers
	results := make([]*storage.CompleteTagsResult, len(matchers))
	// TODO: parallel execution
	for i, matcher := range matchers {
		completeTagsQuery := &storage.CompleteTagsQuery{
			CompleteNameOnly: false,
			TagMatchers:      matcher,
		}

		result, err := h.storage.CompleteTags(ctx, completeTagsQuery, opts)
		if err != nil {
			logger.Error("unable to get matched series", zap.Error(err))
			xhttp.Error(w, err, http.StatusBadRequest)
			return
		}

		results[i] = result
	}

	// TODO: Support multiple result types
	if renderErr := prometheus.RenderSeriesMatchResultsJSON(w, results); renderErr != nil {
		logger.Error("unable to write matched series", zap.Error(renderErr))
		xhttp.Error(w, renderErr, http.StatusBadRequest)
		return
	}
}
