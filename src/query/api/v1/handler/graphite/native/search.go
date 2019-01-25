// Copyright (c) 2019 Uber Technologies, Inc.
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

package native

import (
	"bytes"
	"context"
	"net/http"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest/carbon"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// GraphiteSearchURL is the url for searching graphite paths.
	GraphiteSearchURL = handler.RoutePrefixGraphiteV1 + "/search"

	// GraphiteSearchHTTPMethod is the HTTP method used with this resource.
	GraphiteSearchHTTPMethod = http.MethodGet
)

type grahiteSearchHandler struct {
	storage storage.Storage
}

// NewGraphiteSearchHandler returns a new instance of handler.
func NewGraphiteSearchHandler(
	storage storage.Storage,
) http.Handler {
	return &grahiteSearchHandler{
		storage: storage,
	}
}

func (h *grahiteSearchHandler) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx)
	w.Header().Set("Content-Type", "application/json")
	query, rErr := parseSearchParamsToQuery(r)
	if rErr != nil {
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	opts := storage.NewFetchOptions()
	result, err := h.storage.FetchTags(ctx, query, opts)
	if err != nil {
		logger.Error("unable to complete tags", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	partCount := graphite.CountMetricParts(query.Raw)
	partName := ingestcarbon.GetOrGenerateKeyName(partCount - 1)
	seenMap := make(map[string]bool, len(result.Metrics))
	for _, m := range result.Metrics {
		tags := m.Tags.Tags
		index := 0
		// TODO: make this more performant by computing the index for the tag name.
		for i, tag := range tags {
			if bytes.Equal(partName, tag.Name) {
				index = i
				break
			}
		}

		value := tags[index].Value
		// If this value has already been encountered, check if
		if hadExtra, seen := seenMap[string(value)]; seen {
			if hadExtra {
				continue
			}
		}

		hasExtraParts := len(tags) > partCount
		seenMap[string(value)] = hasExtraParts
	}

	prefix := graphite.DropLastMetricPart(query.Raw)
	if len(prefix) > 0 {
		prefix += "."
	}

	// TODO: Support multiple result types
	if err = searchResultsJSON(w, prefix, seenMap); err != nil {
		logger.Error("unable to print search results", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
	}
}
