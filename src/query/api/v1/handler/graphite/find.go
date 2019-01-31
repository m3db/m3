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

package graphite

import (
	"bytes"
	"context"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// FindURL is the url for finding graphite metrics.
	FindURL = handler.RoutePrefixV1 + "/graphite/metrics/find"
)

var (
	// FindHTTPMethods is the HTTP methods used with this resource.
	FindHTTPMethods = []string{http.MethodGet, http.MethodPost}
)

type grahiteFindHandler struct {
	storage storage.Storage
}

// NewFindHandler returns a new instance of handler.
func NewFindHandler(
	storage storage.Storage,
) http.Handler {
	return &grahiteFindHandler{
		storage: storage,
	}
}

func (h *grahiteFindHandler) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx)
	w.Header().Set("Content-Type", "application/json")
	query, rErr := parseFindParamsToQuery(r)
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
	partName := graphite.TagName(partCount - 1)
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
		if hadExtra, seen := seenMap[string(value)]; seen && hadExtra {
			continue
		}

		hasExtraParts := len(tags) > partCount
		seenMap[string(value)] = hasExtraParts
	}

	prefix := graphite.DropLastMetricPart(query.Raw)
	if len(prefix) > 0 {
		prefix += "."
	}

	// TODO: Support multiple result types
	if err = findResultsJSON(w, prefix, seenMap); err != nil {
		logger.Error("unable to print find results", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
	}
}
