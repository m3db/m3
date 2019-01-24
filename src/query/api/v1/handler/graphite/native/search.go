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
	"context"
	"fmt"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
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

func (h *grahiteSearchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx)
	w.Header().Set("Content-Type", "application/json")

	query, rErr := parseSearchParamsToQuery(r)
	if rErr != nil {
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	fmt.Println("Query!")
	for i, m := range query.TagMatchers {
		fmt.Println(i, string(m.Name), m.Type, string(m.Value))
	}
	for _, f := range query.FilterNameTags {
		fmt.Println("filtering", string(f))
	}

	opts := storage.NewFetchOptions()
	result, err := h.storage.CompleteTags(ctx, query, opts)
	if err != nil {
		logger.Error("unable to complete tags", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	// TODO: Support multiple result types
	prometheus.RenderTagCompletionResultsJSON(w, result)
}
