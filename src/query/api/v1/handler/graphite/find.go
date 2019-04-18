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
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"
	xerrors "github.com/m3db/m3/src/x/errors"

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

func mergeTags(
	terminatedResult *storage.CompleteTagsResult,
	childResult *storage.CompleteTagsResult,
) (map[string]bool, error) {
	// sanity check the case.
	if terminatedResult.CompleteNameOnly {
		return nil, errors.New("terminated result is completing name only")
	}

	if childResult.CompleteNameOnly {
		return nil, errors.New("child result is completing name only")
	}

	mapLength := len(terminatedResult.CompletedTags) + len(childResult.CompletedTags)
	tagMap := make(map[string]bool, mapLength)

	for _, tag := range terminatedResult.CompletedTags {
		for _, value := range tag.Values {
			tagMap[string(value)] = false
		}
	}

	// NB: fine to overwrite any tags which were present in the `terminatedResult` map
	// since if they appear in `childResult`, then they exist AND have children.
	for _, tag := range childResult.CompletedTags {
		for _, value := range tag.Values {
			tagMap[string(value)] = true
		}
	}

	return tagMap, nil
}

func (h *grahiteFindHandler) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx)
	w.Header().Set("Content-Type", "application/json")

	// NB: need to run two separate queries, one of which will match only the
	// provided matchers, and one which will match the provided matchers with at
	// least one more child node. For further information, refer to the comment
	// for parseFindParamsToQueries
	terminatedQuery, childQuery, raw, rErr := parseFindParamsToQueries(r)
	if rErr != nil {
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	var (
		terminatedResult *storage.CompleteTagsResult
		tErr             error
		childResult      *storage.CompleteTagsResult
		cErr             error
		opts             = storage.NewFetchOptions()

		wg sync.WaitGroup
	)

	wg.Add(2)
	go func() {
		terminatedResult, tErr = h.storage.CompleteTags(ctx, terminatedQuery, opts)
		wg.Done()
	}()

	go func() {
		childResult, cErr = h.storage.CompleteTags(ctx, childQuery, opts)
		wg.Done()
	}()

	wg.Wait()
	if err := xerrors.FirstError(tErr, cErr); err != nil {
		logger.Error("unable to complete tags", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	// NB: merge results from both queries to specify which series have children
	seenMap, err := mergeTags(terminatedResult, childResult)
	if err != nil {
		logger.Error("unable to complete tags", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	prefix := graphite.DropLastMetricPart(raw)
	if len(prefix) > 0 {
		prefix += "."
	}

	// TODO: Support multiple result types
	if err = findResultsJSON(w, prefix, seenMap); err != nil {
		logger.Error("unable to print find results", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
	}
}
