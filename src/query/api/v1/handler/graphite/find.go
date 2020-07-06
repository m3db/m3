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
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// FindURL is the url for finding graphite metrics.
	FindURL = handler.RoutePrefixV1 + "/graphite/metrics/find"
)

var (
	// FindHTTPMethods are the HTTP methods for this handler.
	FindHTTPMethods = []string{http.MethodGet, http.MethodPost}
)

type grahiteFindHandler struct {
	storage             storage.Storage
	fetchOptionsBuilder handleroptions.FetchOptionsBuilder
	instrumentOpts      instrument.Options
}

// NewFindHandler returns a new instance of handler.
func NewFindHandler(opts options.HandlerOptions) http.Handler {
	return &grahiteFindHandler{
		storage:             opts.Storage(),
		fetchOptionsBuilder: opts.FetchOptionsBuilder(),
		instrumentOpts:      opts.InstrumentOpts(),
	}
}

type nodeDescriptor struct {
	isLeaf      bool
	hasChildren bool
}

func mergeTags(
	terminatedResult *consolidators.CompleteTagsResult,
	childResult *consolidators.CompleteTagsResult,
) (map[string]nodeDescriptor, error) {
	// sanity check the case.
	if terminatedResult.CompleteNameOnly {
		return nil, errors.New("terminated result is completing name only")
	}

	if childResult.CompleteNameOnly {
		return nil, errors.New("child result is completing name only")
	}

	mapLength := len(terminatedResult.CompletedTags) + len(childResult.CompletedTags)
	tagMap := make(map[string]nodeDescriptor, mapLength)

	for _, tag := range terminatedResult.CompletedTags {
		for _, value := range tag.Values {
			descriptor := tagMap[string(value)]
			descriptor.isLeaf = true
			tagMap[string(value)] = descriptor
		}
	}

	for _, tag := range childResult.CompletedTags {
		for _, value := range tag.Values {
			descriptor := tagMap[string(value)]
			descriptor.hasChildren = true
			tagMap[string(value)] = descriptor
		}
	}

	return tagMap, nil
}

func (h *grahiteFindHandler) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx, h.instrumentOpts)
	w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeJSON)

	// NB: need to run two separate queries, one of which will match only the
	// provided matchers, and one which will match the provided matchers with at
	// least one more child node. For further information, refer to the comment
	// for parseFindParamsToQueries
	terminatedQuery, childQuery, raw, rErr := parseFindParamsToQueries(r)
	if rErr != nil {
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	opts, rErr := h.fetchOptionsBuilder.NewFetchOptions(r)
	if rErr != nil {
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	var (
		terminatedResult *consolidators.CompleteTagsResult
		tErr             error
		childResult      *consolidators.CompleteTagsResult
		cErr             error
		wg               sync.WaitGroup
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

	meta := terminatedResult.Metadata.CombineMetadata(childResult.Metadata)
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

	handleroptions.AddWarningHeaders(w, meta)
	// TODO: Support multiple result types
	if err = findResultsJSON(w, prefix, seenMap); err != nil {
		logger.Error("unable to print find results", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
	}
}
