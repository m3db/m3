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
	"errors"
	"net/http"
	"sync"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/graphite/graphite"
	graphitestorage "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// FindURL is the url for finding graphite metrics.
	FindURL = route.Prefix + "/graphite/metrics/find"
)

// FindHTTPMethods are the HTTP methods for this handler.
var FindHTTPMethods = []string{http.MethodGet, http.MethodPost}

type grahiteFindHandler struct {
	storage             graphitestorage.Storage
	graphiteStorageOpts graphitestorage.M3WrappedStorageOptions
	fetchOptionsBuilder handleroptions.FetchOptionsBuilder
	instrumentOpts      instrument.Options
}

// NewFindHandler returns a new instance of handler.
func NewFindHandler(opts options.HandlerOptions) http.Handler {
	wrappedStore := graphitestorage.NewM3WrappedStorage(opts.Storage(),
		opts.M3DBOptions(), opts.InstrumentOpts(), opts.GraphiteStorageOptions())
	return &grahiteFindHandler{
		storage:             wrappedStore,
		graphiteStorageOpts: opts.GraphiteStorageOptions(),
		fetchOptionsBuilder: opts.GraphiteFindFetchOptionsBuilder(),
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
	var (
		terminatedResultTags = 0
		childResultTags      = 0
	)

	// Sanity check the queries aren't complete name only queries.
	if terminatedResult != nil {
		terminatedResultTags = len(terminatedResult.CompletedTags)
		if terminatedResult.CompleteNameOnly {
			return nil, errors.New("terminated result is completing name only")
		}
	}

	if childResult != nil {
		childResultTags = len(childResult.CompletedTags)
		if childResult.CompleteNameOnly {
			return nil, errors.New("child result is completing name only")
		}
	}

	size := terminatedResultTags + childResultTags
	tagMap := make(map[string]nodeDescriptor, size)
	if terminatedResult != nil {
		for _, tag := range terminatedResult.CompletedTags {
			for _, value := range tag.Values {
				descriptor := tagMap[string(value)]
				descriptor.isLeaf = true
				tagMap[string(value)] = descriptor
			}
		}
	}

	if childResult != nil {
		for _, tag := range childResult.CompletedTags {
			for _, value := range tag.Values {
				descriptor := tagMap[string(value)]
				descriptor.hasChildren = true
				tagMap[string(value)] = descriptor
			}
		}
	}

	return tagMap, nil
}

func (h *grahiteFindHandler) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx, opts, err := h.fetchOptionsBuilder.NewFetchOptions(r.Context(), r)
	if err != nil {
		xhttp.WriteError(w, err)
		return
	}

	logger := logging.WithContext(ctx, h.instrumentOpts)
	w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeJSON)

	// NB: need to run two separate queries, one of which will match only the
	// provided matchers, and one which will match the provided matchers with at
	// least one more child node. For further information, refer to the comment
	// for parseFindParamsToQueries.
	terminatedQuery, childQuery, raw, err := parseFindParamsToQueries(r)
	if err != nil {
		xhttp.WriteError(w, err)
		return
	}

	var (
		terminatedResult *consolidators.CompleteTagsResult
		tErr             error
		childResult      *consolidators.CompleteTagsResult
		cErr             error
		wg               sync.WaitGroup
	)
	if terminatedQuery != nil {
		// Sometimes we only perform the child query, so only perform
		// terminated query if not nil.
		wg.Add(1)
		go func() {
			terminatedResult, tErr = h.storage.CompleteTags(ctx, terminatedQuery, opts)
			wg.Done()
		}()
	}
	// Always perform child query.
	wg.Add(1)
	go func() {
		childResult, cErr = h.storage.CompleteTags(ctx, childQuery, opts)
		wg.Done()
	}()

	wg.Wait()

	if err := xerrors.FirstError(tErr, cErr); err != nil {
		logger.Error("unable to find search", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	meta := childResult.Metadata
	if terminatedResult != nil {
		meta = terminatedResult.Metadata.CombineMetadata(childResult.Metadata)
	}

	// NB: merge results from both queries to specify which series have children
	seenMap, err := mergeTags(terminatedResult, childResult)
	if err != nil {
		logger.Error("unable to find merge", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	prefix := graphite.DropLastMetricPart(raw)
	if len(prefix) > 0 {
		prefix += "."
	}

	err = handleroptions.AddDBResultResponseHeaders(w, meta, opts)
	if err != nil {
		logger.Error("unable to render find header", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	// TODO: Support multiple result types
	resultOpts := findResultsOptions{
		includeBothExpandableAndLeaf: h.graphiteStorageOpts.FindResultsIncludeBothExpandableAndLeaf,
	}
	if err := findResultsJSON(w, prefix, seenMap, resultOpts); err != nil {
		logger.Error("unable to render find results", zap.Error(err))
	}
}
