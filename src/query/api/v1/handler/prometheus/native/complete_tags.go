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

package native

import (
	"context"
	"net/http"
	"sync"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// CompleteTagsURL is the url for searching tags.
	CompleteTagsURL = handler.RoutePrefixV1 + "/search"

	// CompleteTagsHTTPMethod is the HTTP method used with this resource.
	CompleteTagsHTTPMethod = http.MethodGet
)

// CompleteTagsHandler represents a handler for search tags endpoint.
type CompleteTagsHandler struct {
	storage             storage.Storage
	fetchOptionsBuilder handleroptions.FetchOptionsBuilder
	instrumentOpts      instrument.Options
}

// NewCompleteTagsHandler returns a new instance of handler.
func NewCompleteTagsHandler(opts options.HandlerOptions) http.Handler {
	return &CompleteTagsHandler{
		storage:             opts.Storage(),
		fetchOptionsBuilder: opts.FetchOptionsBuilder(),
		instrumentOpts:      opts.InstrumentOpts(),
	}
}

func (h *CompleteTagsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx, h.instrumentOpts)
	w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeJSON)

	tagCompletionQueries, rErr := prometheus.ParseTagCompletionParamsToQueries(r)
	if rErr != nil {
		xhttp.WriteError(w, rErr)
		return
	}

	opts, rErr := h.fetchOptionsBuilder.NewFetchOptions(r)
	if rErr != nil {
		xhttp.WriteError(w, rErr)
		return
	}

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		multiErr xerrors.MultiError

		nameOnly      = tagCompletionQueries.NameOnly
		meta          = block.NewResultMetadata()
		resultBuilder = consolidators.NewCompleteTagsResultBuilder(
			nameOnly, models.NewTagOptions())
	)

	for _, query := range tagCompletionQueries.Queries {
		wg.Add(1)
		// Capture variables.
		query := query
		go func() {
			result, err := h.storage.CompleteTags(ctx, query, opts)
			mu.Lock()
			defer func() {
				mu.Unlock()
				wg.Done()
			}()

			if err != nil {
				multiErr = multiErr.Add(err)
				return
			}

			meta = meta.CombineMetadata(result.Metadata)
			err = resultBuilder.Add(result)
			if err != nil {
				multiErr = multiErr.Add(err)
			}
		}()
	}

	wg.Wait()
	if err := multiErr.FinalError(); err != nil {
		logger.Error("unable to complete tags", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	handleroptions.AddResponseHeaders(w, meta, opts)
	result := resultBuilder.Build()
	if err := prometheus.RenderTagCompletionResultsJSON(w, result); err != nil {
		logger.Error("unable to render results", zap.Error(err))
	}
}
