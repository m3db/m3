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
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xtime "github.com/m3db/m3/src/x/time"

	"go.uber.org/zap"
)

const (
	// ListTagsURL is the url for listing tags.
	ListTagsURL = handler.RoutePrefixV1 + "/labels"
)

var (
	// ListTagsHTTPMethods are the HTTP methods for this handler.
	ListTagsHTTPMethods = []string{http.MethodGet, http.MethodPost}
)

// ListTagsHandler represents a handler for list tags endpoint.
type ListTagsHandler struct {
	storage             storage.Storage
	fetchOptionsBuilder handleroptions.FetchOptionsBuilder
	parseOpts           promql.ParseOptions
	instrumentOpts      instrument.Options
	tagOpts             models.TagOptions
}

// NewListTagsHandler returns a new instance of handler.
func NewListTagsHandler(opts options.HandlerOptions) http.Handler {
	return &ListTagsHandler{
		storage:             opts.Storage(),
		fetchOptionsBuilder: opts.FetchOptionsBuilder(),
		parseOpts: promql.NewParseOptions().
			SetRequireStartEndTime(opts.Config().Query.RequireLabelsEndpointStartEndTime).
			SetNowFn(opts.NowFn()),
		instrumentOpts: opts.InstrumentOpts(),
		tagOpts:        opts.TagOptions(),
	}
}

func (h *ListTagsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeJSON)

	ctx, opts, rErr := h.fetchOptionsBuilder.NewFetchOptions(r.Context(), r)
	if rErr != nil {
		xhttp.WriteError(w, rErr)
		return
	}

	start, end, err := prometheus.ParseStartAndEnd(r, h.parseOpts)
	if err != nil {
		xhttp.WriteError(w, err)
		return
	}

	tagMatchers := models.Matchers{{Type: models.MatchAll}}
	reqTagMatchers, ok, err := prometheus.ParseMatch(r, h.parseOpts, h.tagOpts)
	if err != nil {
		err = xerrors.NewInvalidParamsError(err)
		xhttp.WriteError(w, err)
		return
	}
	if ok {
		if n := len(reqTagMatchers); n != 1 {
			err = xerrors.NewInvalidParamsError(fmt.Errorf(
				"only single tag matcher allowed: actual=%d", n))
			xhttp.WriteError(w, err)
			return
		}
		tagMatchers = reqTagMatchers[0].Matchers
	}

	query := &storage.CompleteTagsQuery{
		CompleteNameOnly: true,
		TagMatchers:      tagMatchers,
		Start:            xtime.ToUnixNano(start),
		End:              xtime.ToUnixNano(end),
	}

	logger := logging.WithContext(ctx, h.instrumentOpts)

	result, err := h.storage.CompleteTags(ctx, query, opts)
	if err != nil {
		logger.Error("unable to complete tags", zap.Error(err))
		if errors.IsTimeout(err) {
			err = errors.NewErrQueryTimeout(err)
		}
		xhttp.WriteError(w, err)
		return
	}

	// First write out results to zero output to check if will limit
	// results and if so then write the header about truncation if occurred.
	var (
		noopWriter = ioutil.Discard
		renderOpts = prometheus.RenderSeriesMetadataOptions{
			ReturnedSeriesMetadataLimit: opts.ReturnedSeriesMetadataLimit,
		}
	)
	renderResult, err := prometheus.RenderListTagResultsJSON(noopWriter, result, renderOpts)
	if err != nil {
		logger.Error("unable to render list tags results", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	meta := result.Metadata
	limited := &handleroptions.ReturnedMetadataLimited{
		Results:      renderResult.Results,
		TotalResults: renderResult.TotalResults,
		Limited:      renderResult.LimitedMaxReturnedData,
	}
	if err := handleroptions.AddResponseHeaders(w, meta, opts, nil, limited); err != nil {
		logger.Error("unable to render list tags headers", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	_, err = prometheus.RenderListTagResultsJSON(w, result, renderOpts)
	if err != nil {
		logger.Error("unable to render list tags results", zap.Error(err))
	}
}
