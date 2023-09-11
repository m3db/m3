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
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

// PromSeriesMatchHTTPMethods are the HTTP methods for this handler.
var PromSeriesMatchHTTPMethods = []string{http.MethodGet, http.MethodPost}

// PromSeriesMatchHandler represents a handler for
// the prometheus series matcher endpoint.
type PromSeriesMatchHandler struct {
	storage             storage.Storage
	tagOptions          models.TagOptions
	fetchOptionsBuilder handleroptions.FetchOptionsBuilder
	instrumentOpts      instrument.Options
	parseOpts           promql.ParseOptions
}

// NewPromSeriesMatchHandler returns a new instance of handler.
// TODO: Remove series match handler, not part of Prometheus HTTP API
// and not used anywhere or documented.
func NewPromSeriesMatchHandler(opts options.HandlerOptions) http.Handler {
	return &PromSeriesMatchHandler{
		tagOptions:          opts.TagOptions(),
		storage:             opts.Storage(),
		fetchOptionsBuilder: opts.FetchOptionsBuilder(),
		instrumentOpts:      opts.InstrumentOpts(),
		parseOpts: opts.Engine().Options().ParseOptions().
			SetRequireStartEndTime(opts.Config().Query.RequireSeriesEndpointStartEndTime),
	}
}

func (h *PromSeriesMatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeJSON)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ctx, opts, rErr := h.fetchOptionsBuilder.NewFetchOptions(r.Context(), r)
	if rErr != nil {
		xhttp.WriteError(w, rErr)
		return
	}

	logger := logging.WithContext(ctx, h.instrumentOpts)

	queries, err := prometheus.ParseSeriesMatchQuery(r, h.parseOpts, h.tagOptions)
	if err != nil {
		logger.Error("unable to parse series match values to query", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	results := make([]models.Metrics, len(queries))
	meta := block.NewResultMetadata()
	for i, query := range queries {
		result, err := h.storage.SearchSeries(ctx, query, opts)
		if err != nil {
			logger.Error("unable to get matched series", zap.Error(err))
			xhttp.WriteError(w, err)
			return
		}

		results[i] = result.Metrics
		meta = meta.CombineMetadata(result.Metadata)
	}

	err = handleroptions.AddDBResultResponseHeaders(w, meta, opts)
	if err != nil {
		logger.Error("error writing database limit headers", zap.Error(err))
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
	renderResult, err := prometheus.RenderSeriesMatchResultsJSON(noopWriter, results, renderOpts)
	if err != nil {
		logger.Error("unable to render match series results", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	limited := &handleroptions.ReturnedMetadataLimited{
		Results:      renderResult.Results,
		TotalResults: renderResult.TotalResults,
		Limited:      renderResult.LimitedMaxReturnedData,
	}
	if err := handleroptions.AddReturnedLimitResponseHeaders(w, nil, limited); err != nil {
		logger.Error("unable to returned data headers", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	// TODO: Support multiple result types
	_, err = prometheus.RenderSeriesMatchResultsJSON(w, results, renderOpts)
	if err != nil {
		logger.Error("unable to render match series", zap.Error(err))
	}
}
