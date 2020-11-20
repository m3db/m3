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

const (
	// PromSeriesMatchURL is the url for remote prom series matcher handler.
	PromSeriesMatchURL = handler.RoutePrefixV1 + "/series"
)

var (
	// PromSeriesMatchHTTPMethods are the HTTP methods for this handler.
	PromSeriesMatchHTTPMethods = []string{http.MethodGet, http.MethodPost}
)

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
func NewPromSeriesMatchHandler(opts options.HandlerOptions) http.Handler {
	return &PromSeriesMatchHandler{
		tagOptions:          opts.TagOptions(),
		storage:             opts.Storage(),
		fetchOptionsBuilder: opts.FetchOptionsBuilder(),
		instrumentOpts:      opts.InstrumentOpts(),
		parseOpts:           opts.Engine().Options().ParseOptions(),
	}
}

func (h *PromSeriesMatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx, h.instrumentOpts)
	w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeJSON)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	queries, err := prometheus.ParseSeriesMatchQuery(r, h.parseOpts, h.tagOptions)
	if err != nil {
		logger.Error("unable to parse series match values to query", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	opts, rErr := h.fetchOptionsBuilder.NewFetchOptions(r)
	if rErr != nil {
		xhttp.WriteError(w, rErr)
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

	handleroptions.AddResponseHeaders(w, meta, opts)
	// TODO: Support multiple result types
	if err := prometheus.RenderSeriesMatchResultsJSON(w, results, false); err != nil {
		logger.Error("unable to write matched series", zap.Error(err))
	}
}
