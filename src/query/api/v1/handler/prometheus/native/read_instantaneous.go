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

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// PromReadInstantURL is the url for native instantaneous prom read
	// handler, this matches the  default URL for the query endpoint
	// found on a Prometheus server
	PromReadInstantURL = handler.RoutePrefixV1 + "/query"
)

var (
	// PromReadInstantHTTPMethods are the HTTP methods for this handler.
	PromReadInstantHTTPMethods = []string{
		http.MethodGet,
		http.MethodPost,
	}
)

// PromReadInstantHandler represents a handler for prometheus instantaneous read endpoint.
type PromReadInstantHandler struct {
	engine              executor.Engine
	fetchOptionsBuilder handleroptions.FetchOptionsBuilder
	tagOpts             models.TagOptions
	timeoutOpts         *prometheus.TimeoutOpts
	instrumentOpts      instrument.Options
}

// NewPromReadInstantHandler returns a new instance of handler.
func NewPromReadInstantHandler(
	opts options.HandlerOptions) *PromReadInstantHandler {
	return &PromReadInstantHandler{
		engine:              opts.Engine(),
		fetchOptionsBuilder: opts.FetchOptionsBuilder(),
		tagOpts:             opts.TagOptions(),
		timeoutOpts:         opts.TimeoutOpts(),
		instrumentOpts:      opts.InstrumentOpts(),
	}
}

func (h *PromReadInstantHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx, h.instrumentOpts)

	fetchOpts, rErr := h.fetchOptionsBuilder.NewFetchOptions(r)
	if rErr != nil {
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	params, rErr := parseInstantaneousParams(r, h.engine.Options(),
		h.timeoutOpts, fetchOpts, h.instrumentOpts)
	if rErr != nil {
		xhttp.Error(w, rErr, rErr.Code())
		return
	}

	if params.Debug {
		logger.Info("request params", zap.Any("params", params))
	}

	queryOpts := &executor.QueryOptions{
		QueryContextOptions: models.QueryContextOptions{
			LimitMaxTimeseries: fetchOpts.Limit,
		}}

	restrictOpts := fetchOpts.RestrictQueryOptions.GetRestrictByType()
	if restrictOpts != nil {
		restrict := &models.RestrictFetchTypeQueryContextOptions{
			MetricsType:   uint(restrictOpts.MetricsType),
			StoragePolicy: restrictOpts.StoragePolicy,
		}
		queryOpts.QueryContextOptions.RestrictFetchType = restrict
	}

	result, err := read(ctx, h.engine, queryOpts, fetchOpts,
		h.tagOpts, w, params, h.instrumentOpts)
	if err != nil {
		logger.Error("unable to fetch data", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	// TODO: Support multiple result types
	w.Header().Set("Content-Type", "application/json")
	handleroptions.AddWarningHeaders(w, result.meta)
	renderResultsInstantaneousJSON(w, result.series)
}
