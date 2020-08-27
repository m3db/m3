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
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/util/logging"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xopentracing "github.com/m3db/m3/src/x/opentracing"

	opentracingext "github.com/opentracing/opentracing-go/ext"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"
)

const (
	// PromReadURL is the url for native prom read handler, this matches the
	// default URL for the query range endpoint found on a Prometheus server.
	PromReadURL = handler.RoutePrefixV1 + "/query_range"

	// PromReadInstantURL is the url for native instantaneous prom read
	// handler, this matches the  default URL for the query endpoint
	// found on a Prometheus server.
	PromReadInstantURL = handler.RoutePrefixV1 + "/query"
)

var (
	// PromReadHTTPMethods are the HTTP methods for the read handler.
	PromReadHTTPMethods = []string{
		http.MethodGet,
		http.MethodPost,
	}

	// PromReadInstantHTTPMethods are the HTTP methods for the instant handler.
	PromReadInstantHTTPMethods = []string{
		http.MethodGet,
		http.MethodPost,
	}
)

// promReadHandler represents a handler for prometheus read endpoint.
type promReadHandler struct {
	instant         bool
	promReadMetrics promReadMetrics
	opts            options.HandlerOptions
}

// NewPromReadHandler returns a new prometheus-compatible read handler.
func NewPromReadHandler(opts options.HandlerOptions) http.Handler {
	return newHandler(opts, false)
}

// NewPromReadInstantHandler returns a new pro instance of handler.
func NewPromReadInstantHandler(opts options.HandlerOptions) http.Handler {
	return newHandler(opts, true)
}

// newHandler returns a new pro instance of handler.
func newHandler(opts options.HandlerOptions, instant bool) http.Handler {
	name := "native-read"
	if instant {
		name = "native-instant-read"
	}

	taggedScope := opts.InstrumentOpts().MetricsScope().
		Tagged(map[string]string{"handler": name})
	h := &promReadHandler{
		promReadMetrics: newPromReadMetrics(taggedScope),
		opts:            opts,
		instant:         instant,
	}

	maxDatapoints := opts.Config().Limits.MaxComputedDatapoints()
	h.promReadMetrics.maxDatapoints.Update(float64(maxDatapoints))
	return h
}

func (h *promReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	timer := h.promReadMetrics.fetchTimerSuccess.Start()
	defer timer.Stop()

	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx, h.opts.InstrumentOpts())

	parsedOptions, rErr := ParseRequest(ctx, r, h.instant, h.opts)
	if rErr != nil {
		h.promReadMetrics.fetchErrorsClient.Inc(1)
		logger.Error("could not parse request", zap.Error(rErr.Inner()))
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	watcher := handler.NewResponseWriterCanceller(w, h.opts.InstrumentOpts())
	parsedOptions.CancelWatcher = watcher

	result, err := read(ctx, parsedOptions, h.opts)
	if err != nil {
		sp := xopentracing.SpanFromContextOrNoop(ctx)
		sp.LogFields(opentracinglog.Error(err))
		opentracingext.Error.Set(sp, true)
		logger.Error("range query error",
			zap.Error(err),
			zap.Any("parsedOptions", parsedOptions))
		h.promReadMetrics.fetchErrorsServer.Inc(1)

		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeJSON)
	handleroptions.AddWarningHeaders(w, result.Meta)
	h.promReadMetrics.fetchSuccess.Inc(1)

	if h.instant {
		renderResultsInstantaneousJSON(w, result, h.opts.Config().ResultOptions.KeepNans)
		return
	}

	if parsedOptions.Params.FormatType == models.FormatM3QL {
		renderM3QLResultsJSON(w, result.Series, parsedOptions.Params)
		return
	}

	err = RenderResultsJSON(w, result, RenderResultsOptions{
		Start:    parsedOptions.Params.Start,
		End:      parsedOptions.Params.End,
		KeepNaNs: h.opts.Config().ResultOptions.KeepNans,
	})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logger.Error("failed to render results", zap.Error(err))
	} else {
		w.WriteHeader(http.StatusOK)
	}
}
