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
	"github.com/m3db/m3/src/query/util/logging"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xopentracing "github.com/m3db/m3/src/x/opentracing"

	opentracingext "github.com/opentracing/opentracing-go/ext"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"
)

const (
	// PromReadURL is the URL for native prom read handler, this matches the
	// default URL for the query range endpoint found on a Prometheus server.
	PromReadURL = handler.RoutePrefixV1 + "/query_range"

	// PromReadInstantURL is the URL for native instantaneous prom read
	// handler, this matches the  default URL for the query endpoint
	// found on a Prometheus server.
	PromReadInstantURL = handler.RoutePrefixV1 + "/query"

	// PrometheusReadURL is the URL for native prom read handler.
	PrometheusReadURL = "/prometheus" + PromReadURL

	// PrometheusReadInstantURL is the URL for native instantaneous prom read handler.
	PrometheusReadInstantURL = "/prometheus" + PromReadInstantURL

	// M3QueryReadURL is the URL for native m3 query read handler.
	M3QueryReadURL = "/m3query" + PromReadURL

	// M3QueryReadInstantURL is the URL for native instantaneous m3 query read handler.
	M3QueryReadInstantURL = "/m3query" + PromReadInstantURL
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
	return h
}

func (h *promReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	timer := h.promReadMetrics.fetchTimerSuccess.Start()
	defer timer.Stop()

	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	iOpts := h.opts.InstrumentOpts()
	logger := logging.WithContext(ctx, iOpts)

	parsedOptions, rErr := ParseRequest(ctx, r, h.instant, h.opts)
	if rErr != nil {
		h.promReadMetrics.incError(rErr)
		logger.Error("could not parse request", zap.Error(rErr))
		xhttp.WriteError(w, rErr)
		return
	}
	ctx = logging.NewContext(ctx,
		iOpts,
		zap.String("query", parsedOptions.Params.Query),
		zap.Time("start", parsedOptions.Params.Start),
		zap.Time("end", parsedOptions.Params.End),
		zap.Duration("step", parsedOptions.Params.Step),
		zap.Duration("timeout", parsedOptions.Params.Timeout),
		zap.Duration("fetchTimeout", parsedOptions.FetchOpts.Timeout),
	)

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
		h.promReadMetrics.incError(err)

		xhttp.WriteError(w, err)
		return
	}

	w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeJSON)
	handleroptions.AddResponseHeaders(w, result.Meta, parsedOptions.FetchOpts)
	h.promReadMetrics.fetchSuccess.Inc(1)

	keepNaNs := h.opts.Config().ResultOptions.KeepNaNs
	if !keepNaNs {
		keepNaNs = result.Meta.KeepNaNs
	}

	if h.instant {
		renderResultsInstantaneousJSON(w, result, keepNaNs)
		return
	}

	err = RenderResultsJSON(w, result, RenderResultsOptions{
		Start:    parsedOptions.Params.Start,
		End:      parsedOptions.Params.End,
		KeepNaNs: h.opts.Config().ResultOptions.KeepNaNs,
	})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logger.Error("failed to render results", zap.Error(err))
	} else {
		w.WriteHeader(http.StatusOK)
	}
}
