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
	"fmt"
	"net/http"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xopentracing "github.com/m3db/m3/src/x/opentracing"

	opentracingext "github.com/opentracing/opentracing-go/ext"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	// PromReadURL is the url for native prom read handler, this matches the
	// default URL for the query range endpoint found on a Prometheus server
	PromReadURL = handler.RoutePrefixV1 + "/query_range"

	// TODO: Move to config
	initialBlockAlloc = 10
)

var (
	// PromReadHTTPMethods are the HTTP methods for this handler.
	PromReadHTTPMethods = []string{
		http.MethodGet,
		http.MethodPost,
	}

	emptySeriesList = []*ts.Series{}
	emptyReqParams  = models.RequestParams{}
)

// PromReadHandler represents a handler for prometheus read endpoint.
type PromReadHandler struct {
	limitsCfg           *config.LimitsConfiguration
	fetchOptionsBuilder handleroptions.FetchOptionsBuilder
	promReadMetrics     promReadMetrics
	opts                options.HandlerOptions
}

type promReadMetrics struct {
	fetchSuccess      tally.Counter
	fetchErrorsServer tally.Counter
	fetchErrorsClient tally.Counter
	fetchTimerSuccess tally.Timer
	maxDatapoints     tally.Gauge
}

func newPromReadMetrics(scope tally.Scope) promReadMetrics {
	return promReadMetrics{
		fetchSuccess: scope.Counter("fetch.success"),
		fetchErrorsServer: scope.Tagged(map[string]string{"code": "5XX"}).
			Counter("fetch.errors"),
		fetchErrorsClient: scope.Tagged(map[string]string{"code": "4XX"}).
			Counter("fetch.errors"),
		fetchTimerSuccess: scope.Timer("fetch.success.latency"),
		maxDatapoints:     scope.Gauge("max_datapoints"),
	}
}

// ReadResponse is the response that gets returned to the user
type ReadResponse struct {
	Results []ts.Series `json:"results,omitempty"`
}

type blockWithMeta struct {
	block block.Block
	meta  block.Metadata
}

// RespError wraps error and status code
type RespError struct {
	Err  error
	Code int
}

// NewPromReadHandler returns a new instance of handler.
func NewPromReadHandler(opts options.HandlerOptions) *PromReadHandler {
	taggedScope := opts.InstrumentOpts().MetricsScope().
		Tagged(map[string]string{"handler": "native-read"})
	limits := opts.Config().Limits

	h := &PromReadHandler{
		limitsCfg:       &limits,
		promReadMetrics: newPromReadMetrics(taggedScope),
		opts:            opts,
	}

	pointCount := float64(limits.MaxComputedDatapoints())
	h.promReadMetrics.maxDatapoints.Update(pointCount)
	return h
}

// Read reads the given query, returning result and params.
func Read(
	// w http.ResponseWriter,
	r *http.Request,
	cancelWatcher handler.CancelWatcher,
	maxDps int64,
	opts options.HandlerOptions,
) (ReadResult, models.RequestParams, *RespError) {
	// timer := h.promReadMetrics.fetchTimerSuccess.Start()
	fetchOpts, rErr := opts.FetchOptionsBuilder().NewFetchOptions(r)
	if rErr != nil {
		err := &RespError{Err: rErr.Inner(), Code: rErr.Code()}
		return ReadResult{}, emptyReqParams, err
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

	return readWithEngine(r, cancelWatcher, queryOpts, fetchOpts, maxDps, opts)
}

// readWithEngine returns query results from the storage with the given engine.
func readWithEngine(
	r *http.Request,
	cancelWatcher handler.CancelWatcher,
	queryOpts *executor.QueryOptions,
	fetchOpts *storage.FetchOptions,
	maxDps int64,
	opts options.HandlerOptions,
) (ReadResult, models.RequestParams, *RespError) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	iOpts := opts.InstrumentOpts()
	logger := logging.WithContext(ctx, iOpts)
	engine := opts.Engine()

	params, rErr := parseParams(r, engine.Options(),
		opts.TimeoutOpts(), fetchOpts, iOpts)
	if rErr != nil {
		return ReadResult{}, emptyReqParams,
			&RespError{Err: rErr.Inner(), Code: rErr.Code()}
	}

	if params.Debug {
		logger.Info("request params", zap.Any("params", params))
	}

	if err := validateRequest(&params, maxDps); err != nil {
		return ReadResult{}, emptyReqParams,
			&RespError{Err: err, Code: http.StatusBadRequest}
	}

	reqCtx, cancel := context.WithTimeout(ctx, params.Timeout)
	// Detect clients closing connections.
	cancelWatcher.WatchForCancel(reqCtx, cancel)
	result, err := read(reqCtx, engine, queryOpts, fetchOpts, opts.TagOptions(), params)
	cancel()
	if err != nil {
		sp := xopentracing.SpanFromContextOrNoop(ctx)
		sp.LogFields(opentracinglog.Error(err))
		opentracingext.Error.Set(sp, true)
		logger.Error("range query error",
			zap.Error(err),
			zap.Any("params", params),
			zap.Any("queryOpts", queryOpts),
			zap.Any("fetchOpts", fetchOpts))
		return ReadResult{}, emptyReqParams, &RespError{
			Err:  err,
			Code: http.StatusInternalServerError,
		}
	}

	return result, params, nil
}

func validateRequest(
	params *models.RequestParams,
	maxComputedDatapoints int64,
) error {
	// Impose a rough limit on the number of returned time series.
	// This is intended to prevent things like querying from the
	//  beginning of time with a 1s step size.
	numSteps := int64(params.End.Sub(params.Start) / params.Step)
	if maxComputedDatapoints > 0 && numSteps > maxComputedDatapoints {
		return fmt.Errorf(
			"querying from %v to %v with step size %v would result in too many "+
				"datapoints (end - start / step > %d). Either decrease the query "+
				"resolution (?step=XX), decrease the time window, "+
				"or increase the limit (`limits.maxComputedDatapoints`)",
			params.Start, params.End, params.Step, maxComputedDatapoints,
		)
	}

	return nil
}

func (h *PromReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	timer := h.promReadMetrics.fetchTimerSuccess.Start()
	maxDps := h.limitsCfg.MaxComputedDatapoints()
	watcher := handler.NewResponseWriterCanceller(w, h.opts.InstrumentOpts())
	result, params, respErr := Read(r, watcher, maxDps, h.opts)
	if respErr != nil {
		if respErr.Code == http.StatusInternalServerError {
			h.promReadMetrics.fetchErrorsServer.Inc(1)
		} else {
			h.promReadMetrics.fetchErrorsClient.Inc(1)
		}

		xhttp.Error(w, respErr.Err, respErr.Code)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	handleroptions.AddWarningHeaders(w, result.Meta)

	if params.FormatType == models.FormatM3QL {
		renderM3QLResultsJSON(w, result.Series, params)
		h.promReadMetrics.fetchSuccess.Inc(1)
		timer.Stop()
		return
	}

	h.promReadMetrics.fetchSuccess.Inc(1)
	timer.Stop()

	keepEmpty := h.opts.Config().ResultOptions.KeepNans
	// TODO: Support multiple result types
	renderResultsJSON(w, result.Series, params, keepEmpty)
}
