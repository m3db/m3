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
	"bytes"
	"context"
	"net/http"
	"sync"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	// PromReadURL is the url for remote prom read handler
	PromReadURL = handler.RoutePrefixV1 + "/prom/remote/read"

	// PromReadHTTPMethod is the HTTP method used with this resource.
	PromReadHTTPMethod = http.MethodPost
)

// promReadHandler is a handler for the prometheus remote read endpoint.
type promReadHandler struct {
	promReadMetrics promReadMetrics
	opts            options.HandlerOptions
}

// NewPromReadHandler returns a new instance of handler.
func NewPromReadHandler(opts options.HandlerOptions) http.Handler {
	taggedScope := opts.InstrumentOpts().MetricsScope().
		Tagged(map[string]string{"handler": "remote-read"})
	return &promReadHandler{
		promReadMetrics: newPromReadMetrics(taggedScope),
		opts:            opts,
	}
}

type promReadMetrics struct {
	fetchSuccess      tally.Counter
	fetchErrorsServer tally.Counter
	fetchErrorsClient tally.Counter
	fetchTimerSuccess tally.Timer
}

func newPromReadMetrics(scope tally.Scope) promReadMetrics {
	return promReadMetrics{
		fetchSuccess: scope.
			Counter("fetch.success"),
		fetchErrorsServer: scope.Tagged(map[string]string{"code": "5XX"}).
			Counter("fetch.errors"),
		fetchErrorsClient: scope.Tagged(map[string]string{"code": "4XX"}).
			Counter("fetch.errors"),
		fetchTimerSuccess: scope.Timer("fetch.success.latency"),
	}
}

func (h *promReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	timer := h.promReadMetrics.fetchTimerSuccess.Start()
	defer timer.Stop()

	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx, h.opts.InstrumentOpts())
	req, fetchOpts, rErr := parseRequest(ctx, r, h.opts)
	if rErr != nil {
		err := rErr.Inner()
		h.promReadMetrics.fetchErrorsClient.Inc(1)
		logger.Error("remote read query parse error",
			zap.Error(err),
			zap.Any("req", req),
			zap.Any("fetchOpts", fetchOpts))
		xhttp.Error(w, err, rErr.Code())
		return
	}

	cancelWatcher := handler.NewResponseWriterCanceller(w, h.opts.InstrumentOpts())
	readResult, err := Read(ctx, cancelWatcher, req, fetchOpts, h.opts)
	if err != nil {
		h.promReadMetrics.fetchErrorsServer.Inc(1)
		logger.Error("remote read query error",
			zap.Error(err),
			zap.Any("req", req),
			zap.Any("fetchOpts", fetchOpts))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	// NB: if this errors, all relevant headers and information should already
	// be sent to the writer; so it is not necessary to do anything here other
	// than increment success/failure metrics.
	err = WriteSnappyCompressed(w, readResult, logger)
	if err != nil {
		h.promReadMetrics.fetchErrorsServer.Inc(1)
	} else {
		h.promReadMetrics.fetchSuccess.Inc(1)
	}
}

// WriteSnappyCompressed writes snappy compressed results to the given writer.
func WriteSnappyCompressed(
	w http.ResponseWriter,
	readResult ReadResult,
	logger *zap.Logger,
) error {
	resp := &prompb.ReadResponse{
		Results: readResult.Result,
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		logger.Error("unable to marshal read results to protobuf", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return err
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")
	handleroptions.AddWarningHeaders(w, readResult.Meta)

	compressed := snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		logger.Error("unable to encode read results to snappy",
			zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
	}

	return err
}

func parseCompressedRequest(
	r *http.Request,
) (*prompb.ReadRequest, *xhttp.ParseError) {
	result, err := prometheus.ParsePromCompressedRequest(r)
	if err != nil {
		return nil, err
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(result.UncompressedBody, &req); err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return &req, nil
}

// ReadResult is a read result.
type ReadResult struct {
	Meta   block.ResultMetadata
	Result []*prompb.QueryResult
}

func parseRequest(
	ctx context.Context,
	r *http.Request,
	opts options.HandlerOptions,
) (*prompb.ReadRequest, *storage.FetchOptions, *xhttp.ParseError) {
	req, rErr := parseCompressedRequest(r)
	if rErr != nil {
		return nil, nil, rErr
	}

	timeout := opts.TimeoutOpts().FetchTimeout
	timeout, err := prometheus.ParseRequestTimeout(r, timeout)
	if err != nil {
		return nil, nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	fetchOpts, rErr := opts.FetchOptionsBuilder().NewFetchOptions(r)
	if rErr != nil {
		return nil, nil, rErr
	}

	fetchOpts.Timeout = timeout
	return req, fetchOpts, nil
}

// Read performs a remote read on the given engine.
func Read(
	ctx context.Context,
	cancelWatcher handler.CancelWatcher,
	r *prompb.ReadRequest,
	fetchOpts *storage.FetchOptions,
	opts options.HandlerOptions,
) (ReadResult, error) {
	var (
		queryCount   = len(r.Queries)
		cancelFuncs  = make([]context.CancelFunc, queryCount)
		queryResults = make([]*prompb.QueryResult, queryCount)
		meta         = block.NewResultMetadata()
		queryOpts    = &executor.QueryOptions{
			QueryContextOptions: models.QueryContextOptions{
				LimitMaxTimeseries: fetchOpts.Limit,
			}}

		engine = opts.Engine()

		wg       sync.WaitGroup
		mu       sync.Mutex
		multiErr xerrors.MultiError
	)

	wg.Add(queryCount)
	for i, promQuery := range r.Queries {
		i, promQuery := i, promQuery // Capture vars for lambda.
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, fetchOpts.Timeout)
			cancelFuncs[i] = cancel
			query, err := storage.PromReadQueryToM3(promQuery)
			if err != nil {
				mu.Lock()
				multiErr = multiErr.Add(err)
				mu.Unlock()
				return
			}

			// Detect clients closing connections.
			if cancelWatcher != nil {
				cancelWatcher.WatchForCancel(ctx, cancel)
			}

			result, err := engine.ExecuteProm(ctx, query, queryOpts, fetchOpts)
			if err != nil {
				mu.Lock()
				multiErr = multiErr.Add(err)
				mu.Unlock()
				return
			}

			result.PromResult.Timeseries = filterResults(
				result.PromResult.GetTimeseries(), fetchOpts)
			mu.Lock()
			queryResults[i] = result.PromResult
			meta = meta.CombineMetadata(result.Metadata)
			mu.Unlock()
		}()
	}

	wg.Wait()
	for _, cancel := range cancelFuncs {
		cancel()
	}

	if err := multiErr.FinalError(); err != nil {
		return ReadResult{Result: nil, Meta: meta}, err
	}

	return ReadResult{Result: queryResults, Meta: meta}, nil
}

// filterResults removes series tags based on options.
func filterResults(
	series []*prompb.TimeSeries,
	opts *storage.FetchOptions,
) []*prompb.TimeSeries {
	if opts == nil {
		return series
	}

	keys := opts.RestrictQueryOptions.GetRestrictByTag().GetFilterByNames()
	if len(keys) == 0 {
		return series
	}

	for i, s := range series {
		series[i].Labels = filterLabels(s.Labels, keys)
	}

	return series
}

func filterLabels(
	labels []prompb.Label,
	filtering [][]byte,
) []prompb.Label {
	if len(filtering) == 0 {
		return labels
	}

	filtered := labels[:0]
	for _, l := range labels {
		skip := false
		for _, f := range filtering {
			if bytes.Equal(l.GetName(), f) {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		filtered = append(filtered, l)
	}

	return filtered
}
