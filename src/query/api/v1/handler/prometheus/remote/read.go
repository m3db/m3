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
	"sync"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
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

// PromReadHandler represents a handler for prometheus read endpoint.
type PromReadHandler struct {
	engine              executor.Engine
	promReadMetrics     promReadMetrics
	timeoutOpts         *prometheus.TimeoutOpts
	fetchOptionsBuilder handler.FetchOptionsBuilder
	keepEmpty           bool
	instrumentOpts      instrument.Options
}

// NewPromReadHandler returns a new instance of handler.
func NewPromReadHandler(
	engine executor.Engine,
	fetchOptionsBuilder handler.FetchOptionsBuilder,
	timeoutOpts *prometheus.TimeoutOpts,
	keepEmpty bool,
	instrumentOpts instrument.Options,
) http.Handler {
	taggedScope := instrumentOpts.MetricsScope().
		Tagged(map[string]string{"handler": "remote-read"})
	return &PromReadHandler{
		engine:              engine,
		promReadMetrics:     newPromReadMetrics(taggedScope),
		timeoutOpts:         timeoutOpts,
		fetchOptionsBuilder: fetchOptionsBuilder,
		keepEmpty:           keepEmpty,
		instrumentOpts:      instrumentOpts,
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

func (h *PromReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	timer := h.promReadMetrics.fetchTimerSuccess.Start()
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx, h.instrumentOpts)
	req, rErr := h.parseRequest(r)
	if rErr != nil {
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	timeout, err := prometheus.ParseRequestTimeout(r, h.timeoutOpts.FetchTimeout)
	if err != nil {
		h.promReadMetrics.fetchErrorsClient.Inc(1)
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	fetchOpts, rErr := h.fetchOptionsBuilder.NewFetchOptions(r)
	if rErr != nil {
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	readResult, err := h.read(ctx, w, req, timeout, fetchOpts)
	if err != nil {
		h.promReadMetrics.fetchErrorsServer.Inc(1)
		logger.Error("unable to fetch data", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &prompb.ReadResponse{
		Results: readResult.result,
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		h.promReadMetrics.fetchErrorsServer.Inc(1)
		logger.Error("unable to marshal read results to protobuf", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")
	handler.AddWarningHeaders(w, readResult.meta)

	compressed := snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		h.promReadMetrics.fetchErrorsServer.Inc(1)
		logger.Error("unable to encode read results to snappy",
			zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	timer.Stop()
	h.promReadMetrics.fetchSuccess.Inc(1)
}

func (h *PromReadHandler) parseRequest(
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

type readResult struct {
	result []*prompb.QueryResult
	meta   block.ResultMetadata
}

func (h *PromReadHandler) read(
	reqCtx context.Context,
	w http.ResponseWriter,
	r *prompb.ReadRequest,
	timeout time.Duration,
	fetchOpts *storage.FetchOptions,
) (readResult, error) {
	var (
		queryCount  = len(r.Queries)
		promResults = make([]*prompb.QueryResult, queryCount)
		cancelFuncs = make([]context.CancelFunc, queryCount)
		queryOpts   = &executor.QueryOptions{
			QueryContextOptions: models.QueryContextOptions{
				LimitMaxTimeseries: fetchOpts.Limit,
			}}

		wg       sync.WaitGroup
		mu       sync.Mutex
		multiErr xerrors.MultiError
		meta     = block.NewResultMetadata()
	)

	wg.Add(queryCount)
	for i, promQuery := range r.Queries {
		i, promQuery := i, promQuery // Capture vars for lambda.
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(reqCtx, timeout)
			cancelFuncs[i] = cancel
			query, err := storage.PromReadQueryToM3(promQuery)
			if err != nil {
				mu.Lock()
				multiErr = multiErr.Add(err)
				mu.Unlock()
				return
			}

			// Detect clients closing connections
			handler.CloseWatcher(ctx, cancel, w, h.instrumentOpts)
			result, err := h.engine.Execute(ctx, query, queryOpts, fetchOpts)
			if err != nil {
				mu.Lock()
				multiErr = multiErr.Add(err)
				mu.Unlock()
				return
			}

			mu.Lock()
			meta = meta.CombineMetadata(result.Metadata)
			mu.Unlock()
			promRes := storage.FetchResultToPromResult(result, h.keepEmpty)
			promResults[i] = promRes
		}()
	}

	wg.Wait()
	for _, cancel := range cancelFuncs {
		cancel()
	}

	if err := multiErr.FinalError(); err != nil {
		return readResult{nil, meta}, err
	}

	return readResult{promResults, meta}, nil
}
