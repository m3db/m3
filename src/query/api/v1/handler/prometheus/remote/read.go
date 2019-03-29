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
	"fmt"
	"net/http"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
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
	engine          *executor.Engine
	promReadMetrics promReadMetrics
	timeoutOpts     *prometheus.TimeoutOpts
}

// NewPromReadHandler returns a new instance of handler.
func NewPromReadHandler(engine *executor.Engine, scope tally.Scope, timeoutOpts *prometheus.TimeoutOpts) http.Handler {
	return &PromReadHandler{
		engine:          engine,
		promReadMetrics: newPromReadMetrics(scope),
		timeoutOpts:     timeoutOpts,
	}
}

type promReadMetrics struct {
	fetchSuccess      tally.Counter
	fetchErrorsServer tally.Counter
	fetchErrorsClient tally.Counter
}

func newPromReadMetrics(scope tally.Scope) promReadMetrics {
	return promReadMetrics{
		fetchSuccess:      scope.Counter("fetch.success"),
		fetchErrorsServer: scope.Tagged(map[string]string{"code": "5XX"}).Counter("fetch.errors"),
		fetchErrorsClient: scope.Tagged(map[string]string{"code": "4XX"}).Counter("fetch.errors"),
	}
}

func (h *PromReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)

	logger := logging.WithContext(ctx)

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

	result, err := h.read(ctx, w, req, timeout)
	if err != nil {
		h.promReadMetrics.fetchErrorsServer.Inc(1)
		logger.Error("unable to fetch data", zap.Any("error", err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &prompb.ReadResponse{
		Results: result,
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		h.promReadMetrics.fetchErrorsServer.Inc(1)
		logger.Error("unable to marshal read results to protobuf", zap.Any("error", err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed := snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		h.promReadMetrics.fetchErrorsServer.Inc(1)
		logger.Error("unable to encode read results to snappy", zap.Any("err", err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	h.promReadMetrics.fetchSuccess.Inc(1)
}

func (h *PromReadHandler) parseRequest(
	r *http.Request,
) (*prompb.ReadRequest, *xhttp.ParseError) {
	reqBuf, err := prometheus.ParsePromCompressedRequest(r)
	if err != nil {
		return nil, err
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return &req, nil
}

func (h *PromReadHandler) read(
	reqCtx context.Context,
	w http.ResponseWriter,
	r *prompb.ReadRequest,
	timeout time.Duration,
) ([]*prompb.QueryResult, error) {
	// TODO: Handle multi query use case
	if len(r.Queries) != 1 {
		return nil, fmt.Errorf("prometheus read endpoint currently only supports one query at a time")
	}

	ctx, cancel := context.WithTimeout(reqCtx, timeout)
	defer cancel()
	promQuery := r.Queries[0]
	query, err := storage.PromReadQueryToM3(promQuery)
	if err != nil {
		return nil, err
	}

	// Results is closed by execute
	results := make(chan *storage.QueryResult)

	opts := &executor.EngineOptions{}
	// Detect clients closing connections
	handler.CloseWatcher(ctx, cancel, w)
	go h.engine.Execute(ctx, query, opts, results)

	promResults := make([]*prompb.QueryResult, 0, 1)
	for result := range results {
		if result.Err != nil {
			return nil, result.Err
		}

		promRes := storage.FetchResultToPromResult(result.FetchResult)
		promResults = append(promResults, promRes)
	}

	return promResults, nil
}
