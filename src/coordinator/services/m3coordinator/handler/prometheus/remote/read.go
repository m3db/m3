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

	"github.com/m3db/m3coordinator/executor"
	"github.com/m3db/m3coordinator/generated/proto/prompb"
	"github.com/m3db/m3coordinator/services/m3coordinator/handler"
	"github.com/m3db/m3coordinator/services/m3coordinator/handler/prometheus"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"go.uber.org/zap"
)

const (
	// PromReadURL is the url for remote prom read handler
	PromReadURL = "/api/v1/prom/remote/read"
)

// PromReadHandler represents a handler for prometheus read endpoint.
type PromReadHandler struct {
	engine *executor.Engine
}

// NewPromReadHandler returns a new instance of handler.
func NewPromReadHandler(engine *executor.Engine) http.Handler {
	return &PromReadHandler{engine: engine}
}

func (h *PromReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, rErr := h.parseRequest(r)

	if rErr != nil {
		handler.Error(w, rErr.Error(), rErr.Code())
		return
	}

	params, err := prometheus.ParseRequestParams(r)
	if err != nil {
		handler.Error(w, err, http.StatusBadRequest)
		return
	}

	result, err := h.read(ctx, w, req, params)
	if err != nil {
		logger.Error("unable to fetch data", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &prompb.ReadResponse{
		Results: result,
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		logger.Error("unable to marshal read results to protobuf", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed := snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		logger.Error("unable to encode read results to snappy", zap.Any("err", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *PromReadHandler) parseRequest(r *http.Request) (*prompb.ReadRequest, *handler.ParseError) {
	reqBuf, err := prometheus.ParsePromCompressedRequest(r)
	if err != nil {
		return nil, err
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	return &req, nil
}

func (h *PromReadHandler) read(reqCtx context.Context, w http.ResponseWriter, r *prompb.ReadRequest, params *prometheus.RequestParams) ([]*prompb.QueryResult, error) {
	// TODO: Handle multi query use case
	if len(r.Queries) != 1 {
		return nil, fmt.Errorf("prometheus read endpoint currently only supports one query at a time")
	}

	ctx, cancel := context.WithTimeout(reqCtx, params.Timeout)
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
	abortCh, closingCh := handler.CloseWatcher(ctx, w)
	opts.AbortCh = abortCh

	go h.engine.Execute(ctx, query, opts, closingCh, results)

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
