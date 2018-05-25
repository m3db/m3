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
	"encoding/json"
	"errors"
	"net/http"

	"github.com/m3db/m3db/src/cmd/services/m3coordinator/handler"
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/handler/prometheus"
	"github.com/m3db/m3db/src/coordinator/executor"
	"github.com/m3db/m3db/src/coordinator/parser/promql"
	"github.com/m3db/m3db/src/coordinator/ts"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"go.uber.org/zap"
)

const (
	// PromReadURL is the url for native prom read handler
	PromReadURL = handler.RoutePrefix + "/prom/native/read"

	targetQuery = "target"
)

var (
	errBatchQuery    = errors.New("batch queries are currently not supported")
	errNoQueryFound  = errors.New("no query found")
	errNoTargetFound = errors.New("no target found")
)

// PromReadHandler represents a handler for prometheus read endpoint.
type PromReadHandler struct {
	engine *executor.Engine
}

// ReadResponse is the response that gets returned to the user
type ReadResponse struct {
	Results []ts.Series `json:"results,omitempty"`
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

	resp := &ReadResponse{
		Results: result,
	}

	data, err := json.Marshal(resp)
	if err != nil {
		logger.Error("unable to marshal read results to json", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if _, err := w.Write(data); err != nil {
		logger.Error("unable to write results", zap.Any("err", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}
}

func (h *PromReadHandler) parseRequest(r *http.Request) (string, *handler.ParseError) {
	targetQueries, ok := r.URL.Query()[targetQuery]
	if !ok {
		return "", handler.NewParseError(errNoTargetFound, http.StatusBadRequest)
	}

	// NB(braskin): currently, we only support one query at a time
	if len(targetQueries) > 1 {
		return "", handler.NewParseError(errBatchQuery, http.StatusBadRequest)
	}

	if len(targetQueries) == 0 {
		return "", handler.NewParseError(errNoQueryFound, http.StatusBadRequest)
	}

	return targetQueries[0], nil
}

func (h *PromReadHandler) read(reqCtx context.Context, w http.ResponseWriter, req string, params *prometheus.RequestParams) ([]ts.Series, error) {
	ctx, cancel := context.WithTimeout(reqCtx, params.Timeout)
	defer cancel()

	opts := &executor.EngineOptions{}
	// Detect clients closing connections
	abortCh, _ := handler.CloseWatcher(ctx, w)
	opts.AbortCh = abortCh

	_, err := promql.Parse(req)
	if err != nil {
		return nil, err
	}

	// todo(braskin): implement query execution
	return nil, errors.New("not implemented")
}
