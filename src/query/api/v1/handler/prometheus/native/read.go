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
	"strings"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// PromReadURL is the url for native prom read handler, this matches the
	// default URL for the query range endpoint found on a Prometheus server
	PromReadURL = handler.RoutePrefixV1 + "/query_range"

	// PromReadHTTPMethod is the HTTP method used with this resource.
	PromReadHTTPMethod = http.MethodGet

	// TODO: Move to config
	initialBlockAlloc = 10
)

var (
	emptySeriesList = []*ts.Series{}
	emptyReqParams  = models.RequestParams{}
)

// PromReadHandler represents a handler for prometheus read endpoint.
type PromReadHandler struct {
	engine    *executor.Engine
	tagOpts   models.TagOptions
	limitsCfg *config.LimitsConfiguration
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
func NewPromReadHandler(
	engine *executor.Engine,
	tagOpts models.TagOptions,
	limitsCfg *config.LimitsConfiguration,
) *PromReadHandler {
	return &PromReadHandler{
		engine:    engine,
		tagOpts:   tagOpts,
		limitsCfg: limitsCfg,
	}
}

func (h *PromReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	result, params, respErr := h.ServeHTTPWithEngine(w, r, h.engine)
	if respErr != nil {
		xhttp.Error(w, respErr.Err, respErr.Code)
		return
	}

	if strings.ToLower(r.Header.Get("X-M3-Render-Format")) == "m3ql" {
		w.Header().Set("Content-Type", "application/json")
		renderM3QLResultsJSON(w, result, params)
		return
	}

	// TODO: Support multiple result types
	w.Header().Set("Content-Type", "application/json")
	renderResultsJSON(w, result, params)
}

// ServeHTTPWithEngine returns query results from the storage
func (h *PromReadHandler) ServeHTTPWithEngine(w http.ResponseWriter, r *http.Request, engine *executor.Engine) ([]*ts.Series, models.RequestParams, *RespError) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx)

	params, rErr := parseParams(r)
	if rErr != nil {
		return nil, emptyReqParams, &RespError{Err: rErr.Inner(), Code: rErr.Code()}
	}

	if params.Debug {
		logger.Info("Request params", zap.Any("params", params))
	}

	if err := h.validateRequest(&params); err != nil {
		return nil, emptyReqParams, &RespError{Err: err, Code: http.StatusBadRequest}
	}

	result, err := read(ctx, engine, h.tagOpts, w, params)
	if err != nil {
		logger.Error("unable to fetch data", zap.Error(err))
		return nil, emptyReqParams, &RespError{Err: err, Code: http.StatusInternalServerError}
	}

	return result, params, nil
}

func (h *PromReadHandler) validateRequest(params *models.RequestParams) error {
	// Impose a rough limit on the number of returned time series. This is intended to prevent things like
	// querying from the beginning of time with a 1s step size.
	// Approach taken directly from prom.
	numSteps := int64(params.End.Sub(params.Start) / params.Step)
	if h.limitsCfg.MaxComputedDatapoints > 0 && numSteps > h.limitsCfg.MaxComputedDatapoints {
		return fmt.Errorf(
			"querying from %v to %v with step size %v would result in too many datapoints "+
				"(end - start / step > %d). Either decrease the query resolution (?step=XX), decrease the time window, "+
				"or increase the limit (`limits.maxComputedDatapoints`)",
			params.Start, params.End, params.Step, h.limitsCfg.MaxComputedDatapoints,
		)
	}

	return nil
}
