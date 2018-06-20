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
	"math"
	"net/http"
	"time"

	"github.com/m3db/m3db/src/coordinator/api/v1/handler"
	"github.com/m3db/m3db/src/coordinator/api/v1/handler/prometheus"
	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/executor"
	"github.com/m3db/m3db/src/coordinator/parser/promql"
	"github.com/m3db/m3db/src/coordinator/ts"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"go.uber.org/zap"
)

const (
	// PromReadURL is the url for native prom read handler
	PromReadURL = handler.RoutePrefixV1 + "/prom/native/read"

	// PromReadHTTPMethod is the HTTP method used with this resource.
	PromReadHTTPMethod = http.MethodGet

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

	timeout, err := prometheus.ParseRequestTimeout(r)
	if err != nil {
		handler.Error(w, err, http.StatusBadRequest)
		return
	}

	result, err := h.read(ctx, w, req, timeout)
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

func (h *PromReadHandler) read(reqCtx context.Context, w http.ResponseWriter, req string, timeout time.Duration) ([]ts.Series, error) {
	ctx, cancel := context.WithTimeout(reqCtx, timeout)
	defer cancel()

	opts := &executor.EngineOptions{
		Now: time.Now(),
	}
	// Detect clients closing connections
	abortCh, _ := handler.CloseWatcher(ctx, w)
	opts.AbortCh = abortCh

	parser, err := promql.Parse(req)
	if err != nil {
		return nil, err
	}

	// Results is closed by execute
	results := make(chan executor.Query)
	// Block series slices are sorted by start time
	seriesMap := make(map[string][]block.Series)
	go h.engine.ExecuteExpr(ctx, parser, opts, results)

	for result := range results {
		if result.Err != nil {
			return nil, err
		}

		blocks := result.Result.Blocks()
		// TODO: Stream blocks to client
		for blk := range blocks {
			iter := blk.SeriesIter()
			for iter.Next() {
				insertSeriesInMap(iter.Current(), seriesMap)
			}

			blk.Close()
		}
	}

	return seriesMapToSeriesList(seriesMap)
}

func insertSeriesInMap(blockSeries block.Series, seriesMap map[string][]block.Series) {
	blockList, ok := seriesMap[blockSeries.ID]
	if !ok {
		seriesMap[blockSeries.ID] = make([]block.Series, 1)
		seriesMap[blockSeries.ID][0] = blockSeries
		return
	}

	// Insert sorted by start time
	for idx, s := range blockList {
		if blockSeries.Bounds.Start.Before(s.Bounds.Start) {
			blockList = append(blockList, block.Series{})
			copy(blockList[idx+1:], blockList[idx:])
			blockList[idx] = blockSeries
			seriesMap[blockSeries.ID] = blockList
			return
		}
	}

	// If all start times lesser, then append to the end
	seriesMap[blockSeries.ID] = append(blockList, blockSeries)
}

func seriesMapToSeriesList(seriesMap map[string][]block.Series) ([]ts.Series, error) {
	seriesList := make([]ts.Series, 0, len(seriesMap))
	for _, blockSeriesList := range seriesMap {
		s, err := blockSeriesListToSeries(blockSeriesList)
		if err != nil {
			return nil, err
		}

		seriesList = append(seriesList, *s)
	}

	return seriesList, nil
}

func blockSeriesListToSeries(series []block.Series) (*ts.Series, error) {
	if len(series) == 0 {
		return &ts.Series{}, nil
	}

	firstSeries := series[0]
	var totalDatapoints int
	for _, s := range series {
		totalDatapoints += s.Len()
	}

	values := ts.NewFixedStepValues(firstSeries.Bounds.StepSize, totalDatapoints, math.NaN(), firstSeries.Bounds.Start)
	valIdx := 0
	for _, s := range series {
		for idx := 0 ; idx < s.Len(); idx++ {
			values.SetValueAt(valIdx, s.ValueAtStep(idx))
			valIdx++
		}
	}

	return ts.NewSeries(firstSeries.ID, values, nil), nil
}
