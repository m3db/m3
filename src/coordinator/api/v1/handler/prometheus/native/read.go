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
	"math"
	"net/http"

	"github.com/m3db/m3db/src/coordinator/api/v1/handler"
	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/executor"
	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/m3db/m3db/src/coordinator/parser/promql"
	"github.com/m3db/m3db/src/coordinator/ts"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"go.uber.org/zap"
	"sort"
	"fmt"
)

const (
	// PromReadURL is the url for native prom read handler
	PromReadURL = handler.RoutePrefixV1 + "/prom/native/read"

	// PromReadHTTPMethod is the HTTP method used with this resource.
	PromReadHTTPMethod = http.MethodGet
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

	params, rErr := parseParams(r)
	if rErr != nil {
		handler.Error(w, rErr.Error(), rErr.Code())
		return
	}

	result, err := h.read(ctx, w, params)
	if err != nil {
		logger.Error("unable to fetch data", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	renderResultsJSON(w, result)
}

func (h *PromReadHandler) read(reqCtx context.Context, w http.ResponseWriter, params models.RequestParams) ([]ts.Series, error) {
	ctx, cancel := context.WithTimeout(reqCtx, params.Timeout)
	defer cancel()

	opts := &executor.EngineOptions{}
	// Detect clients closing connections
	abortCh, _ := handler.CloseWatcher(ctx, w)
	opts.AbortCh = abortCh

	// TODO: Capture timing
	parser, err := promql.Parse(params.Target)
	if err != nil {
		return nil, err
	}

	// Results is closed by execute
	results := make(chan executor.Query)
	go h.engine.ExecuteExpr(ctx, parser, opts, params, results)

	// Block slices are sorted by start time
	// TODO: Pooling
	sortedBlockList := make([]block.Block, 0, 10)
	for result := range results {
		if result.Err != nil {
			return nil, result.Err
		}

		blocks := result.Result.Blocks()
		// TODO(nikunj): Stream blocks to client
		for blkResult := range blocks {
			if blkResult.Err != nil {
				return nil, blkResult.Err
			}

			// Insert blocks sorted by start time
			sortedBlockList, err = insertSortedBlock(blkResult.Block, sortedBlockList)
			if err != nil {
				return nil, err
			}
		}
	}

	return sortedBlocksToSeriesList(sortedBlockList)
}

func sortedBlocksToSeriesList(blockList []block.Block) ([]ts.Series, error) {
	if len(blockList) == 0 {
		return []ts.Series{}, nil
	}

	firstBlock := blockList[0]
	numSeries := firstBlock.Series()
	seriesList := make([]ts.Series, numSeries)
	seriesIters := make([]block.SeriesIter, len(blockList))
	for i, b := range blockList {
		seriesIters[i] = b.SeriesIter()
	}

	seriesMeta := firstBlock.SeriesMeta()
	bounds := firstBlock.Meta().Bounds
	for i := 0; i < numSeries; i++ {
		values := ts.NewFixedStepValues(bounds.StepSize, firstBlock.Steps()*len(blockList), math.NaN(), bounds.Start)
		valIdx := 0
		for idx, iter := range seriesIters {
			fmt.Println("Series Iter", i, idx, firstBlock.Series(), firstBlock.Steps())
			if !iter.Next() {
				return nil, fmt.Errorf("invalid number of datapoints for series: %d, block: %d", i, idx)
			}

			blockSeries := iter.Current()
			for i := 0; i < blockSeries.Len(); i++ {
				values.SetValueAt(valIdx, blockSeries.ValueAtStep(i))
				valIdx++
			}
		}

		seriesList[i] = *ts.NewSeries(seriesMeta[i].Name, values, seriesMeta[i].Tags)

	}

	return seriesList, nil
}

func insertSortedBlock(b block.Block, blockList []block.Block) ([]block.Block, error) {
	if len(blockList) == 0 {
		blockList = append(blockList, b)
		return blockList, nil
	}

	firstBlock := blockList[0]
	if firstBlock.Series() != b.Series() {
		return nil, fmt.Errorf("mismatch in number of series for the block, wanted: %d, found: %d", firstBlock.Series(), b.Series())
	}

	if firstBlock.Steps() != b.Steps() {
		return nil, fmt.Errorf("mismatch in number of steps for the block, wanted: %d, found: %d", firstBlock.Steps(), b.Steps())
	}

	index := sort.Search(len(blockList), func(i int) bool { return blockList[i].Meta().Bounds.Start.Before(b.Meta().Bounds.Start) })
	blockList = append(blockList, b)
	copy(blockList[index+1:], blockList[index:])
	blockList[index] = b
	return blockList, nil
}
