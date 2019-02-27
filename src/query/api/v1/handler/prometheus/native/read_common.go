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
	"math"
	"net/http"
	"sort"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/ts"
	opentracingutil "github.com/m3db/m3/src/query/util/opentracing"

	opentracinglog "github.com/opentracing/opentracing-go/log"
)

func read(
	reqCtx context.Context,
	engine *executor.Engine,
	tagOpts models.TagOptions,
	w http.ResponseWriter,
	params models.RequestParams,
) ([]*ts.Series, error) {
	ctx, cancel := context.WithTimeout(reqCtx, params.Timeout)
	defer cancel()

	sp := opentracingutil.SpanFromContextOrNoop(ctx)
	sp.LogFields(
		opentracinglog.String("params.query", params.Query),
		opentracingutil.Time("params.start", params.Start),
		opentracingutil.Time("params.end", params.End),
		opentracingutil.Time("params.now", params.Now),
		opentracingutil.Duration("params.step", params.Step),
	)

	opts := &executor.EngineOptions{}
	// Detect clients closing connections
	handler.CloseWatcher(ctx, cancel, w)

	// TODO: Capture timing
	parser, err := promql.Parse(params.Query, tagOpts)
	if err != nil {
		return nil, err
	}

	// Results is closed by execute
	results := make(chan executor.Query)
	go engine.ExecuteExpr(ctx, parser, opts, params, results)
	// Block slices are sorted by start time
	// TODO: Pooling
	sortedBlockList := make([]blockWithMeta, 0, initialBlockAlloc)
	var processErr error
	for result := range results {
		if result.Err != nil {
			processErr = result.Err
			break
		}

		resultChan := result.Result.ResultChan()
		firstElement := false
		var numSteps, numSeries int
		// TODO(nikunj): Stream blocks to client
		for blkResult := range resultChan {
			if blkResult.Err != nil {
				processErr = blkResult.Err
				break
			}

			b := blkResult.Block
			if !firstElement {
				firstElement = true
				firstStepIter, err := b.StepIter()
				if err != nil {
					processErr = err
					break
				}

				firstSeriesIter, err := b.SeriesIter()
				if err != nil {
					processErr = err
					break
				}

				numSteps = firstStepIter.StepCount()
				numSeries = firstSeriesIter.SeriesCount()
			}

			// Insert blocks sorted by start time
			sortedBlockList, err = insertSortedBlock(b, sortedBlockList, numSteps, numSeries)
			if err != nil {
				processErr = err
				break
			}
		}
	}

	// Ensure that the blocks are closed. Can't do this above since sortedBlockList might change
	defer func() {
		for _, b := range sortedBlockList {
			// FIXME: this will double close blocks that have gone through the function pipeline
			b.block.Close()
		}
	}()

	if processErr != nil {
		// Drain anything remaining
		drainResultChan(results)
		return nil, processErr
	}

	return sortedBlocksToSeriesList(sortedBlockList)
}

func drainResultChan(resultsChan chan executor.Query) {
	for result := range resultsChan {
		// Ignore errors during drain
		if result.Err != nil {
			continue
		}

		for range result.Result.ResultChan() {
			// drain out
		}
	}
}

func sortedBlocksToSeriesList(blockList []blockWithMeta) ([]*ts.Series, error) {
	if len(blockList) == 0 {
		return emptySeriesList, nil
	}

	firstBlock := blockList[0].block
	firstSeriesIter, err := firstBlock.SeriesIter()
	if err != nil {
		return nil, err
	}

	numSeries := firstSeriesIter.SeriesCount()
	seriesMeta := firstSeriesIter.SeriesMeta()
	bounds := firstSeriesIter.Meta().Bounds
	commonTags := firstSeriesIter.Meta().Tags.Tags

	seriesList := make([]*ts.Series, numSeries)
	seriesIters := make([]block.SeriesIter, len(blockList))
	// To create individual series, we iterate over seriesIterators for each block in the block list.
	// For each iterator, the nth current() will be combined to give the nth series
	for i, b := range blockList {
		seriesIter, err := b.block.SeriesIter()
		if err != nil {
			return nil, err
		}

		seriesIters[i] = seriesIter
	}

	numValues := 0
	for _, block := range blockList {
		b, err := block.block.StepIter()
		if err != nil {
			return nil, err
		}

		numValues += b.StepCount()
	}

	for i := 0; i < numSeries; i++ {
		values := ts.NewFixedStepValues(bounds.StepSize, numValues, math.NaN(), bounds.Start)
		valIdx := 0
		for idx, iter := range seriesIters {
			if !iter.Next() {
				if err = iter.Err(); err != nil {
					return nil, err
				}

				return nil, fmt.Errorf("invalid number of datapoints for series: %d, block: %d", i, idx)
			}

			if err = iter.Err(); err != nil {
				return nil, err
			}

			blockSeries := iter.Current()
			for j := 0; j < blockSeries.Len(); j++ {
				values.SetValueAt(valIdx, blockSeries.ValueAtStep(j))
				valIdx++
			}
		}

		tags := seriesMeta[i].Tags.AddTags(commonTags)
		seriesList[i] = ts.NewSeries(seriesMeta[i].Name, values, tags)
	}

	return seriesList, nil
}

func insertSortedBlock(
	b block.Block,
	blockList []blockWithMeta,
	stepCount,
	seriesCount int,
) ([]blockWithMeta, error) {
	blockSeriesIter, err := b.SeriesIter()
	if err != nil {
		return nil, err
	}

	blockMeta := blockSeriesIter.Meta()
	if len(blockList) == 0 {
		blockList = append(blockList, blockWithMeta{
			block: b,
			meta:  blockMeta,
		})
		return blockList, nil
	}

	blockSeriesCount := blockSeriesIter.SeriesCount()
	if seriesCount != blockSeriesCount {
		return nil, fmt.Errorf("mismatch in number of series for "+
			"the block, wanted: %d, found: %d", seriesCount, blockSeriesCount)
	}

	// Binary search to keep the start times sorted
	index := sort.Search(len(blockList), func(i int) bool {
		return blockList[i].meta.Bounds.Start.After(blockMeta.Bounds.Start)
	})

	// Append here ensures enough size in the slice
	blockList = append(blockList, blockWithMeta{})
	copy(blockList[index+1:], blockList[index:])
	blockList[index] = blockWithMeta{
		block: b,
		meta:  blockMeta,
	}

	return blockList, nil
}
