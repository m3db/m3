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
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/instrument"
	xopentracing "github.com/m3db/m3/src/x/opentracing"

	opentracinglog "github.com/opentracing/opentracing-go/log"
)

func read(
	reqCtx context.Context,
	engine executor.Engine,
	opts *executor.QueryOptions,
	fetchOpts *storage.FetchOptions,
	tagOpts models.TagOptions,
	w http.ResponseWriter,
	params models.RequestParams,
	instrumentOpts instrument.Options,
) ([]*ts.Series, error) {
	ctx, cancel := context.WithTimeout(reqCtx, params.Timeout)
	defer cancel()

	sp := xopentracing.SpanFromContextOrNoop(ctx)
	sp.LogFields(
		opentracinglog.String("params.query", params.Query),
		xopentracing.Time("params.start", params.Start),
		xopentracing.Time("params.end", params.End),
		xopentracing.Time("params.now", params.Now),
		xopentracing.Duration("params.step", params.Step),
	)

	// Detect clients closing connections.
	handler.CloseWatcher(ctx, cancel, w, instrumentOpts)

	// TODO: Capture timing
	parser, err := promql.Parse(params.Query, tagOpts)
	if err != nil {
		return nil, err
	}

	result, err := engine.ExecuteExpr(ctx, parser, opts, fetchOpts, params)
	if err != nil {
		return nil, err
	}

	// Block slices are sorted by start time.
	// TODO: Pooling
	sortedBlockList := make([]blockWithMeta, 0, initialBlockAlloc)
	resultChan := result.ResultChan()
	defer func() {
		for range resultChan {
			// NB: drain result channel in case of early termination.
		}
	}()

	firstElement := false
	var numSteps, numSeries int
	// TODO(nikunj): Stream blocks to client
	for blkResult := range resultChan {
		if err := blkResult.Err; err != nil {
			return nil, err
		}

		b := blkResult.Block
		if !firstElement {
			firstElement = true
			firstStepIter, err := b.StepIter()
			if err != nil {
				return nil, err
			}

			firstSeriesIter, err := b.SeriesIter()
			if err != nil {
				return nil, err
			}

			numSteps = firstStepIter.StepCount()
			numSeries = firstSeriesIter.SeriesCount()
		}

		// Insert blocks sorted by start time
		sortedBlockList, err = insertSortedBlock(b, sortedBlockList,
			numSteps, numSeries)
		if err != nil {
			return nil, err
		}
	}

	// Ensure that the blocks are closed. Can't do this above since
	// sortedBlockList might change.
	defer func() {
		for _, b := range sortedBlockList {
			// FIXME: this will double close blocks that have gone through the
			// function pipeline.
			b.block.Close()
		}
	}()

	return sortedBlocksToSeriesList(sortedBlockList)
}

func sortedBlocksToSeriesList(blockList []blockWithMeta) ([]*ts.Series, error) {
	if len(blockList) == 0 {
		return emptySeriesList, nil
	}

	var (
		firstBlock = blockList[0].block
		meta       = firstBlock.Meta()
		bounds     = meta.Bounds
		commonTags = meta.Tags.Tags
	)

	firstSeriesIter, err := firstBlock.SeriesIter()
	if err != nil {
		return nil, err
	}

	var (
		numSeries   = firstSeriesIter.SeriesCount()
		seriesMeta  = firstSeriesIter.SeriesMeta()
		seriesList  = make([]*ts.Series, 0, numSeries)
		seriesIters = make([]block.SeriesIter, 0, len(blockList))
	)

	// To create individual series, we iterate over seriesIterators for each
	// block in the block list.  For each iterator, the nth current() will
	// be combined to give the nth series.
	for _, b := range blockList {
		seriesIter, err := b.block.SeriesIter()
		if err != nil {
			return nil, err
		}

		seriesIters = append(seriesIters, seriesIter)
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
		values := ts.NewFixedStepValues(bounds.StepSize, numValues,
			math.NaN(), bounds.Start)
		valIdx := 0
		for idx, iter := range seriesIters {
			if !iter.Next() {
				if err = iter.Err(); err != nil {
					return nil, err
				}

				return nil, fmt.Errorf(
					"invalid number of datapoints for series: %d, block: %d", i, idx)
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

		var (
			meta   = seriesMeta[i]
			tags   = meta.Tags.AddTags(commonTags)
			series = ts.NewSeries(meta.Name, values, tags)
		)

		seriesList = append(seriesList, series)
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

	meta := b.Meta()
	if len(blockList) == 0 {
		blockList = append(blockList, blockWithMeta{
			block: b,
			meta:  meta,
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
		return blockList[i].meta.Bounds.Start.After(meta.Bounds.Start)
	})

	// Append here ensures enough size in the slice
	blockList = append(blockList, blockWithMeta{})
	copy(blockList[index+1:], blockList[index:])
	blockList[index] = blockWithMeta{
		block: b,
		meta:  meta,
	}

	return blockList, nil
}
