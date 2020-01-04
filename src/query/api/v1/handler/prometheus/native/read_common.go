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
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
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

type readResult struct {
	series []*ts.Series
	meta   block.ResultMetadata
}

func read(
	reqCtx context.Context,
	engine executor.Engine,
	opts *executor.QueryOptions,
	fetchOpts *storage.FetchOptions,
	tagOpts models.TagOptions,
	w http.ResponseWriter,
	params models.RequestParams,
	instrumentOpts instrument.Options,
) (readResult, error) {
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
	emptyResult := readResult{meta: block.NewResultMetadata()}

	// TODO: Capture timing
	parser, err := promql.Parse(params.Query, params.Step, tagOpts)
	if err != nil {
		return emptyResult, err
	}

	result, err := engine.ExecuteExpr(ctx, parser, opts, fetchOpts, params)
	if err != nil {
		return emptyResult, err
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

	var (
		numSteps  int
		numSeries int

		firstElement bool
	)

	meta := block.NewResultMetadata()
	// TODO(nikunj): Stream blocks to client
	for blkResult := range resultChan {
		if err := blkResult.Err; err != nil {
			return emptyResult, err
		}

		b := blkResult.Block
		if !firstElement {
			firstElement = true
			firstStepIter, err := b.StepIter()
			if err != nil {
				return emptyResult, err
			}

			numSteps = firstStepIter.StepCount()
			numSeries = len(firstStepIter.SeriesMeta())
			meta = b.Meta().ResultMetadata
		}

		// Insert blocks sorted by start time.
		insertResult, err := insertSortedBlock(b, sortedBlockList,
			numSteps, numSeries)
		if err != nil {
			return emptyResult, err
		}

		sortedBlockList = insertResult.blocks
		meta = meta.CombineMetadata(insertResult.meta)
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

	series, err := sortedBlocksToSeriesList(sortedBlockList)
	if err != nil {
		return emptyResult, err
	}

	series = prometheus.FilterSeriesByOptions(series, fetchOpts)
	return readResult{
		series: series,
		meta:   meta,
	}, nil
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

	firstIter, err := firstBlock.StepIter()
	if err != nil {
		return nil, err
	}

	var (
		seriesMeta = firstIter.SeriesMeta()
		numSeries  = len(seriesMeta)
		seriesList = make([]*ts.Series, 0, numSeries)
		iters      = make([]block.StepIter, 0, len(blockList))
	)

	// To create individual series, we iterate over seriesIterators for each
	// block in the block list.  For each iterator, the nth current() will
	// be combined to give the nth series.
	for _, b := range blockList {
		it, err := b.block.StepIter()
		if err != nil {
			return nil, err
		}

		iters = append(iters, it)
	}

	numValues := 0
	for _, block := range blockList {
		b, err := block.block.StepIter()
		if err != nil {
			return nil, err
		}

		numValues += b.StepCount()
	}

	// Initialize data slices.
	data := make([]ts.FixedResolutionMutableValues, numSeries)
	for i := range data {
		data[i] = ts.NewFixedStepValues(bounds.StepSize, numValues,
			math.NaN(), bounds.Start)
	}

	stepIndex := 0
	for _, it := range iters {
		for it.Next() {
			step := it.Current()
			for seriesIndex, v := range step.Values() {
				// NB: iteration moves by time step across a block, so each value in the
				// step iterator corresponds to a different series; transform it to
				// series-based iteration using mutable series values.
				mutableValuesForSeries := data[seriesIndex]
				mutableValuesForSeries.SetValueAt(stepIndex, v)
			}

			stepIndex++
		}

		if err := it.Err(); err != nil {
			return nil, err
		}
	}

	for i, values := range data {
		var (
			meta   = seriesMeta[i]
			tags   = meta.Tags.AddTags(commonTags)
			series = ts.NewSeries(meta.Name, values, tags)
		)

		seriesList = append(seriesList, series)
	}

	return seriesList, nil
}

type insertBlockResult struct {
	blocks []blockWithMeta
	meta   block.ResultMetadata
}

func insertSortedBlock(
	b block.Block,
	blockList []blockWithMeta,
	stepCount,
	seriesCount int,
) (insertBlockResult, error) {
	it, err := b.StepIter()
	emptyResult := insertBlockResult{meta: b.Meta().ResultMetadata}
	if err != nil {
		return emptyResult, err
	}

	meta := b.Meta()
	if len(blockList) == 0 {
		blockList = append(blockList, blockWithMeta{
			block: b,
			meta:  meta,
		})

		return insertBlockResult{
			blocks: blockList,
			meta:   b.Meta().ResultMetadata,
		}, nil
	}

	blockSeriesCount := len(it.SeriesMeta())
	if seriesCount != blockSeriesCount {
		return emptyResult, fmt.Errorf(
			"mismatch in number of series for the block, wanted: %d, found: %d",
			seriesCount, blockSeriesCount)
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

	return insertBlockResult{
		meta:   b.Meta().ResultMetadata,
		blocks: blockList,
	}, nil
}
