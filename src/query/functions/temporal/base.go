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

package temporal

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/opentracing"

	"go.uber.org/zap"
)

var emptyOp = baseOp{}

// baseOp stores required properties for logical operations.
type baseOp struct {
	operatorType string
	duration     time.Duration
	processorFn  makeProcessor
}

func newBaseOp(
	duration time.Duration,
	operatorType string,
	processorFn makeProcessor,
) (baseOp, error) {
	return baseOp{
		operatorType: operatorType,
		processorFn:  processorFn,
		duration:     duration,
	}, nil
}

func (o baseOp) OpType() string {
	return o.operatorType
}

func (o baseOp) String() string {
	return fmt.Sprintf("type: %s, duration: %v", o.OpType(), o.duration)
}

// Node creates an execution node.
func (o baseOp) Node(
	controller *transform.Controller,
	opts transform.Options,
) transform.OpNode {
	return &baseNode{
		controller:    controller,
		cache:         newBlockCache(o, opts),
		op:            o,
		processor:     o.processorFn.initialize(o.duration, controller, opts),
		transformOpts: opts,
	}
}

// baseNode is an execution node.
type baseNode struct {
	// controller uses an interface here so we can mock it out in tests.
	// TODO: use an exported interface everywhere instead of *transform.Controller.
	// https://github.com/m3db/m3/issues/1430
	controller    controller
	op            baseOp
	cache         *blockCache
	processor     processor
	transformOpts transform.Options
}

// Process processes a block. The processing steps are as follows:
// 1. Figure out the maximum blocks needed for the temporal function
// 2. For the current block, figure out whether we have enough previous blocks
//    which can help process it
// 3. For the blocks after current block, figure out which can be processed
//    right now
// 4. Process all valid blocks from #3, #4 and mark them as processed
// 5. Run a sweep phase to free up blocks which are no longer needed to be
//    cached
func (c *baseNode) Process(
	queryCtx *models.QueryContext,
	ID parser.NodeID,
	b block.Block,
) error {
	unconsolidatedBlock, err := b.Unconsolidated()
	if err != nil {
		return err
	}

	if unconsolidatedBlock == nil {
		return fmt.Errorf(
			"block needs to be unconsolidated for temporal operations: %s", c.op)
	}

	meta := b.Meta()
	bounds := meta.Bounds
	queryStartBounds := bounds.Nearest(c.transformOpts.TimeSpec().Start)
	if bounds.Duration == 0 {
		return fmt.Errorf("bound duration cannot be 0, bounds: %v", bounds)
	}

	if bounds.Start.Before(queryStartBounds.Start) {
		return fmt.Errorf(
			"block start cannot be before query start, bounds: %v, queryStart: %v",
			bounds, queryStartBounds)
	}

	queryEndBounds := bounds.
		Nearest(c.transformOpts.TimeSpec().End.Add(-1 * bounds.StepSize))
	if bounds.Start.After(queryEndBounds.Start) {
		return fmt.Errorf(
			"block start cannot be after query end, bounds: %v, query end: %v",
			bounds, queryEndBounds)
	}

	c.cache.initialize(bounds)
	blockDuration := bounds.Duration
	// Figure out the maximum blocks needed for the temporal function.
	maxBlocks := int(math.Ceil(float64(c.op.duration) / float64(blockDuration)))

	// Figure out the leftmost block.
	leftRangeStart := bounds.Previous(maxBlocks)

	if leftRangeStart.Start.Before(queryStartBounds.Start) {
		leftRangeStart = queryStartBounds
	}

	// Figure out the rightmost blocks.
	rightRangeStart := bounds.Next(maxBlocks)

	if rightRangeStart.Start.After(queryEndBounds.Start) {
		rightRangeStart = queryEndBounds
	}

	// Process the current block by figuring out the left range.
	leftBlks, emptyLeftBlocks, err := c.processCurrent(bounds, leftRangeStart)
	if err != nil {
		return err
	}

	processRequests := make([]processRequest, 0, len(leftBlks))
	// If we have all blocks for the left range in the cache, then
	// process the current block.
	if !emptyLeftBlocks {
		processRequests = append(processRequests, processRequest{
			blk:      unconsolidatedBlock,
			deps:     leftBlks,
			bounds:   bounds,
			queryCtx: queryCtx,
		})
	}

	leftBlks = append(leftBlks, unconsolidatedBlock)

	// Process right side of the range.
	rightBlks, emptyRightBlocks, err := c.processRight(bounds, rightRangeStart)
	if err != nil {
		return err
	}

	for i := 0; i < len(rightBlks); i++ {
		lStart := maxBlocks - i
		if lStart > len(leftBlks) {
			continue
		}

		deps := leftBlks[len(leftBlks)-lStart:]
		deps = append(deps, rightBlks[:i]...)
		processRequests = append(
			processRequests,
			processRequest{
				blk:      rightBlks[i],
				deps:     deps,
				bounds:   bounds.Next(i + 1),
				queryCtx: queryCtx})
	}

	// If either the left range or right range wasn't fully processed then
	// cache the current block.
	if emptyLeftBlocks || emptyRightBlocks {
		if err := c.cache.add(bounds.Start, unconsolidatedBlock); err != nil {
			return err
		}
	}

	blocks, err := c.processCompletedBlocks(queryCtx, processRequests, maxBlocks)
	if err != nil {
		return err
	}

	defer closeBlocks(blocks)

	return c.propagateNextBlocks(processRequests, blocks, maxBlocks)
}

func closeBlocks(blocks []block.Block) {
	for _, bl := range blocks {
		bl.Close()
	}
}

// processCurrent processes the current block. For the current block,
// figure out whether we have enough previous blocks which can help process it.
func (c *baseNode) processCurrent(
	bounds models.Bounds,
	leftRangeStart models.Bounds,
) ([]block.UnconsolidatedBlock, bool, error) {
	numBlocks := bounds.Blocks(leftRangeStart.Start)
	leftBlks, err := c.cache.multiGet(leftRangeStart, numBlocks, true)
	if err != nil {
		return nil, false, err
	}
	return leftBlks, len(leftBlks) != numBlocks, nil
}

// processRight processes blocks after current block. This is done by fetching
// all contiguous right blocks until the right range.
func (c *baseNode) processRight(
	bounds models.Bounds,
	rightRangeStart models.Bounds,
) ([]block.UnconsolidatedBlock, bool, error) {
	numBlocks := rightRangeStart.Blocks(bounds.Start)
	rightBlks, err := c.cache.multiGet(bounds.Next(1), numBlocks, false)
	if err != nil {
		return nil, false, err
	}

	return rightBlks, len(rightBlks) != numBlocks, nil
}

func (c *baseNode) propagateNextBlocks(
	processRequests []processRequest,
	blocks []block.Block,
	maxBlocks int,
) error {
	processedKeys := make([]time.Time, len(processRequests))

	// propagate blocks downstream
	for i, nextBlock := range blocks {
		req := processRequests[i]
		if err := c.controller.Process(req.queryCtx, nextBlock); err != nil {
			return err
		}

		processedKeys[i] = req.bounds.Start
	}

	// Mark all blocks as processed
	c.cache.markProcessed(processedKeys)

	// Sweep to free blocks from cache with no dependencies
	c.sweep(c.cache.processed(), maxBlocks)
	return nil
}

// processCompletedBlocks processes all blocks for which all
// dependent blocks are present.
func (c *baseNode) processCompletedBlocks(
	queryCtx *models.QueryContext,
	processRequests []processRequest,
	maxBlocks int,
) ([]block.Block, error) {
	sp, _ := opentracing.StartSpanFromContext(queryCtx.Ctx, c.op.OpType())
	defer sp.Finish()

	blocks := make([]block.Block, 0, len(processRequests))
	// NB: valueBuffer gets populated and re-used within the processSingleRequest
	// function call.
	var valueBuffer ts.Datapoints
	for _, req := range processRequests {
		bl, err := c.processSingleRequest(req, valueBuffer)
		if err != nil {
			// cleanup any blocks we opened
			closeBlocks(blocks)
			return nil, err
		}

		blocks = append(blocks, bl)
	}

	return blocks, nil
}

// getIndices returns the index of the points on the left and the right of the
// datapoint list given a starting index, as well as a boolean indicating if
// the returned indices are valid.
//
// NB: return values from getIndices should be used as subslice indices rather
// than direct index accesses, as that may cause panics when reaching the end of
// the datapoint list.
func getIndices(
	dp []ts.Datapoint,
	lBound time.Time,
	rBound time.Time,
	init int,
) (int, int, bool) {
	if init >= len(dp) || init < 0 {
		return -1, -1, false
	}

	var (
		l, r      = init, -1
		leftBound = false
	)

	for i, dp := range dp[init:] {
		ts := dp.Timestamp
		if !leftBound {
			// Trying to set left bound.
			if ts.Before(lBound) {
				// data point before 0.
				continue
			}

			leftBound = true
			l = i
		}

		if ts.Before(rBound) {
			continue
		}

		r = i
		break
	}

	if r == -1 {
		r = len(dp)
	} else {
		r = r + init
	}

	if leftBound {
		l = l + init
	}

	return l, r, true
}

func buildValueBuffer(
	current block.UnconsolidatedSeries,
	iters []block.UnconsolidatedSeriesIter,
) ts.Datapoints {
	l := 0
	for _, dps := range current.Datapoints() {
		l += len(dps)
	}

	for _, it := range iters {
		for _, dps := range it.Current().Datapoints() {
			l += len(dps)
		}
	}

	// NB: sanity check; theoretically this should never happen
	// as empty series should not exist when building the value buffer.
	if l < 1 {
		return ts.Datapoints{}
	}

	return make(ts.Datapoints, 0, l)
}

func (c *baseNode) processSingleRequest(
	request processRequest,
	valueBuffer ts.Datapoints,
) (block.Block, error) {
	seriesIter, err := request.blk.SeriesIter()
	if err != nil {
		return nil, err
	}

	var (
		meta       = request.blk.Meta()
		bounds     = meta.Bounds
		seriesMeta = seriesIter.SeriesMeta()
	)

	// rename series to exclude their __name__ tag as part of function processing.
	resultSeriesMeta := make([]block.SeriesMeta, 0, len(seriesMeta))
	for _, m := range seriesMeta {
		tags := m.Tags.WithoutName()
		resultSeriesMeta = append(resultSeriesMeta, block.SeriesMeta{
			Name: tags.ID(),
			Tags: tags,
		})
	}

	builder, err := c.controller.BlockBuilder(request.queryCtx,
		meta, resultSeriesMeta)
	if err != nil {
		return nil, err
	}

	if err := builder.AddCols(bounds.Steps()); err != nil {
		return nil, err
	}

	aggDuration := c.op.duration
	depIters := make([]block.UnconsolidatedSeriesIter, 0, len(request.deps))
	for _, b := range request.deps {
		iter, err := b.SeriesIter()
		if err != nil {
			return nil, err
		}

		depIters = append(depIters, iter)
	}

	for seriesIter.Next() {
		series := seriesIter.Current()
		// First, advance the iterators to ensure they all have this series.
		for i, iter := range depIters {
			if !iter.Next() {
				return nil, fmt.Errorf("incorrect number of series for block: %d", i)
			}
		}

		// If valueBuffer is still unset, build it here; if it's been set in a
		// previous iteration, reset it for this processing step.
		if valueBuffer == nil {
			valueBuffer = buildValueBuffer(series, depIters)
		} else {
			valueBuffer = valueBuffer[:0]
		}

		// Write datapoints into value buffer.
		for _, iter := range depIters {
			s := iter.Current()
			for _, dps := range s.Datapoints() {
				valueBuffer = append(valueBuffer, dps...)
			}
		}

		var (
			newVal      float64
			init        = 0
			alignedTime = bounds.Start
			start       = alignedTime.Add(-1 * aggDuration)
		)

		for i := 0; i < series.Len(); i++ {
			val := series.DatapointsAtStep(i)
			valueBuffer = append(valueBuffer, val...)
			l, r, b := getIndices(valueBuffer, start, alignedTime, init)
			if !b {
				newVal = c.processor.process(ts.Datapoints{}, alignedTime)
			} else {
				init = l
				newVal = c.processor.process(valueBuffer[l:r], alignedTime)
			}

			if err := builder.AppendValue(i, newVal); err != nil {
				return nil, err
			}

			start = start.Add(bounds.StepSize)
			alignedTime = alignedTime.Add(bounds.StepSize)
		}
	}

	if err = seriesIter.Err(); err != nil {
		return nil, err
	}

	return builder.Build(), nil
}

func (c *baseNode) sweep(processedKeys []bool, maxBlocks int) {
	prevProcessed := 0
	maxRight := len(processedKeys) - 1
	for i := maxRight; i >= 0; i-- {
		processed := processedKeys[i]
		if !processed {
			prevProcessed = 0
			continue
		}

		dependentBlocks := maxBlocks
		remainingBlocks := maxRight - i
		if dependentBlocks > remainingBlocks {
			dependentBlocks = remainingBlocks
		}

		if prevProcessed >= dependentBlocks {
			if err := c.cache.remove(i); err != nil {
				logging.WithContext(context.TODO(), c.transformOpts.InstrumentOptions()).
					Warn("unable to remove key from cache", zap.Int("index", i))
			}
		}

		prevProcessed++
	}
}

// processor is implemented by the underlying transforms.
type processor interface {
	process(valueBuffer ts.Datapoints, evaluationTime time.Time) float64
}

// makeProcessor is a way to create a transform.
type makeProcessor interface {
	// initialize initializes the processor.
	initialize(
		duration time.Duration,
		controller *transform.Controller,
		opts transform.Options,
	) processor
}

type processRequest struct {
	queryCtx *models.QueryContext
	blk      block.UnconsolidatedBlock
	bounds   models.Bounds
	deps     []block.UnconsolidatedBlock
}

// blockCache keeps track of blocks from the same parent across time
type blockCache struct {
	mu              sync.Mutex
	initialized     bool
	blockList       []block.UnconsolidatedBlock
	op              baseOp
	transformOpts   transform.Options
	startBounds     models.Bounds
	endBounds       models.Bounds
	processedBlocks []bool
}

func newBlockCache(op baseOp, transformOpts transform.Options) *blockCache {
	return &blockCache{
		op:            op,
		transformOpts: transformOpts,
	}
}

func (c *blockCache) initialize(bounds models.Bounds) {
	if c.initialized {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return
	}

	timeSpec := c.transformOpts.TimeSpec()
	c.startBounds = bounds.Nearest(timeSpec.Start)
	c.endBounds = bounds.Nearest(timeSpec.End.Add(-1 * bounds.StepSize))
	numBlocks := c.endBounds.End().Sub(c.startBounds.Start) / bounds.Duration
	c.blockList = make([]block.UnconsolidatedBlock, numBlocks)
	c.processedBlocks = make([]bool, numBlocks)
	c.initialized = true
}

func (c *blockCache) index(t time.Time) (int, error) {
	start := c.startBounds.Start
	if t.Before(start) || t.After(c.endBounds.Start) {
		return 0, fmt.Errorf("invalid time for the block cache: %v, start: %v, end: %v", t, start, c.endBounds.Start)
	}

	return int(t.Sub(start) / c.startBounds.Duration), nil
}

// Add the block to the cache, errors out if block already exists
func (c *blockCache) add(key time.Time, b block.UnconsolidatedBlock) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	index, err := c.index(key)
	if err != nil {
		return err
	}

	if c.blockList[index] != nil {
		return fmt.Errorf("block already exists at index: %d", index)
	}

	c.blockList[index] = b
	return nil
}

// Remove the block from the cache
func (c *blockCache) remove(idx int) error {
	if idx >= len(c.blockList) {
		return fmt.Errorf("index out of range for remove: %d", idx)
	}

	c.mu.Lock()
	c.blockList[idx] = nil
	c.mu.Unlock()

	return nil
}

// Get the block from the cache
func (c *blockCache) get(key time.Time) (block.UnconsolidatedBlock, bool) {
	c.mu.Lock()
	index, err := c.index(key)
	if err != nil {
		c.mu.Unlock()
		return nil, false
	}

	b := c.blockList[index]
	c.mu.Unlock()
	return b, b != nil
}

// multiGet retrieves multiple blocks from the cache at once until if finds an empty block
func (c *blockCache) multiGet(startBounds models.Bounds, numBlocks int, reverse bool) ([]block.UnconsolidatedBlock, error) {
	if numBlocks == 0 {
		return []block.UnconsolidatedBlock{}, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	blks := make([]block.UnconsolidatedBlock, 0, numBlocks)
	startIdx, err := c.index(startBounds.Start)
	if err != nil {
		return nil, err
	}

	// Fetch an index and notified if it was empty
	fetchAndCheckEmpty := func(i int) (bool, error) {
		if startIdx+i >= len(c.blockList) {
			return true, fmt.Errorf("index out of range: %d", startIdx+i)
		}

		b := c.blockList[startIdx+i]
		if b == nil {
			return true, nil
		}

		blks = append(blks, b)
		return false, nil
	}

	if reverse {
		for i := numBlocks - 1; i >= 0; i-- {
			empty, err := fetchAndCheckEmpty(i)
			if err != nil {
				return nil, err
			}

			if empty {
				break
			}
		}

		reverseSlice(blks)
		return blks, nil
	}

	for i := 0; i < numBlocks; i++ {
		empty, err := fetchAndCheckEmpty(i)
		if err != nil {
			return nil, err
		}

		if empty {
			break
		}
	}

	return blks, nil
}

// reverseSlice reverses a slice
func reverseSlice(blocks []block.UnconsolidatedBlock) {
	for i, j := 0, len(blocks)-1; i < j; i, j = i+1, j-1 {
		blocks[i], blocks[j] = blocks[j], blocks[i]
	}
}

// MarkProcessed is used to mark a block as processed
func (c *blockCache) markProcessed(keys []time.Time) {
	c.mu.Lock()
	for _, key := range keys {
		index, err := c.index(key)
		if err != nil {
			continue
		}

		c.processedBlocks[index] = true
	}

	c.mu.Unlock()
}

// Processed returns all processed block times from the cache
func (c *blockCache) processed() []bool {
	c.mu.Lock()
	processedBlocks := make([]bool, len(c.processedBlocks))
	copy(processedBlocks, c.processedBlocks)

	c.mu.Unlock()
	return processedBlocks
}
