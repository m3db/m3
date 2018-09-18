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

	"go.uber.org/zap"
)

var emptyOp = baseOp{}

// baseOp stores required properties for logical operations
type baseOp struct {
	operatorType string
	duration     time.Duration
	processorFn  MakeProcessor
	aggFunc      aggFunc
}

// skipping lint check for a single operator type since we will be adding more
// nolint : unparam
func newBaseOp(args []interface{}, operatorType string, processorFn MakeProcessor, aggFunc aggFunc) (baseOp, error) {
	if len(args) != 1 {
		return emptyOp, fmt.Errorf("invalid number of args for %s: %d", operatorType, len(args))
	}

	duration, ok := args[0].(time.Duration)
	if !ok {
		return emptyOp, fmt.Errorf("unable to cast to scalar argument: %v for %s", args[0], operatorType)
	}

	return baseOp{
		operatorType: operatorType,
		processorFn:  processorFn,
		duration:     duration,
		aggFunc:      aggFunc,
	}, nil
}

// OpType for the operator
func (o baseOp) OpType() string {
	return o.operatorType
}

// String representation
func (o baseOp) String() string {
	return fmt.Sprintf("type: %s, duration: %v", o.OpType(), o.duration)
}

// Node creates an execution node
func (o baseOp) Node(controller *transform.Controller, opts transform.Options) transform.OpNode {
	return &baseNode{
		controller:    controller,
		cache:         newBlockCache(o, opts),
		op:            o,
		processor:     o.processorFn(o, controller, opts),
		transformOpts: opts,
	}
}

// baseNode is an execution node
type baseNode struct {
	op            baseOp
	controller    *transform.Controller
	cache         *blockCache
	processor     Processor
	transformOpts transform.Options
}

// Process processes a block. The processing steps are as follows:
// 1. Figure out the maximum blocks needed for the temporal function
// 2. For the current block, figure out whether we have enough previous blocks which can help process it
// 3. For the blocks after current block, figure out which can be processed right now
// 4. Process all valid blocks from #3, #4 and mark them as processed
// 5. Run a sweep phase to free up blocks which are no longer needed to be cached
// TODO: Figure out if something else needs to be locked
func (c *baseNode) Process(ID parser.NodeID, b block.Block) error {
	unconsolidatedBlock, err := b.Unconsolidated()
	if err != nil {
		return err
	}

	if unconsolidatedBlock == nil {
		return fmt.Errorf("block needs to be unconsolidated for the op: %s", c.op)
	}

	iter, err := unconsolidatedBlock.StepIter()
	if err != nil {
		return err
	}

	meta := iter.Meta()
	bounds := meta.Bounds
	queryStartBounds := bounds.Nearest(c.transformOpts.TimeSpec.Start)
	if bounds.Duration == 0 {
		return fmt.Errorf("bound duration cannot be 0, bounds: %v", bounds)
	}

	if bounds.Start.Before(queryStartBounds.Start) {
		return fmt.Errorf("block start cannot be before query start, bounds: %v, queryStart: %v", bounds, queryStartBounds)
	}

	queryEndBounds := bounds.Nearest(c.transformOpts.TimeSpec.End.Add(-1 * bounds.StepSize))
	if bounds.Start.After(queryEndBounds.Start) {
		return fmt.Errorf("block start cannot be after query end, bounds: %v, query end: %v", bounds, queryEndBounds)
	}

	c.cache.init(bounds)
	blockDuration := bounds.Duration
	// Figure out the maximum blocks needed for the temporal function
	maxBlocks := int(math.Ceil(float64(c.op.duration) / float64(blockDuration)))

	// Figure out the leftmost block
	leftRangeStart := bounds.Previous(maxBlocks)

	if leftRangeStart.Start.Before(queryStartBounds.Start) {
		leftRangeStart = queryStartBounds
	}

	// Figure out the rightmost blocks
	rightRangeStart := bounds.Next(maxBlocks)

	if rightRangeStart.Start.After(queryEndBounds.Start) {
		rightRangeStart = queryEndBounds
	}

	// Process the current block by figuring out the left range
	leftBlks, emptyLeftBlocks, err := c.processCurrent(bounds, leftRangeStart)
	if err != nil {
		return err
	}

	processRequests := make([]processRequest, 0, len(leftBlks))
	// If we have all blocks for the left range in the cache, then process the current block
	if !emptyLeftBlocks {
		processRequests = append(processRequests, processRequest{blk: unconsolidatedBlock, deps: leftBlks, bounds: bounds})
	}

	leftBlks = append(leftBlks, unconsolidatedBlock)

	// Process right side of the range
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
		processRequests = append(processRequests, processRequest{blk: rightBlks[i], deps: deps, bounds: bounds.Next(i + 1)})
	}

	// If either the left range or right range wasn't fully processed then cache the current block
	if emptyLeftBlocks || emptyRightBlocks {
		if err := c.cache.add(bounds.Start, unconsolidatedBlock); err != nil {
			return err
		}
	}

	return c.processCompletedBlocks(processRequests, maxBlocks)
}

// processCurrent processes the current block. For the current block, figure out whether we have enough previous blocks which can help process it
func (c *baseNode) processCurrent(bounds models.Bounds, leftRangeStart models.Bounds) ([]block.UnconsolidatedBlock, bool, error) {
	numBlocks := bounds.Blocks(leftRangeStart.Start)
	leftBlks, err := c.cache.multiGet(leftRangeStart, numBlocks, true)
	if err != nil {
		return nil, false, err
	}
	return leftBlks, len(leftBlks) != numBlocks, nil
}

// processRight processes blocks after current block. This is done by fetching all contiguous right blocks until the right range
func (c *baseNode) processRight(bounds models.Bounds, rightRangeStart models.Bounds) ([]block.UnconsolidatedBlock, bool, error) {
	numBlocks := rightRangeStart.Blocks(bounds.Start)
	rightBlks, err := c.cache.multiGet(bounds.Next(1), numBlocks, false)
	if err != nil {
		return nil, false, err
	}

	return rightBlks, len(rightBlks) != numBlocks, nil
}

// processCompletedBlocks processes all blocks for which all dependent blocks are present
func (c *baseNode) processCompletedBlocks(processRequests []processRequest, maxBlocks int) error {
	processedKeys := make([]time.Time, len(processRequests))
	for i, req := range processRequests {
		if err := c.processSingleRequest(req); err != nil {
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

func (c *baseNode) processSingleRequest(request processRequest) error {
	seriesIter, err := request.blk.SeriesIter()
	if err != nil {
		return err
	}

	depIters := make([]block.UnconsolidatedSeriesIter, len(request.deps))
	for i, blk := range request.deps {
		iter, err := blk.SeriesIter()
		if err != nil {
			return err
		}

		depIters[i] = iter
	}

	bounds := seriesIter.Meta().Bounds

	seriesMeta := seriesIter.SeriesMeta()
	resultSeriesMeta := make([]block.SeriesMeta, len(seriesMeta))
	for i, m := range seriesMeta {
		tags := m.Tags.WithoutName()
		resultSeriesMeta[i].Tags = tags
		resultSeriesMeta[i].Name = tags.ID()
	}

	builder, err := c.controller.BlockBuilder(seriesIter.Meta(), resultSeriesMeta)
	if err != nil {
		return err
	}

	if err := builder.AddCols(bounds.Steps()); err != nil {
		return err
	}

	aggDuration := c.op.duration
	steps := int((aggDuration + bounds.Duration) / bounds.StepSize)
	values := make([]ts.Datapoints, 0, steps)
	desiredLength := int(math.Ceil(float64(aggDuration) / float64(bounds.StepSize)))
	for seriesIter.Next() {
		values = values[:0]
		for i, iter := range depIters {
			if !iter.Next() {
				return fmt.Errorf("incorrect number of series for block: %d", i)
			}

			s, err := iter.Current()
			if err != nil {
				return err
			}

			values = append(values, s.Datapoints()...)
		}

		series, err := seriesIter.Current()
		if err != nil {
			return err
		}

		for i := 0; i < series.Len(); i++ {
			val := series.DatapointsAtStep(i)
			values = append(values, val)
			newVal := math.NaN()
			alignedTime, _ := bounds.TimeForIndex(i)
			oldestDatapointTimestamp := alignedTime.Add(-1 * aggDuration)
			// Remove the older values from slice as newer values are pushed in.
			// TODO: Consider using a rotating slice since this is inefficient
			if desiredLength <= len(values) {
				values = values[len(values)-desiredLength:]
				flattenedValues := make(ts.Datapoints, 0)
				for _, dps := range values {
					for _, dp := range dps {
						if dp.Timestamp.Before(oldestDatapointTimestamp) {
							continue
						}

						flattenedValues = append(flattenedValues, dp)
					}
				}

				newVal = c.processor.Process(flattenedValues)
			}

			builder.AppendValue(i, newVal)
		}
	}

	nextBlock := builder.Build()
	defer nextBlock.Close()
	return c.controller.Process(nextBlock)
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
				logging.WithContext(context.TODO()).Warn("unable to remove key from cache", zap.Int("index", i))
			}
		}

		prevProcessed++
	}
}

// Processor is implemented by the underlying transforms
type Processor interface {
	Process(values ts.Datapoints) float64
}

// MakeProcessor is a way to create a transform
type MakeProcessor func(op baseOp, controller *transform.Controller, opts transform.Options) Processor

type processRequest struct {
	blk    block.UnconsolidatedBlock
	bounds models.Bounds
	deps   []block.UnconsolidatedBlock
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

func (c *blockCache) init(bounds models.Bounds) {
	if c.initialized {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return
	}

	timeSpec := c.transformOpts.TimeSpec
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
