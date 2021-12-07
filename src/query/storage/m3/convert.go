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

package m3

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	initBlockReplicaLength = 10
	// This outputs time as 11:12:03AM
	blockTimeFormat = "3:04:05PM"
)

// blockReplica contains the replicas for a single m3db block.
type seriesBlock struct {
	// internal Start time for the block.
	blockStart xtime.UnixNano
	// time at which the first point in the block will appear.
	readStart xtime.UnixNano
	blockSize time.Duration
	replicas  []encoding.MultiReaderIterator
}

type seriesBlocks []seriesBlock

func (b seriesBlock) String() string {
	return fmt.Sprint("BlockSize:", b.blockSize.Hours(), " blockStart:",
		b.blockStart.Format(blockTimeFormat), " readStart:",
		b.readStart.Format(blockTimeFormat), " num replicas", len(b.replicas))
}

func (b seriesBlocks) Len() int {
	return len(b)
}

func (b seriesBlocks) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b seriesBlocks) Less(i, j int) bool {
	return b[i].blockStart.Before(b[j].blockStart)
}

func seriesIteratorsToEncodedBlockIterators(
	result consolidators.SeriesFetchResult,
	bounds models.Bounds,
	opts Options,
) ([]block.Block, error) {
	bl, err := NewEncodedBlock(result, bounds, true, opts)
	if err != nil {
		return nil, err
	}

	return []block.Block{bl}, nil
}

// ConvertM3DBSeriesIterators converts series iterators to iterator blocks. If
// lookback is greater than 0, converts the entire series into a single block,
// otherwise, splits the series into blocks.
func ConvertM3DBSeriesIterators(
	result consolidators.SeriesFetchResult,
	bounds models.Bounds,
	opts Options,
) ([]block.Block, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	if opts.SplittingSeriesByBlock() {
		return convertM3DBSegmentedBlockIterators(result, bounds, opts)
	}

	return seriesIteratorsToEncodedBlockIterators(result, bounds, opts)
}

// convertM3DBSegmentedBlockIterators converts series iterators to a list of blocks
func convertM3DBSegmentedBlockIterators(
	result consolidators.SeriesFetchResult,
	bounds models.Bounds,
	opts Options,
) ([]block.Block, error) {
	defer result.Close()
	blockBuilder := newEncodedBlockBuilder(result, opts)
	var (
		pools        = opts.IteratorPools()
		checkedPools = opts.CheckedBytesPool()
	)

	count := result.Count()
	for i := 0; i < count; i++ {
		iter, tags, err := result.IterTagsAtIndex(i, opts.TagOptions())
		if err != nil {
			return nil, err
		}

		blockReplicas, err := blockReplicasFromSeriesIterator(
			iter,
			bounds,
			pools,
			checkedPools,
		)

		if err != nil {
			return nil, err
		}

		blockReplicas = updateSeriesBlockStarts(
			blockReplicas,
			bounds.StepSize,
			iter.Start(),
		)

		err = seriesBlocksFromBlockReplicas(
			blockBuilder,
			tags,
			result.Metadata,
			blockReplicas,
			bounds.StepSize,
			iter,
			pools,
		)

		if err != nil {
			return nil, err
		}
	}

	return blockBuilder.build()
}

func blockReplicasFromSeriesIterator(
	seriesIterator encoding.SeriesIterator,
	bounds models.Bounds,
	pools encoding.IteratorPools,
	checkedPools pool.CheckedBytesPool,
) (seriesBlocks, error) {
	blocks := make(seriesBlocks, 0, bounds.Steps())
	pool := pools.MultiReaderIterator()

	replicas, err := seriesIterator.Replicas()
	if err != nil {
		return nil, err
	}

	for _, replica := range replicas {
		perBlockSliceReaders := replica.Readers()
		for next := true; next; next = perBlockSliceReaders.Next() {
			l, start, bs := perBlockSliceReaders.CurrentReaders()
			readers := make([]xio.SegmentReader, l)
			for i := 0; i < l; i++ {
				reader := perBlockSliceReaders.CurrentReaderAt(i)
				// NB(braskin): important to clone the reader as we need its position reset before
				// we use the contents of it again
				clonedReader, err := reader.Clone(checkedPools)
				if err != nil {
					return nil, err
				}

				readers[i] = clonedReader
			}

			iter := pool.Get()
			// TODO [haijun] query assumes schemaless iterators.
			iter.Reset(readers, start, bs, nil)
			inserted := false
			for _, bl := range blocks {
				if bl.blockStart == start {
					inserted = true
					bl.replicas = append(bl.replicas, iter)
					break
				}
			}

			if !inserted {
				blocks = append(blocks, seriesBlock{
					blockStart: start,
					blockSize:  bs,
					replicas:   []encoding.MultiReaderIterator{iter},
				})
			}
		}
	}

	// sort series blocks by Start time
	sort.Sort(blocks)
	return blocks, nil
}

func blockDuration(blockSize, stepSize time.Duration) time.Duration {
	numSteps := math.Ceil(float64(blockSize) / float64(stepSize))
	return stepSize * time.Duration(numSteps)
}

// calculates duration required to fill the gap of fillSize in stepSize sized
// increments.
func calculateFillDuration(fillSize, stepSize time.Duration) time.Duration {
	numberToFill := int(fillSize / stepSize)
	if fillSize%stepSize != 0 {
		numberToFill++
	}

	return stepSize * time.Duration(numberToFill)
}

// pads series blocks.
func updateSeriesBlockStarts(
	blocks seriesBlocks,
	stepSize time.Duration,
	iterStart xtime.UnixNano,
) seriesBlocks {
	if len(blocks) == 0 {
		return blocks
	}

	firstStart := blocks[0].blockStart
	if iterStart.Before(firstStart) {
		fillSize := firstStart.Sub(iterStart)
		iterStart = iterStart.Add(calculateFillDuration(fillSize, stepSize))
	}

	// Update read starts for existing blocks.
	for i, bl := range blocks {
		blocks[i].readStart = iterStart
		fillSize := bl.blockStart.Add(bl.blockSize).Sub(iterStart)
		iterStart = iterStart.Add(calculateFillDuration(fillSize, stepSize))
	}

	return blocks
}

func seriesBlocksFromBlockReplicas(
	blockBuilder *encodedBlockBuilder,
	tags models.Tags,
	resultMetadata block.ResultMetadata,
	blockReplicas seriesBlocks,
	stepSize time.Duration,
	seriesIterator encoding.SeriesIterator,
	pools encoding.IteratorPools,
) error {
	// NB: clone ID and Namespace since they must be owned by the series blocks.
	var (
		// todo(braskin): use ident pool
		clonedID        = ident.StringID(seriesIterator.ID().String())
		clonedNamespace = ident.StringID(seriesIterator.Namespace().String())
	)

	replicaLength := len(blockReplicas) - 1
	// TODO: use pooling
	for i, block := range blockReplicas {
		filterValuesStart := seriesIterator.Start()
		if block.blockStart.After(filterValuesStart) {
			filterValuesStart = block.blockStart
		}

		end := block.blockStart.Add(block.blockSize)
		filterValuesEnd := seriesIterator.End()
		if end.Before(filterValuesEnd) {
			filterValuesEnd = end
		}

		iter := encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{
			ID:             clonedID,
			Namespace:      clonedNamespace,
			StartInclusive: filterValuesStart,
			EndExclusive:   filterValuesEnd,
			Replicas:       block.replicas,
		}, nil)

		// NB: if querying a small range, such that blockSize is greater than the
		// iterator duration, use the smaller range instead.
		duration := filterValuesEnd.Sub(filterValuesStart)
		if duration > block.blockSize {
			duration = block.blockSize
		}

		// NB(braskin): we should be careful when directly accessing the series iterators.
		// Instead, we should access them through the SeriesBlock.
		isLastBlock := i == replicaLength
		blockBuilder.add(
			iter,
			tags,
			resultMetadata,
			models.Bounds{
				Start:    block.readStart,
				Duration: duration,
				StepSize: stepSize,
			},
			isLastBlock,
		)
	}

	return nil
}
