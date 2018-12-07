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

package m3db

import (
	"fmt"
	"sort"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3x/ident"
)

const (
	initBlockReplicaLength = 10
)

// blockReplica contains the replicas for a single m3db block
type seriesBlock struct {
	start     time.Time
	blockSize time.Duration
	replicas  []encoding.MultiReaderIterator
}

type seriesBlocks []seriesBlock

func (b seriesBlocks) Len() int {
	return len(b)
}

func (b seriesBlocks) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b seriesBlocks) Less(i, j int) bool {
	return b[i].start.Before(b[j].start)
}

// ConvertM3DBSeriesIterators converts series iterators to iterator blocks
func ConvertM3DBSeriesIterators(
	iterators encoding.SeriesIterators,
	tagOptions models.TagOptions,
	bounds models.Bounds,
) ([]block.Block, error) {
	bl, err := NewEncodedBlock(
		iterators.Iters(),
		bounds,
		tagOptions,
		true,
	)

	if err != nil {
		return nil, err
	}

	return []block.Block{bl}, nil
}

// ConvertM3DBSegmentedBlockIterators converts series iterators to a list of blocks
func ConvertM3DBSegmentedBlockIterators(
	iterators encoding.SeriesIterators,
	iterAlloc encoding.ReaderIteratorAllocate,
	tagOptions models.TagOptions,
	bounds *models.Bounds,
	pools encoding.IteratorPools,
) ([]block.Block, error) {
	return nil, fmt.Errorf("WIP: splitting blocks will require lookback propagation")
	defer iterators.Close()
	// TODO bounds here should be bounds for this block only not for everything!
	blockBuilder := newEncodedBlockBuilder(tagOptions, TakeLast)
	for _, seriesIterator := range iterators.Iters() {
		blockReplicas, err := blockReplicasFromSeriesIterator(seriesIterator, iterAlloc, bounds, pools)
		if err != nil {
			return nil, err
		}

		err = seriesBlocksFromBlockReplicas(blockBuilder, blockReplicas, bounds.StepSize, seriesIterator, pools)
		if err != nil {
			return nil, err
		}
	}

	return blockBuilder.build(), nil
}

func blockReplicasFromSeriesIterator(
	seriesIterator encoding.SeriesIterator,
	iterAlloc encoding.ReaderIteratorAllocate,
	bounds *models.Bounds,
	pools encoding.IteratorPools,
) (seriesBlocks, error) {
	blocks := make(seriesBlocks, 0, bounds.Steps())
	for _, replica := range seriesIterator.Replicas() {
		perBlockSliceReaders := replica.Readers()
		for next := true; next; next = perBlockSliceReaders.Next() {
			l, start, bs := perBlockSliceReaders.CurrentReaders()
			readers := make([]xio.SegmentReader, l)
			for i := 0; i < l; i++ {
				reader := perBlockSliceReaders.CurrentReaderAt(i)
				// NB(braskin): important to clone the reader as we need its position reset before
				// we use the contents of it again
				clonedReader, err := reader.Clone()
				if err != nil {
					return nil, err
				}

				readers[i] = clonedReader
			}

			iter := encoding.NewMultiReaderIterator(iterAlloc, nil)
			iter.Reset(readers, start, bs)
			inserted := false
			for _, bl := range blocks {
				if bl.start.Equal(start) {
					inserted = true
					bl.replicas = append(bl.replicas, iter)
					break
				}
			}

			if !inserted {
				blocks = append(blocks, seriesBlock{
					start:     start,
					blockSize: bs,
					replicas:  []encoding.MultiReaderIterator{iter},
				})
			}
		}
	}

	// sort series blocks by start time
	sort.Sort(blocks)
	return blocks, nil
}

func seriesBlocksFromBlockReplicas(
	blockBuilder *encodedBlockBuilder,
	blockReplicas seriesBlocks,
	stepSize time.Duration,
	seriesIterator encoding.SeriesIterator,
	pools encoding.IteratorPools,
) error {
	// NB(braskin): we need to clone the ID, namespace, and tags since we close the series iterator
	var (
		// todo(braskin): use ident pool
		clonedID        = ident.StringID(seriesIterator.ID().String())
		clonedNamespace = ident.StringID(seriesIterator.Namespace().String())
	)

	clonedTags, err := cloneTagIterator(seriesIterator.Tags())
	if err != nil {
		return err
	}

	replicaLength := len(blockReplicas) - 1
	// TODO: use pooling
	for i, block := range blockReplicas {
		filterValuesStart := seriesIterator.Start()
		if block.start.After(filterValuesStart) {
			filterValuesStart = block.start
		}

		end := block.start.Add(block.blockSize)
		filterValuesEnd := seriesIterator.End()
		if end.Before(filterValuesEnd) {
			filterValuesEnd = end
		}

		iter := encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{
			ID:             clonedID,
			Namespace:      clonedNamespace,
			Tags:           clonedTags.Duplicate(),
			StartInclusive: filterValuesStart,
			EndExclusive:   filterValuesEnd,
			Replicas:       block.replicas,
		}, nil)

		// NB(braskin): we should be careful when directly accessing the series iterators.
		// Instead, we should access them through the SeriesBlock.
		isLastBlock := i == replicaLength
		blockBuilder.add(
			models.Bounds{
				Start:    filterValuesStart,
				Duration: block.blockSize,
				StepSize: stepSize,
			},
			iter,
			isLastBlock,
		)
	}

	return nil
}

func cloneTagIterator(tagIter ident.TagIterator) (ident.TagIterator, error) {
	tags := ident.NewTags()
	dupeIter := tagIter.Duplicate()
	for dupeIter.Next() {
		tag := dupeIter.Current()
		tags.Append(ident.Tag{
			Name:  ident.BytesID(tag.Name.Bytes()),
			Value: ident.BytesID(tag.Value.Bytes()),
		})
	}

	err := dupeIter.Err()
	if err != nil {
		return nil, err
	}

	return ident.NewTagsIterator(tags), nil
}
