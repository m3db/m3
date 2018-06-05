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
	"sort"
	"time"

	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/x/xio"
	"github.com/m3db/m3x/ident"
)

const (
	initBlockReplicaLength = 10
)

// blockReplica contains the replicas for a single m3db block
type blockReplica struct {
	start     time.Time
	blockSize time.Duration
	replicas  []encoding.MultiReaderIterator
}

type blockReplicas []blockReplica

func (b blockReplicas) Len() int {
	return len(b)
}

func (b blockReplicas) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b blockReplicas) Less(i, j int) bool {
	return b[i].start.Before(b[j].start)
}

// ConvertM3DBSeriesIterators converts m3db SeriesIterators to SeriesBlocks
// which are used to construct Blocks for query processing.
func ConvertM3DBSeriesIterators(iterators encoding.SeriesIterators, iterAlloc encoding.ReaderIteratorAllocate) ([]SeriesBlocks, error) {
	defer iterators.Close()
	multiSeriesBlocks := make([]SeriesBlocks, iterators.Len())

	for i, seriesIterator := range iterators.Iters() {
		blockReplicas, err := blockReplicasFromSeriesIterator(seriesIterator, iterAlloc)
		if err != nil {
			return nil, err
		}

		series, err := seriesBlocksFromBlockReplicas(blockReplicas, seriesIterator)
		if err != nil {
			return nil, err
		}

		multiSeriesBlocks[i] = series
	}

	return multiSeriesBlocks, nil
}

func blockReplicasFromSeriesIterator(seriesIterator encoding.SeriesIterator, iterAlloc encoding.ReaderIteratorAllocate) ([]blockReplica, error) {
	blockReplicas := make(blockReplicas, 0, initBlockReplicaLength)
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
			// todo(braskin): pooling
			iter := encoding.NewMultiReaderIterator(iterAlloc, nil)
			iter.Reset(readers, start, bs)

			inserted := false
			for i := range blockReplicas {
				if blockReplicas[i].start.Equal(start) {
					inserted = true
					blockReplicas[i].replicas = append(blockReplicas[i].replicas, iter)
					break
				}
			}
			if !inserted {
				blockReplicas = append(blockReplicas, blockReplica{
					start:     start,
					blockSize: bs,
					replicas:  []encoding.MultiReaderIterator{iter},
				})
			}
		}
	}

	// sort block replicas by start time
	sort.Sort(blockReplicas)

	return blockReplicas, nil
}

func seriesBlocksFromBlockReplicas(blockReplicas []blockReplica, seriesIterator encoding.SeriesIterator) (SeriesBlocks, error) {
	// NB(braskin): we need to clone the ID, namespace, and tags since we close the series iterator
	var (
		// todo(braskin): use ident pool
		clonedID        = ident.StringID(seriesIterator.ID().String())
		clonedNamespace = ident.StringID(seriesIterator.Namespace().String())
	)

	clonedTags, err := cloneTagIterator(seriesIterator.Tags())
	if err != nil {
		return SeriesBlocks{}, err
	}

	series := SeriesBlocks{
		ID:        clonedID,
		Namespace: clonedNamespace,
		Tags:      clonedTags,
		Blocks:    make([]SeriesBlock, len(blockReplicas)),
	}

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

		// todo(braskin): pooling
		// NB(braskin): we should be careful when directly accessing the series iterators.
		// Instead, we should access them through the SeriesBlock.
		valuesIter := encoding.NewSeriesIterator(clonedID, clonedNamespace,
			clonedTags.Duplicate(), filterValuesStart, filterValuesEnd, block.replicas, nil)

		series.Blocks[i] = SeriesBlock{
			start:          filterValuesStart,
			end:            filterValuesEnd,
			seriesIterator: valuesIter,
		}
	}

	return series, nil
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
