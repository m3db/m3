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
	"time"

	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/x/xio"
)

// BlockReplica contains the replicas for a single m3db block
type BlockReplica struct {
	Start     time.Time
	BlockSize time.Duration
	Replicas  []encoding.MultiReaderIterator
}

// ConvertM3DBSeriesIterators takes in series iterators from m3db and returns
// coordinator SeriesBlocks which are used to construct Blocks for query processing.
func ConvertM3DBSeriesIterators(iterators encoding.SeriesIterators, iterAlloc encoding.ReaderIteratorAllocate) ([]SeriesBlocks, error) {
	multiSeriesBlocks := make([]SeriesBlocks, iterators.Len())

	for i, seriesIterator := range iterators.Iters() {

		blockReplicas, err := blockReplicasFromSeriesIterator(seriesIterator, iterAlloc)
		if err != nil {
			return []SeriesBlocks{}, err
		}

		series := seriesBlocksFromBlockReplicas(blockReplicas, seriesIterator)
		multiSeriesBlocks[i] = series
	}

	return multiSeriesBlocks, nil
}

func blockReplicasFromSeriesIterator(seriesIterator encoding.SeriesIterator, iterAlloc encoding.ReaderIteratorAllocate) ([]BlockReplica, error) {
	var blockReplicas []BlockReplica
	for _, replica := range seriesIterator.Replicas() {
		perBlockSliceReaders := replica.Readers()
		next := true
		for next {
			l, start, bs := perBlockSliceReaders.CurrentReaders()
			var readers []xio.SegmentReader
			for i := 0; i < l; i++ {
				reader := perBlockSliceReaders.CurrentReaderAt(i)
				// import to clone the reader as we need its position reset before
				// we use the contents of it again
				clonedReader, err := reader.Clone()
				if err != nil {
					return []BlockReplica{}, err
				}
				readers = append(readers, clonedReader)
			}
			// todo(braskin): pooling
			iter := encoding.NewMultiReaderIterator(iterAlloc, nil)
			iter.Reset(readers, start, bs)

			inserted := false
			for i := range blockReplicas {
				if blockReplicas[i].Start.Equal(start) {
					inserted = true
					blockReplicas[i].Replicas = append(blockReplicas[i].Replicas, iter)
					break
				}
			}
			if !inserted {
				blockReplicas = append(blockReplicas, BlockReplica{
					Start:     start,
					BlockSize: bs,
					Replicas:  []encoding.MultiReaderIterator{iter},
				})
			}

			next = perBlockSliceReaders.Next()
		}
	}

	return blockReplicas, nil
}

func seriesBlocksFromBlockReplicas(blockReplicas []BlockReplica, seriesIterator encoding.SeriesIterator) SeriesBlocks {
	series := SeriesBlocks{
		ID:        seriesIterator.ID(),
		Namespace: seriesIterator.Namespace(),
	}

	for _, block := range blockReplicas {
		filterValuesStart := seriesIterator.Start()
		if block.Start.After(filterValuesStart) {
			filterValuesStart = block.Start
		}

		end := block.Start.Add(block.BlockSize)

		filterValuesEnd := seriesIterator.End()
		if end.Before(filterValuesEnd) {
			filterValuesEnd = end
		}

		// todo(braskin): pooling
		valuesIter := encoding.NewSeriesIterator(seriesIterator.ID(), seriesIterator.Namespace(),
			seriesIterator.Tags(), filterValuesStart, filterValuesEnd, block.Replicas, nil)

		series.Blocks = append(series.Blocks, SeriesBlock{
			Start:          filterValuesStart,
			End:            filterValuesEnd,
			SeriesIterator: valuesIter,
		})
	}

	return series
}
