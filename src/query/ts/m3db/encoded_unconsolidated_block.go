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

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/ts"
)

type encodedBlockUnconsolidated struct {
	// There is slightly different execution for the last block in the series.
	lastBlock            bool
	meta                 block.Metadata
	consolidation        consolidationSettings
	seriesMetas          []block.SeriesMeta
	seriesBlockIterators []encoding.SeriesIterator
	options              Options
}

func (b *encodedBlockUnconsolidated) Consolidate() (block.Block, error) {
	return &encodedBlock{
		lastBlock:            b.lastBlock,
		meta:                 b.meta,
		consolidation:        b.consolidation,
		seriesMetas:          b.seriesMetas,
		seriesBlockIterators: b.seriesBlockIterators,
		options:              b.options,
	}, nil
}

func (b *encodedBlockUnconsolidated) Close() error {
	for _, bl := range b.seriesBlockIterators {
		bl.Close()
	}

	return nil
}

func (b *encodedBlockUnconsolidated) Meta() block.Metadata {
	return b.meta
}

// MultiSeriesIter returns batched series iterators for the block based on
// given concurrency.
func (b *encodedBlockUnconsolidated) MultiSeriesIter(
	concurrency int,
) ([]block.UnconsolidatedSeriesIterBatch, error) {
	if concurrency < 1 {
		return nil, fmt.Errorf("batch size %d must be greater than 0", concurrency)
	}

	var (
		iterCount  = len(b.seriesBlockIterators)
		iters      = make([]block.UnconsolidatedSeriesIterBatch, 0, concurrency)
		chunkSize  = iterCount / concurrency
		remainder  = iterCount % concurrency
		chunkSizes = make([]int, concurrency)
	)

	ts.MemsetInt(chunkSizes, chunkSize)
	for i := 0; i < remainder; i++ {
		chunkSizes[i] = chunkSizes[i] + 1
	}

	start := 0
	for _, chunkSize := range chunkSizes {
		end := start + chunkSize

		if end > iterCount {
			end = iterCount
		}

		iter := &encodedSeriesIterUnconsolidated{
			idx:              -1,
			meta:             b.meta,
			seriesMeta:       b.seriesMetas[start:end],
			seriesIters:      b.seriesBlockIterators[start:end],
			lookbackDuration: b.options.LookbackDuration(),
		}

		iters = append(iters, block.UnconsolidatedSeriesIterBatch{
			Iter: iter,
			Size: end - start,
		})

		start = end
	}

	return iters, nil
}
