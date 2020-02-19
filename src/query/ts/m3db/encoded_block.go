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
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
	"github.com/m3db/m3/src/query/util"
)

type consolidationSettings struct {
	consolidationFn consolidators.ConsolidationFunc
	currentTime     time.Time
	bounds          models.Bounds
}

type encodedBlock struct {
	// There is slightly different execution for the last block in the series.
	lastBlock            bool
	meta                 block.Metadata
	consolidation        consolidationSettings
	seriesMetas          []block.SeriesMeta
	seriesBlockIterators []encoding.SeriesIterator
	options              Options
	resultMeta           block.ResultMetadata
	instrumented         bool
}

// NewEncodedBlock builds an encoded block.
func NewEncodedBlock(
	seriesBlockIterators []encoding.SeriesIterator,
	bounds models.Bounds,
	lastBlock bool,
	resultMeta block.ResultMetadata,
	opts Options,
) (block.Block, error) {
	consolidation := consolidationSettings{
		consolidationFn: consolidators.TakeLast,
		currentTime:     bounds.Start,
		bounds:          bounds,
	}

	bl := newEncodedBlock(
		seriesBlockIterators,
		consolidation,
		lastBlock,
		resultMeta,
		opts,
	)

	if err := bl.generateMetas(); err != nil {
		return nil, err
	}

	return &bl, nil
}

func newEncodedBlock(
	seriesBlockIterators []encoding.SeriesIterator,
	consolidation consolidationSettings,
	lastBlock bool,
	resultMeta block.ResultMetadata,
	options Options,
) encodedBlock {
	return encodedBlock{
		seriesBlockIterators: seriesBlockIterators,
		consolidation:        consolidation,
		lastBlock:            lastBlock,
		resultMeta:           resultMeta,
		options:              options,
		instrumented:         true,
	}
}

func (b *encodedBlock) Close() error {
	for _, bl := range b.seriesBlockIterators {
		bl.Close()
	}

	return nil
}

func (b *encodedBlock) Meta() block.Metadata {
	return b.meta
}

func (b *encodedBlock) buildSeriesMeta() error {
	b.seriesMetas = make([]block.SeriesMeta, len(b.seriesBlockIterators))
	tagOptions := b.options.TagOptions()
	for i, iter := range b.seriesBlockIterators {
		tags, err := storage.FromIdentTagIteratorToTags(iter.Tags(), tagOptions)
		if err != nil {
			return err
		}

		b.seriesMetas[i] = block.SeriesMeta{
			Name: iter.ID().Bytes(),
			Tags: tags,
		}
	}

	return nil
}

func (b *encodedBlock) buildMeta() {
	b.meta = block.Metadata{
		Tags:           models.NewTags(0, b.options.TagOptions()),
		Bounds:         b.consolidation.bounds,
		ResultMetadata: b.resultMeta,
	}
}

func (b *encodedBlock) generateMetas() error {
	err := b.buildSeriesMeta()
	if err != nil {
		return err
	}

	b.buildMeta()
	return nil
}

func (b *encodedBlock) Info() block.BlockInfo {
	return block.NewBlockInfo(block.BlockM3TSZCompressed)
}

// MultiSeriesIter returns batched series iterators for the block based on
// given concurrency.
func (b *encodedBlock) MultiSeriesIter(
	concurrency int,
) ([]block.SeriesIterBatch, error) {
	if concurrency < 1 {
		return nil, fmt.Errorf("batch size %d must be greater than 0", concurrency)
	}

	var (
		iterCount  = len(b.seriesBlockIterators)
		iters      = make([]block.SeriesIterBatch, 0, concurrency)
		chunkSize  = iterCount / concurrency
		remainder  = iterCount % concurrency
		chunkSizes = make([]int, concurrency)
	)

	util.MemsetInt(chunkSizes, chunkSize)
	for i := 0; i < remainder; i++ {
		chunkSizes[i] = chunkSizes[i] + 1
	}

	start := 0
	for _, chunkSize := range chunkSizes {
		end := start + chunkSize

		if end > iterCount {
			end = iterCount
		}

		iter := &encodedSeriesIter{
			idx:              -1,
			meta:             b.meta,
			seriesMeta:       b.seriesMetas[start:end],
			seriesIters:      b.seriesBlockIterators[start:end],
			lookbackDuration: b.options.LookbackDuration(),
		}

		iters = append(iters, block.SeriesIterBatch{
			Iter: iter,
			Size: end - start,
		})

		start = end
	}

	return iters, nil
}
