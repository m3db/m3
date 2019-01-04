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
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
)

type encodedBlockUnconsolidated struct {
	// There is slightly different execution for the last block in the series
	lastBlock            bool
	meta                 block.Metadata
	tagOptions           models.TagOptions
	consolidation        consolidationSettings
	seriesMetas          []block.SeriesMeta
	seriesBlockIterators []encoding.SeriesIterator
}

func (b *encodedBlockUnconsolidated) Consolidate() (block.Block, error) {
	return &encodedBlock{
		lastBlock:            b.lastBlock,
		meta:                 b.meta,
		tagOptions:           b.tagOptions,
		consolidation:        b.consolidation,
		seriesMetas:          b.seriesMetas,
		seriesBlockIterators: b.seriesBlockIterators,
	}, nil
}

func (b *encodedBlockUnconsolidated) Close() error {
	for _, bl := range b.seriesBlockIterators {
		bl.Close()
	}

	return nil
}

func (b *encodedBlockUnconsolidated) WithMetadata(
	meta block.Metadata,
	seriesMetas []block.SeriesMeta,
) (block.UnconsolidatedBlock, error) {
	return &encodedBlockUnconsolidated{
		lastBlock:            b.lastBlock,
		tagOptions:           b.tagOptions,
		consolidation:        b.consolidation,
		seriesBlockIterators: b.seriesBlockIterators,
		meta:                 meta,
		seriesMetas:          seriesMetas,
	}, nil
}

func (b *encodedBlockUnconsolidated) StepIter() (
	block.UnconsolidatedStepIter,
	error,
) {
	return &encodedStepIterUnconsolidated{
		meta:        b.meta,
		seriesMeta:  b.seriesMetas,
		seriesIters: b.seriesBlockIterators,
		lastBlock:   b.lastBlock,
		validIters:  make([]bool, len(b.seriesBlockIterators)),
	}, nil
}

func (b *encodedBlockUnconsolidated) SeriesIter() (
	block.UnconsolidatedSeriesIter,
	error,
) {
	return &encodedSeriesIterUnconsolidated{
		idx:         -1,
		meta:        b.meta,
		seriesMeta:  b.seriesMetas,
		seriesIters: b.seriesBlockIterators,
	}, nil
}
