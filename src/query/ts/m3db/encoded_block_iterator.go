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

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
)

type consolidationSettings struct {
	consolidationFn ConsolidationFunc
	currentTime     time.Time
	bounds          *models.Bounds
}

type encodedBlock struct {
	lastBlock            bool
	meta                 block.Metadata
	tagOptions           models.TagOptions
	consolidation        *consolidationSettings
	seriesMetas          []block.SeriesMeta
	seriesBlockIterators []encoding.SeriesIterator
}

// verify encodedSeriesIter satisfies both block and unconsolidated block
var (
	_ block.Block               = (*encodedBlock)(nil)
	_ block.UnconsolidatedBlock = (*encodedBlock)(nil)
)

// MakeEncodedBlock builds an encoded block
func MakeEncodedBlock(
	seriesBlockIterators []encoding.SeriesIterator,
	bounds *models.Bounds,
	tagOptions models.TagOptions,
	lastBlock bool,
) block.MultipurposeBlock {
	consolidation := &consolidationSettings{
		consolidationFn: TakeLast,
		currentTime:     bounds.Start,
		bounds:          bounds,
	}

	bl := makeEncodedBlock(
		seriesBlockIterators,
		tagOptions,
		consolidation,
		lastBlock,
		true,
	)
	return &bl
}

func makeEncodedBlock(
	seriesBlockIterators []encoding.SeriesIterator,
	tagOptions models.TagOptions,
	consolidation *consolidationSettings,
	lastBlock bool,
	generateMetas bool,
) encodedBlock {
	b := encodedBlock{
		seriesBlockIterators: seriesBlockIterators,
		tagOptions:           tagOptions,
		consolidation:        consolidation,
		lastBlock:            lastBlock,
	}

	if generateMetas {
		// generate metadata and series metadatas
		b.generateMetas()
	}

	return b
}

func (b *encodedBlock) Unconsolidated() (block.UnconsolidatedBlock, error) {
	// NB: the encodedBlock should satisfy both consolidated and unconsolidated
	// block interfaces
	return b, nil
}

func (b *encodedBlock) Consolidate() (block.Block, error) {
	// NB: the encodedBlock should satisfy both consolidated and unconsolidated
	// block interfaces
	return b, nil
}

func (b *encodedBlock) Close() error {
	for _, bl := range b.seriesBlockIterators {
		bl.Close()
	}

	return nil
}

func (b *encodedBlock) buildSeriesMeta() error {
	b.seriesMetas = make([]block.SeriesMeta, len(b.seriesBlockIterators))
	for i, iter := range b.seriesBlockIterators {
		tags, err := storage.FromIdentTagIteratorToTags(iter.Tags(), b.tagOptions)
		if err != nil {
			return err
		}

		b.seriesMetas[i] = block.SeriesMeta{
			Name: iter.ID().String(),
			Tags: tags,
		}
	}

	return nil
}

func (b *encodedBlock) buildMeta() {
	tags, metas := utils.DedupeMetadata(b.seriesMetas)
	b.seriesMetas = metas
	b.meta = block.Metadata{
		Tags: tags,
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

func (b *encodedBlock) withMetadata(
	meta block.Metadata,
	seriesMetas []block.SeriesMeta,
) (*encodedBlock, error) {
	bl := makeEncodedBlock(
		b.seriesBlockIterators,
		b.tagOptions,
		b.consolidation,
		b.lastBlock,
		false,
	)
	bl.meta = meta
	bl.seriesMetas = seriesMetas

	return &bl, nil
}

func (b *encodedBlock) WithMetadata(
	meta block.Metadata,
	seriesMetas []block.SeriesMeta,
) (block.Block, error) {
	return b.withMetadata(meta, seriesMetas)
}

func (b *encodedBlock) WithMetadataUnconsolidated(
	meta block.Metadata,
	seriesMetas []block.SeriesMeta,
) (block.UnconsolidatedBlock, error) {
	return b.withMetadata(meta, seriesMetas)
}

func (b *encodedBlock) StepIter() (block.StepIter, error) {
	return b.stepIter(), nil
}

func (b *encodedBlock) StepIterUnconsolidated() (
	block.UnconsolidatedStepIter,
	error,
) {
	return b.stepIterUnconsolidated(), nil
}

func (b *encodedBlock) SeriesIter() (block.SeriesIter, error) {
	return b.seriesIter(), nil
}

func (b *encodedBlock) SeriesIterUnconsolidated() (
	block.UnconsolidatedSeriesIter,
	error,
) {
	return b.seriesIterUnconsolidated(), nil
}
