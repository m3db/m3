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
	"sync"

	"github.com/m3db/m3/src/query/functions/utils"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
)

type encodedBlock struct {
	once                 sync.Once
	err                  error
	meta                 block.Metadata
	seriesMetas          []block.SeriesMeta
	seriesBlockIterators []encoding.SeriesIterator
}

// MakeEncodedBlock builds an encoded block
func MakeEncodedBlock(seriesBlockIterators []encoding.SeriesIterator) block.Block {
	b := &encodedBlock{
		seriesBlockIterators: seriesBlockIterators,
	}

	// generate metadata and series metadatas
	b.generateMetas()
	return b
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
		tags, err := storage.FromIdentTagIteratorToTags(iter.Tags())
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

func (b *encodedBlock) generateMetas() (block.Metadata, []block.SeriesMeta, error) {
	b.once.Do(func() {
		err := b.buildSeriesMeta()
		if err != nil {
			b.err = err
			return
		}

		b.buildMeta()
	})

	return b.meta, b.seriesMetas, b.err
}

func (b *encodedBlock) StepIter() (block.StepIter, error) {
	meta, seriesMetas, err := b.generateMetas()
	if err != nil {
		return nil, err
	}

	return makeEncodedStepIter(meta, seriesMetas, b.seriesBlockIterators), nil
}

func (b *encodedBlock) SeriesIter() (block.SeriesIter, error) {
	meta, seriesMetas, err := b.generateMetas()
	if err != nil {
		return nil, err
	}

	return makeEncodedSeriesIter(meta, seriesMetas, b.seriesBlockIterators), nil
}
