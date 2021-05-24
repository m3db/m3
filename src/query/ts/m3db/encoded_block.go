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
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	tsconsolidators "github.com/m3db/m3/src/query/ts/m3db/consolidators"
	xtime "github.com/m3db/m3/src/x/time"
)

type consolidationSettings struct {
	consolidationFn tsconsolidators.ConsolidationFunc
	currentTime     xtime.UnixNano
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
}

// NewEncodedBlock builds an encoded block.
func NewEncodedBlock(
	result consolidators.SeriesFetchResult,
	bounds models.Bounds,
	lastBlock bool,
	opts Options,
) (block.Block, error) {
	if err := result.Verify(); err != nil {
		return nil, err
	}

	consolidation := consolidationSettings{
		consolidationFn: tsconsolidators.TakeLast,
		currentTime:     bounds.Start,
		bounds:          bounds,
	}

	bl, err := newEncodedBlock(
		result,
		consolidation,
		lastBlock,
		opts,
	)

	if err != nil {
		return nil, err
	}

	return bl, nil
}

func newEncodedBlock(
	result consolidators.SeriesFetchResult,
	consolidation consolidationSettings,
	lastBlock bool,
	options Options,
) (*encodedBlock, error) {
	count := result.Count()
	seriesMetas := make([]block.SeriesMeta, 0, count)
	for i := 0; i < count; i++ {
		iter, tags, err := result.IterTagsAtIndex(i, options.TagOptions())
		if err != nil {
			return nil, err
		}

		seriesMetas = append(seriesMetas, block.SeriesMeta{
			Name: iter.ID().Bytes(),
			Tags: tags,
		})
	}

	return &encodedBlock{
		seriesBlockIterators: result.SeriesIterators(),
		consolidation:        consolidation,
		lastBlock:            lastBlock,
		resultMeta:           result.Metadata,
		options:              options,
		seriesMetas:          seriesMetas,
		meta: block.Metadata{
			Tags:           models.NewTags(0, options.TagOptions()),
			Bounds:         consolidation.bounds,
			ResultMetadata: result.Metadata,
		},
	}, nil
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

func (b *encodedBlock) Info() block.BlockInfo {
	return block.NewBlockInfo(block.BlockM3TSZCompressed)
}
