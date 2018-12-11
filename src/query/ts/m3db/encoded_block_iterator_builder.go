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
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
)

type encodedBlockBuilder struct {
	blocksAtTime    map[string]encodedBlock
	tagOptions      models.TagOptions
	consolidationFn consolidators.ConsolidationFunc
}

func newEncodedBlockBuilder(
	tagOptions models.TagOptions,
	consolidationFn consolidators.ConsolidationFunc,
) *encodedBlockBuilder {
	return &encodedBlockBuilder{
		blocksAtTime:    make(map[string]encodedBlock, 10),
		tagOptions:      tagOptions,
		consolidationFn: consolidationFn,
	}
}

func (b *encodedBlockBuilder) add(
	bounds models.Bounds,
	iter encoding.SeriesIterator,
	lastBlock bool,
) {
	start := bounds.Start
	consolidation := consolidationSettings{
		consolidationFn: consolidators.TakeLast,
		currentTime:     start,
		bounds:          bounds,
	}

	str := start.String()
	if _, found := b.blocksAtTime[str]; !found {
		// Add a new encoded block
		b.blocksAtTime[str] = newEncodedBlock(
			[]encoding.SeriesIterator{},
			b.tagOptions,
			consolidation,
			lastBlock,
		)
	}

	block := b.blocksAtTime[str]
	block.seriesBlockIterators = append(block.seriesBlockIterators, iter)
	b.blocksAtTime[str] = block
}

func (b *encodedBlockBuilder) build() []block.Block {
	blocks := make([]block.Block, 0, len(b.blocksAtTime))
	for _, block := range b.blocksAtTime {
		block.generateMetas()
		blocks = append(blocks, &block)
	}

	return blocks
}
