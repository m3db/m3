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
	"bytes"
	"sort"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

const initBlockLength = 10

type blockAtTime struct {
	time  xtime.UnixNano
	block encodedBlock
}

type blocksAtTime []blockAtTime

type encodedBlockBuilder struct {
	resultMeta   block.ResultMetadata
	blocksAtTime blocksAtTime
	options      Options
}

func newEncodedBlockBuilder(
	result consolidators.SeriesFetchResult,
	options Options,
) *encodedBlockBuilder {
	return &encodedBlockBuilder{
		resultMeta:   result.Metadata,
		blocksAtTime: make(blocksAtTime, 0, initBlockLength),
		options:      options,
	}
}

func (b *encodedBlockBuilder) add(
	iter encoding.SeriesIterator,
	tags models.Tags,
	meta block.ResultMetadata,
	bounds models.Bounds,
	lastBlock bool,
) error {
	start := bounds.Start
	consolidation := consolidationSettings{
		consolidationFn: b.options.ConsolidationFunc(),
		currentTime:     start,
		bounds:          bounds,
	}

	seriesMeta := block.SeriesMeta{
		Name: iter.ID().Bytes(),
		Tags: tags,
	}

	for idx, bl := range b.blocksAtTime {
		if bl.time.Equal(start) {
			block := bl.block
			block.seriesBlockIterators = append(block.seriesBlockIterators, iter)
			block.seriesMetas = append(block.seriesMetas, seriesMeta)
			b.blocksAtTime[idx].block = block
			return nil
		}
	}

	bl, err := newEncodedBlock(
		consolidators.NewEmptyFetchResult(meta),
		consolidation,
		lastBlock,
		b.options,
	)

	if err != nil {
		return err
	}

	bl.seriesBlockIterators = append(bl.seriesBlockIterators, iter)
	bl.seriesMetas = append(bl.seriesMetas, seriesMeta)
	b.blocksAtTime = append(b.blocksAtTime, blockAtTime{
		time:  start,
		block: *bl,
	})

	return nil
}

func (b *encodedBlockBuilder) build() ([]block.Block, error) {
	if err := b.backfillMissing(); err != nil {
		return nil, err
	}

	// Sort blocks by ID.
	for _, bl := range b.blocksAtTime {
		sort.Sort(seriesIteratorByID(bl.block.seriesBlockIterators))
	}

	blocks := make([]block.Block, 0, len(b.blocksAtTime))
	for _, bl := range b.blocksAtTime {
		block := bl.block
		blocks = append(blocks, &block)
	}

	return blocks, nil
}

// Since some series can be missing if there are no values in composing blocks,
// need to backfill these, since downstream functions rely on series order.
//
// TODO: this will be removed after https://github.com/m3db/m3/issues/1281 is
// completed, which will allow temporal functions, and final read accumulator
// to empty series, and do its own matching rather than relying on matching
// series order. This is functionally throwaway code and should be regarded
// as such.
func (b *encodedBlockBuilder) backfillMissing() error {
	// map series to indeces of b.blocksAtTime at which they are seen
	seenMap := make(map[string]seriesIteratorDetails, initBlockReplicaLength)
	for idx, bl := range b.blocksAtTime {
		block := bl.block
		for i, iter := range block.seriesBlockIterators {
			id := iter.ID().String()
			if seen, found := seenMap[id]; !found {
				seenMap[id] = seriesIteratorDetails{
					start:   iter.Start(),
					end:     iter.End(),
					id:      iter.ID(),
					ns:      iter.Namespace(),
					tags:    block.seriesMetas[i].Tags,
					present: []int{idx},
				}
			} else {
				seen.present = append(seen.present, idx)
				seenMap[id] = seen
			}
		}
	}

	// make sure that each seen series exists in every block.
	blockLen := len(b.blocksAtTime)
	for _, iterDetails := range seenMap {
		present := iterDetails.present
		if len(present) == blockLen {
			// This series exists in every block already, thus no backfilling necessary.
			continue
		}

		for blockIdx, bl := range b.blocksAtTime {
			found := false
			for _, presentVal := range present {
				if presentVal == blockIdx {
					found = true
					break
				}
			}

			// this series exists in the current block.
			if found {
				continue
			}

			iter := encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{
				ID:             ident.StringID(iterDetails.id.String()),
				Namespace:      ident.StringID(iterDetails.ns.String()),
				StartInclusive: iterDetails.start,
				EndExclusive:   iterDetails.end,
			}, nil)

			newBl := bl.block
			newBl.seriesBlockIterators = append(newBl.seriesBlockIterators, iter)
			newBl.seriesMetas = append(newBl.seriesMetas, block.SeriesMeta{
				Name: iter.ID().Bytes(),
				Tags: iterDetails.tags,
			})

			b.blocksAtTime[blockIdx].block = newBl
		}
	}

	return nil
}

type seriesIteratorDetails struct {
	start, end xtime.UnixNano
	id, ns     ident.ID
	tags       models.Tags
	// NB: the indices that this series iterator exists in already.
	present []int
}

type seriesIteratorByID []encoding.SeriesIterator

func (s seriesIteratorByID) Len() int      { return len(s) }
func (s seriesIteratorByID) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s seriesIteratorByID) Less(i, j int) bool {
	return bytes.Compare(s[i].ID().Bytes(), s[j].ID().Bytes()) == -1
}
