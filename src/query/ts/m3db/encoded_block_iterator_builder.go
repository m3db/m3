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
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
	"github.com/m3db/m3x/ident"
)

const initBlockLength = 10

type blockAtTime struct {
	time  time.Time
	block encodedBlock
}

type blocksAtTime []blockAtTime

type encodedBlockBuilder struct {
	blocksAtTime    blocksAtTime
	tagOptions      models.TagOptions
	consolidationFn consolidators.ConsolidationFunc
}

func newEncodedBlockBuilder(
	tagOptions models.TagOptions,
	consolidationFn consolidators.ConsolidationFunc,
) *encodedBlockBuilder {
	return &encodedBlockBuilder{
		blocksAtTime:    make(blocksAtTime, 0, initBlockLength),
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

	for idx, bl := range b.blocksAtTime {
		if bl.time.Equal(start) {
			block := bl.block
			block.seriesBlockIterators = append(block.seriesBlockIterators, iter)
			b.blocksAtTime[idx].block = block

			return
		}
	}

	block := newEncodedBlock(
		[]encoding.SeriesIterator{},
		b.tagOptions,
		consolidation,
		time.Duration(0),
		lastBlock,
	)

	block.seriesBlockIterators = append(block.seriesBlockIterators, iter)
	b.blocksAtTime = append(b.blocksAtTime, blockAtTime{
		time:  start,
		block: block,
	})
}

func (b *encodedBlockBuilder) build() ([]block.Block, error) {
	if err := b.backfillMissing(); err != nil {
		return nil, err
	}

	blocks := make([]block.Block, 0, len(b.blocksAtTime))
	for _, bl := range b.blocksAtTime {
		block := bl.block
		if err := block.generateMetas(); err != nil {
			return nil, err
		}

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
		for _, iter := range block.seriesBlockIterators {
			id := iter.ID().String()
			if seen, found := seenMap[id]; !found {
				seenMap[id] = seriesIteratorDetails{
					start:   iter.Start(),
					end:     iter.End(),
					id:      iter.ID(),
					ns:      iter.Namespace(),
					tagIter: iter.Tags(),
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

			// blockIdx does not contain the present value; need to populate it with
			// an empty series iterator.
			tags, err := cloneTagIterator(iterDetails.tagIter)
			if err != nil {
				return err
			}

			iter := encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{
				ID:             ident.StringID(iterDetails.id.String()),
				Namespace:      ident.StringID(iterDetails.ns.String()),
				Tags:           tags,
				StartInclusive: iterDetails.start,
				EndExclusive:   iterDetails.end,
			}, nil)

			block := bl.block
			block.seriesBlockIterators = append(block.seriesBlockIterators, iter)
			b.blocksAtTime[blockIdx].block = block
		}
	}

	for _, bl := range b.blocksAtTime {
		sort.Sort(seriesIteratorByID(bl.block.seriesBlockIterators))
	}

	return nil
}

type seriesIteratorDetails struct {
	start, end time.Time
	id, ns     ident.ID
	tagIter    ident.TagIterator
	// NB: the indices that this series iterator exists in already.
	present []int
}

type seriesIteratorByID []encoding.SeriesIterator

func (s seriesIteratorByID) Len() int      { return len(s) }
func (s seriesIteratorByID) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s seriesIteratorByID) Less(i, j int) bool {
	return bytes.Compare(s[i].ID().Bytes(), s[j].ID().Bytes()) == -1
}
