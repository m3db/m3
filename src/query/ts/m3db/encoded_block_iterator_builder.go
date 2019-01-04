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
		// NB: the lookback will always be 0, as splitting series into blocks is not
		// supported with positive lookback durations.
		b.blocksAtTime[str] = newEncodedBlock(
			[]encoding.SeriesIterator{},
			b.tagOptions,
			consolidation,
			time.Duration(0),
			lastBlock,
		)
	}

	block := b.blocksAtTime[str]
	block.seriesBlockIterators = append(block.seriesBlockIterators, iter)
	b.blocksAtTime[str] = block
}

func (b *encodedBlockBuilder) build() ([]block.Block, error) {
	if err := b.backfillMissing(); err != nil {
		return nil, err
	}

	blocks := make([]block.Block, 0, len(b.blocksAtTime))
	for _, block := range b.blocksAtTime {
		if err := block.generateMetas(); err != nil {
			return nil, err
		}

		bl := block
		blocks = append(blocks, &bl)
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
	// Collect all possible IDs and tag iterator lists
	seenMap := make(map[string]seriesIteratorDetails, initBlockReplicaLength)
	for key, block := range b.blocksAtTime {
		for _, iter := range block.seriesBlockIterators {
			id := iter.ID().String()
			if seen, found := seenMap[id]; !found {
				seenMap[id] = seriesIteratorDetails{
					start:   iter.Start(),
					end:     iter.End(),
					id:      iter.ID(),
					ns:      iter.Namespace(),
					tagIter: iter.Tags(),
					present: []string{key},
				}
			} else {
				seen.present = append(seen.present, key)
				seenMap[id] = seen
			}
		}
	}

	for key, block := range b.blocksAtTime {
		for _, iterDetails := range seenMap {
			keyPresent := false
			for _, k := range iterDetails.present {
				if k == key {
					keyPresent = true
					break
				}
			}

			if !keyPresent {
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

				block.seriesBlockIterators = append(block.seriesBlockIterators, iter)
				b.blocksAtTime[key] = block
			}
		}
	}

	for _, block := range b.blocksAtTime {
		sort.Sort(seriesIteratorByID(block.seriesBlockIterators))
	}

	return nil
}

type seriesIteratorDetails struct {
	start, end time.Time
	id, ns     ident.ID
	tagIter    ident.TagIterator
	// NB: the indices that this series iterator exists in already.
	present []string
}

type seriesIteratorByID []encoding.SeriesIterator

func (s seriesIteratorByID) Len() int      { return len(s) }
func (s seriesIteratorByID) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s seriesIteratorByID) Less(i, j int) bool {
	return bytes.Compare(s[i].ID().Bytes(), s[j].ID().Bytes()) == -1
}
