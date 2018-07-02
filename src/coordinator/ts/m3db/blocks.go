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
	"errors"
	"time"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/dbnode/encoding"
)

var (
	errBlocksMisaligned = errors.New("blocks misaligned")
)

// SeriesBlockToMultiSeriesBlocks converts M3DB blocks to multi series blocks
func SeriesBlockToMultiSeriesBlocks(multiNamespaceSeriesList []MultiNamespaceSeries, seriesIteratorsPool encoding.MutableSeriesIteratorsPool) (MultiSeriesBlocks, error) {
	// todo(braskin): validate blocks size and aligment per namespace before creating []MultiNamespaceSeries
	var (
		multiSeriesBlocks MultiSeriesBlocks

		commonTags      = make(map[string]string)
		firstSeriesTags = make(map[string]string)
	)

	for multiNamespaceSeriesIdx, multiNamespaceSeries := range multiNamespaceSeriesList {
		consolidatedSeriesBlocks, err := newConsolidatedSeriesBlocks(multiNamespaceSeries, seriesIteratorsPool)
		if err != nil {
			return nil, err
		}

		// once we get the length of consolidatedSeriesBlocks, we can create a
		// MultiSeriesBlocks list with the proper size
		if multiNamespaceSeriesIdx == 0 {
			multiSeriesBlocks = make(MultiSeriesBlocks, len(consolidatedSeriesBlocks))
			commonTags, err = storage.FromIdentTagIteratorToTags(consolidatedSeriesBlocks[0].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].Tags())
			if err != nil {
				return nil, err
			}
		}

		for consolidatedSeriesBlockIdx, consolidatedSeriesBlock := range consolidatedSeriesBlocks {
			// we only want to set the start and end times once
			if multiNamespaceSeriesIdx == 0 {
				multiSeriesBlocks[consolidatedSeriesBlockIdx].Metadata.Bounds.StepSize = consolidatedSeriesBlock.Metadata.Bounds.StepSize
				multiSeriesBlocks[consolidatedSeriesBlockIdx].Metadata.Bounds.Start = consolidatedSeriesBlock.Metadata.Bounds.Start
				multiSeriesBlocks[consolidatedSeriesBlockIdx].Metadata.Bounds.End = consolidatedSeriesBlock.Metadata.Bounds.End
			} else {
				dupedTags := consolidatedSeriesBlock.ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].Tags().Duplicate()
				seriesTags, err := storage.FromIdentTagIteratorToTags(dupedTags)
				if err != nil {
					return nil, err
				}

				for tag, val := range commonTags {
					if seriesTags[tag] != val {
						delete(commonTags, tag)
						firstSeriesTags[tag] = val
					} else {
						delete(seriesTags, tag)
					}
				}
				consolidatedSeriesBlock.Metadata.Tags = seriesTags
			}

			if !consolidatedSeriesBlock.beyondBounds(multiSeriesBlocks[consolidatedSeriesBlockIdx]) {
				return nil, errBlocksMisaligned
			}

			multiSeriesBlocks[consolidatedSeriesBlockIdx].Blocks = append(multiSeriesBlocks[consolidatedSeriesBlockIdx].Blocks, consolidatedSeriesBlock)
		}
	}

	multiSeriesBlocks[0].Blocks[0].Metadata.Tags = firstSeriesTags
	multiSeriesBlocks.setCommonTags(commonTags)

	return multiSeriesBlocks, nil
}

func (m MultiSeriesBlocks) setCommonTags(commonTags map[string]string) {
	for i := range m {
		m[i].Metadata.Tags = commonTags
	}
}

// newConsolidatedSeriesBlocks creates consolidated blocks by timeseries across namespaces
func newConsolidatedSeriesBlocks(multiNamespaceSeries MultiNamespaceSeries, seriesIteratorsPool encoding.MutableSeriesIteratorsPool) (ConsolidatedSeriesBlocks, error) {
	var consolidatedSeriesBlocks ConsolidatedSeriesBlocks

	for seriesBlocksIdx, seriesBlocks := range multiNamespaceSeries {
		consolidatedNSBlocks := newConsolidatedNSBlocks(seriesBlocks, seriesIteratorsPool)
		// once we get the length of consolidatedNSBlocks, we can create a
		// ConsolidatedSeriesBlocks list with the proper size
		if seriesBlocksIdx == 0 {
			consolidatedSeriesBlocks = make(ConsolidatedSeriesBlocks, len(consolidatedNSBlocks))
		}

		for consolidatedNSBlockIdx, consolidatedNSBlock := range consolidatedNSBlocks {
			// we only want to set the start and end times once
			if seriesBlocksIdx == 0 {
				consolidatedSeriesBlocks[consolidatedNSBlockIdx].Metadata.Bounds.StepSize = consolidatedNSBlock.Bounds.StepSize
				consolidatedSeriesBlocks[consolidatedNSBlockIdx].Metadata.Bounds.Start = consolidatedNSBlock.Bounds.Start
				consolidatedSeriesBlocks[consolidatedNSBlockIdx].Metadata.Bounds.End = consolidatedNSBlock.Bounds.End
			}

			if !consolidatedNSBlock.beyondBounds(consolidatedSeriesBlocks[consolidatedNSBlockIdx]) {
				return nil, errBlocksMisaligned
			}

			consolidatedSeriesBlocks[consolidatedNSBlockIdx].ConsolidatedNSBlocks = append(consolidatedSeriesBlocks[consolidatedNSBlockIdx].ConsolidatedNSBlocks, consolidatedNSBlock)
		}
	}

	return consolidatedSeriesBlocks, nil
}

// newConsolidatedNSBlocks creates a slice of consolidated blocks per namespace for a single timeseries
// nolint: unparam
func newConsolidatedNSBlocks(seriesBlocks SeriesBlocks, seriesIteratorsPool encoding.MutableSeriesIteratorsPool) []ConsolidatedNSBlock {
	consolidatedNSBlocks := make([]ConsolidatedNSBlock, 0, len(seriesBlocks.Blocks))
	namespace := seriesBlocks.Namespace
	id := seriesBlocks.ID
	for _, seriesBlock := range seriesBlocks.Blocks {
		consolidatedNSBlock := ConsolidatedNSBlock{
			Namespace: namespace,
			ID:        id,
			Bounds: block.Bounds{
				Start:    seriesBlock.start,
				End:      seriesBlock.end,
				StepSize: time.Minute, // get actual step size!!
			},
		}
		s := []encoding.SeriesIterator{seriesBlock.seriesIterator}
		// todo(braskin): figure out how many series iterators we need based on largest step size (i.e. namespace)
		// and in future copy SeriesIterators using the seriesIteratorsPool
		consolidatedNSBlock.SeriesIterators = encoding.NewSeriesIterators(s, nil)
		consolidatedNSBlocks = append(consolidatedNSBlocks, consolidatedNSBlock)
	}

	return consolidatedNSBlocks
}
