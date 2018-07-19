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

package block

import (
	"errors"
	"time"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/dbnode/encoding"
)

var (
	errBlocksMisaligned = errors.New("blocks misaligned")
)

// SeriesBlockToMultiSeriesBlocks converts M3DB blocks to multi series blocks
func SeriesBlockToMultiSeriesBlocks(
	multiNamespaceSeriesList []MultiNamespaceSeries,
	seriesIteratorsPool encoding.MutableSeriesIteratorsPool,
	stepSize time.Duration,
) (MultiSeriesBlocks, error) {
	// todo(braskin): validate blocks size and aligment per namespace before creating []MultiNamespaceSeries
	var multiSeriesBlocks MultiSeriesBlocks

	for seriesIdx, multiNamespaceSeries := range multiNamespaceSeriesList {
		consolidatedSeriesBlocks, err := newConsolidatedSeriesBlocks(multiNamespaceSeries, seriesIteratorsPool, stepSize)
		if err != nil {
			return nil, err
		}

		// once we get the length of consolidatedSeriesBlocks, we can create a
		// MultiSeriesBlocks list with the proper size
		if seriesIdx == 0 {
			multiSeriesBlocks = make(MultiSeriesBlocks, len(consolidatedSeriesBlocks))
		}

		for consolidatedSeriesBlockIdx, consolidatedSeriesBlock := range consolidatedSeriesBlocks {

			// we only want to set the start and end times once
			if seriesIdx == 0 {
				blockBounds := multiSeriesBlocks[consolidatedSeriesBlockIdx].Metadata.Bounds
				blockBounds.StepSize = consolidatedSeriesBlock.Metadata.Bounds.StepSize
				blockBounds.Start = consolidatedSeriesBlock.Metadata.Bounds.Start
				blockBounds.End = consolidatedSeriesBlock.Metadata.Bounds.End
				multiSeriesBlocks[consolidatedSeriesBlockIdx].Metadata.Bounds = blockBounds
			}

			// take the tags from the first iterator and set that as the tags for the series block
			dupedTags := consolidatedSeriesBlock.NSBlocks[0].SeriesIterators.Iters()[0].Tags().Duplicate()
			seriesTags, err := storage.FromIdentTagIteratorToTags(dupedTags)
			if err != nil {
				return nil, err
			}
			consolidatedSeriesBlock.Metadata.Tags = seriesTags

			if !consolidatedSeriesBlock.Metadata.Bounds.Equals(multiSeriesBlocks[consolidatedSeriesBlockIdx].Metadata.Bounds) {
				return nil, errBlocksMisaligned
			}

			multiSeriesBlocks[consolidatedSeriesBlockIdx].Blocks = append(multiSeriesBlocks[consolidatedSeriesBlockIdx].Blocks, consolidatedSeriesBlock)
		}
	}

	commonTags := multiSeriesBlocks.commonTags()
	multiSeriesBlocks.setCommonTags(commonTags)

	return multiSeriesBlocks, nil
}

func (m MultiSeriesBlocks) setCommonTags(commonTags models.Tags) {
	for i := range m {
		m[i].Metadata.Tags = commonTags
	}
}

func (m MultiSeriesBlocks) commonTags() models.Tags {
	if len(m) == 0 || len(m[0].Blocks) == 0 {
		return models.Tags{}
	}

	commonTags := m[0].Blocks[0].Metadata.Tags.Clone()

	// NB(braskin): all series are the same across MultiSeriesBlocks so only need
	// to look at one
	for _, series := range m[0].Blocks[1:] {
		seriesTags := series.Metadata.Tags
		for tag, val := range commonTags {
			if seriesTags[tag] != val {
				delete(commonTags, tag)
			}
		}
	}
	return commonTags
}

// newConsolidatedSeriesBlocks creates consolidated blocks by timeseries across namespaces
func newConsolidatedSeriesBlocks(
	multiNamespaceSeries MultiNamespaceSeries,
	seriesIteratorsPool encoding.MutableSeriesIteratorsPool,
	stepSize time.Duration,
) (ConsolidatedBlocks, error) {
	var consolidatedBlocks ConsolidatedBlocks

	for seriesBlocksIdx, seriesBlocks := range multiNamespaceSeries {
		nsBlocks := newNSBlocks(seriesBlocks, seriesIteratorsPool, stepSize)
		// once we get the length of consolidatedNSBlocks, we can create a
		// ConsolidatedSeriesBlocks list with the proper size
		if seriesBlocksIdx == 0 {
			consolidatedBlocks = make(ConsolidatedBlocks, len(nsBlocks))
		}

		for nsBlockIdx, nsBlock := range nsBlocks {
			// we only want to set the start and end times once
			if seriesBlocksIdx == 0 {
				blockBounds := consolidatedBlocks[nsBlockIdx].Metadata.Bounds
				blockBounds.StepSize = nsBlock.Bounds.StepSize
				blockBounds.Start = nsBlock.Bounds.Start
				blockBounds.End = nsBlock.Bounds.End
				consolidatedBlocks[nsBlockIdx].Metadata.Bounds = blockBounds
			}

			if !nsBlock.Bounds.Equals(consolidatedBlocks[nsBlockIdx].Metadata.Bounds) {
				return nil, errBlocksMisaligned
			}

			consolidatedBlocks[nsBlockIdx].NSBlocks = append(consolidatedBlocks[nsBlockIdx].NSBlocks, nsBlock)
		}
	}

	return consolidatedBlocks, nil
}

// newNSBlocks creates a slice of consolidated blocks per namespace for a single timeseries
// nolint: unparam
func newNSBlocks(
	seriesBlocks SeriesBlocks,
	seriesIteratorsPool encoding.MutableSeriesIteratorsPool,
	stepSize time.Duration,
) []NSBlock {
	nsBlocks := make([]NSBlock, 0, len(seriesBlocks.Blocks))
	namespace := seriesBlocks.Namespace
	id := seriesBlocks.ID
	for _, seriesBlock := range seriesBlocks.Blocks {
		nsBlock := NSBlock{
			Namespace: namespace,
			ID:        id,
			Bounds: block.Bounds{
				Start:    seriesBlock.start,
				End:      seriesBlock.end,
				StepSize: stepSize,
			},
		}
		s := []encoding.SeriesIterator{seriesBlock.seriesIterator}
		// todo(braskin): figure out how many series iterators we need based on largest step size (i.e. namespace)
		// and in future copy SeriesIterators using the seriesIteratorsPool
		nsBlock.SeriesIterators = encoding.NewSeriesIterators(s, nil)
		nsBlocks = append(nsBlocks, nsBlock)
	}

	return nsBlocks
}
