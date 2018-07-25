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
	"fmt"
	"time"

	"github.com/m3db/m3x/ident"

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
	multiNSSeries map[ident.ID][]SeriesBlocks,
	multiNamespaceSeriesList []NamespaceSeriesList,
	seriesIteratorsPool encoding.MutableSeriesIteratorsPool,
	stepSize time.Duration,
) (MultiSeriesBlocks, error) {
	// todo(braskin): validate blocks size and aligment per namespace before creating []MultiNamespaceSeries
	var multiSeriesBlocks MultiSeriesBlocks
	// consolidatedSeriesBlocks := make(ConsolidatedBlocks, len())
	var consolidatedSeriesBlocks []ConsolidatedBlocks
	var err error

	for id, blocks := range multiNSSeries {
		consolidatedBlocks := newCSBlocks(blocks)
	}

	for nsIdx, nsSeriesList := range multiNamespaceSeriesList {
		fmt.Println("multi namespace series")
		for _, e := range nsSeriesList.SeriesList {
			fmt.Println(e)
		}
		consolidatedSeriesBlocks, err = newConsolidatedSeriesBlocks(nsSeriesList, seriesIteratorsPool, stepSize)
		if err != nil {
			return nil, err
		}

		fmt.Println("\n", "cs blocks: ")
		for _, c := range consolidatedSeriesBlocks {
			fmt.Println("***")
			fmt.Println(c)
			fmt.Println("***")
		}

		// // once we get the length of consolidatedSeriesBlocks, we can create a
		// // MultiSeriesBlocks list with the proper size
		// **this is wrong, we need to get number of blocks based on bounds
		if nsIdx == 0 {
			multiSeriesBlocks = make(MultiSeriesBlocks, len(consolidatedSeriesBlocks[0]))
		}

		groupedCSBlocks := make([]ConsolidatedBlocks, len(consolidatedSeriesBlocks[0]))
		// var groupedCSBlocks []ConsolidatedBlocks
		// var timeSet bool

		for _, consolidatedSeriesBlock := range consolidatedSeriesBlocks {
			// if len(consolidatedSeriesBlock[0].NSBlocks) == 0 {
			// 	continue
			// }
			dupedTags := consolidatedSeriesBlock[0].NSBlocks[0].SeriesIterators.Iters()[0].Tags().Duplicate()
			seriesTags, err := storage.FromIdentTagIteratorToTags(dupedTags)
			if err != nil {
				return nil, err
			}

			for idx, csBlock := range consolidatedSeriesBlock {
				csBlock.Metadata.Tags = seriesTags

				if nsIdx == 0 {
					blockBounds := multiSeriesBlocks[idx].Metadata.Bounds
					blockBounds.StepSize = csBlock.Metadata.Bounds.StepSize
					blockBounds.Start = csBlock.Metadata.Bounds.Start
					blockBounds.End = csBlock.Metadata.Bounds.End
					multiSeriesBlocks[idx].Metadata.Bounds = blockBounds
				}

				groupedCSBlocks[idx] = append(groupedCSBlocks[idx], csBlock)

			}
		}

		fmt.Println("\n", "grouped cs blocks: ")
		for _, j := range groupedCSBlocks {
			fmt.Println(j)
		}

		// multiSeriesBlocks[seriesIdx].Blocks = groupedCSBlocks
		for i := 0; i < len(consolidatedSeriesBlocks[0]); i++ {
			for _, csBlocks := range groupedCSBlocks[i] {
				if !csBlocks.Metadata.Bounds.Equals(multiSeriesBlocks[i].Metadata.Bounds) {
					return nil, errBlocksMisaligned
				}
			}

			multiSeriesBlocks[i].Blocks = groupedCSBlocks[i]
		}

		fmt.Println("\n", "multi series blocks: ")
		for _, m := range multiSeriesBlocks {
			fmt.Println(m)
		}
	}

	commonTags := multiSeriesBlocks.commonTags()
	multiSeriesBlocks.setCommonTags(commonTags)

	// fmt.Println("****", multiSeriesBlocks[0].Metadata, multiSeriesBlocks[1].Metadata)
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
	nsSeriesList NamespaceSeriesList,
	seriesIteratorsPool encoding.MutableSeriesIteratorsPool,
	stepSize time.Duration,
) ([]ConsolidatedBlocks, error) {
	fmt.Println("number series: ", len(nsSeriesList.SeriesList))
	sliceOfCSBlocks := make([]ConsolidatedBlocks, len(nsSeriesList.SeriesList))

	var nsBlocks []NSBlock

	for seriesBlocksIdx, seriesBlocks := range nsSeriesList.SeriesList {
		var consolidatedBlocks ConsolidatedBlocks
		nsBlocks = newNSBlocks(seriesBlocks, seriesIteratorsPool, stepSize, nsSeriesList.Namespace)
		consolidatedBlocks = make(ConsolidatedBlocks, len(nsBlocks))

		for nsBlockIdx, nsBlock := range nsBlocks {
			blockBounds := consolidatedBlocks[nsBlockIdx].Metadata.Bounds
			blockBounds.StepSize = nsBlock.Bounds.StepSize
			blockBounds.Start = nsBlock.Bounds.Start
			blockBounds.End = nsBlock.Bounds.End
			consolidatedBlocks[nsBlockIdx].Metadata.Bounds = blockBounds

			if !nsBlock.Bounds.Equals(consolidatedBlocks[nsBlockIdx].Metadata.Bounds) {
				return nil, errBlocksMisaligned
			}

			consolidatedBlocks[nsBlockIdx].NSBlocks = append(consolidatedBlocks[nsBlockIdx].NSBlocks, nsBlock)
		}
		// consolidatedBlocks[seriesBlocksIdx].NSBlocks = nsBlocks
		sliceOfCSBlocks[seriesBlocksIdx] = consolidatedBlocks
	}

	// fmt.Println("len consolidated blocks: ", len(consolidatedBlocks))
	return sliceOfCSBlocks, nil
}

// newNSBlocks creates a slice of consolidated blocks per namespace for a single timeseries
// nolint: unparam
func newNSBlocks(
	seriesBlocks SeriesBlocks,
	seriesIteratorsPool encoding.MutableSeriesIteratorsPool,
	stepSize time.Duration,
	ns string,
) []NSBlock {
	nsBlocks := make([]NSBlock, 0, len(seriesBlocks.Blocks))
	// namespace := seriesBlocks.Namespace
	id := seriesBlocks.ID
	for _, seriesBlock := range seriesBlocks.Blocks {
		nsBlock := NSBlock{
			Namespace: ident.StringID(ns),
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
