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
	"testing"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/models"
	coordtest "github.com/m3db/m3db/src/coordinator/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockMultiBlockStepIter struct {
	dps       []float64
	idx       int
	numSeries int
}

func (m *mockMultiBlockStepIter) Next() bool {
	m.idx++
	if m.idx < len(m.dps) {
		return true
	}
	return false
}

func (m *mockMultiBlockStepIter) Current() float64 {
	return m.dps[m.idx]
}

func (m *mockMultiBlockStepIter) Close() {}

func newMockValueStepIter(dps [][]float64) []block.ValueIterator {
	var valueIters []block.ValueIterator
	for _, dp := range dps {
		valueIters = append(valueIters, &mockMultiBlockStepIter{
			dps: dp,
			idx: -1,
		})
	}

	return valueIters
}

func createMultiBlockStepIter(seriesBlockIters []block.ValueIterator, msBlock MultiSeriesBlock) multiBlockStepIter {
	return multiBlockStepIter{
		seriesIters: seriesBlockIters,
		index:       -1,
		m:           msBlock,
	}
}

type multiBlockStepIterTestCase struct {
	dps             [][]float64
	expectedResults [][]float64
	description     string
}

func TestMultiBlockStepIter(t *testing.T) {
	testCases := []multiBlockStepIterTestCase{
		{
			dps:             [][]float64{{1, 2, 3, 4, 5}},
			expectedResults: [][]float64{{1}, {2}, {3}, {4}, {5}},
			description:     "only return one set of datapoints",
		},
		{
			dps:             [][]float64{{6, 7, 8, 9, 10}, {1, 2, 3, 4, 5}},
			expectedResults: [][]float64{{6, 1}, {7, 2}, {8, 3}, {9, 4}, {10, 5}},
			description:     "take only the first set of datapoints (consolidation)",
		},
	}

	for _, test := range testCases {
		mockNSBlockIters := newMockValueStepIter(test.dps)
		consolidatedSeriesBlock := createMultiBlockStepIter(mockNSBlockIters, MultiSeriesBlock{})

		var actualResults [][]float64
		for consolidatedSeriesBlock.Next() {
			step, err := consolidatedSeriesBlock.Current()
			require.NoError(t, err)
			actualResults = append(actualResults, step.Values())
		}
		coordtest.EqualsWithNans(t, test.expectedResults, actualResults)
	}
}

func createMultiBlockSeriesIter(seriesBlockIters []block.ValueIterator, msBlock MultiSeriesBlock) multiBlockSeriesIter {
	return multiBlockSeriesIter{
		seriesIters: seriesBlockIters,
		index:       -1,
		m:           msBlock,
	}
}

type multiBlockSeriesIterTestCase struct {
	dps             [][]float64
	expectedResults [][]float64
	blocksMeta      []models.Tags
	description     string
}

func TestMultiBlockSeriesIter(t *testing.T) {
	testCases := []multiBlockSeriesIterTestCase{
		{
			dps:             [][]float64{{1, 2, 3, 4, 5}},
			expectedResults: [][]float64{{1, 2, 3, 4, 5}},
			blocksMeta: []models.Tags{
				{"foo": "bar", "__name__": "test_name"},
			},
			description: "only return one set of datapoints",
		},
		{
			dps:             [][]float64{{6, 7, 8, 9, 10}, {1, 2, 3, 4, 5}},
			expectedResults: [][]float64{{6, 7, 8, 9, 10}, {1, 2, 3, 4, 5}},
			blocksMeta: []models.Tags{
				{"foo": "bar", "__name__": "test_series_one"},
				{"biz": "baz", "__name__": "test_series_two"},
			},
			description: "take only the first set of datapoints (consolidation)",
		},
	}

	for _, test := range testCases {
		mockNSBlockIters := newMockValueStepIter(test.dps)
		msBlock := createBlocksWithTagsOnly(test.blocksMeta)
		multiBlockSeriesIter := createMultiBlockSeriesIter(mockNSBlockIters, msBlock)

		var actualResults [][]float64
		for multiBlockSeriesIter.Next() {
			series, err := multiBlockSeriesIter.Current()
			require.NoError(t, err)
			actualResults = append(actualResults, series.Values())
		}

		coordtest.EqualsWithNans(t, test.expectedResults, actualResults)
	}
}

func createBlocksWithTagsOnly(tags []models.Tags) MultiSeriesBlock {
	csBlocks := make(ConsolidatedBlocks, len(tags))
	for i, t := range tags {
		csBlocks[i] = ConsolidatedBlock{
			Metadata: block.Metadata{
				Tags: t,
			},
		}
	}
	return MultiSeriesBlock{Blocks: csBlocks}
}

type mockBlockIterTestCase struct {
	dps                                        [][]float64
	expectedStepResults, expectedSeriesResults [][]float64
	seriesMeta                                 []block.Metadata
	blockMeta                                  block.Metadata
	description                                string
}

func newMSBlock(seriesMeta []block.Metadata, blockMeta block.Metadata) MultiSeriesBlock {
	csBlocks := make([]ConsolidatedBlock, len(seriesMeta))
	for i, meta := range seriesMeta {
		csBlocks[i] = ConsolidatedBlock{
			Metadata: meta,
		}
	}
	return MultiSeriesBlock{Blocks: csBlocks, Metadata: blockMeta}
}

func TestMultiSeriesBlock(t *testing.T) {
	testCases := []mockBlockIterTestCase{
		{
			dps:                   [][]float64{{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}},
			expectedStepResults:   [][]float64{{1, 6}, {2, 7}, {3, 8}, {4, 9}, {5, 10}},
			expectedSeriesResults: [][]float64{{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}},
			description:           "return values from multiple blocks and test metadata",
			seriesMeta: []block.Metadata{
				{
					Tags: map[string]string{"my": "tag", "same": "tag"},
				},
				{
					Tags: map[string]string{"my": "other_tag", "same": "tag"},
				},
			},
			blockMeta: block.Metadata{
				Tags: map[string]string{"same": "tag"},
			},
		},
	}

	for _, test := range testCases {
		msBlock := newMSBlock(test.seriesMeta, test.blockMeta)
		mockSeriesBlockIters := newMockValueStepIter(test.dps)
		stepIter := createMultiBlockStepIter(mockSeriesBlockIters, msBlock)

		var actualStepResults [][]float64
		for stepIter.Next() {
			step, err := stepIter.Current()
			require.NoError(t, err)
			actualStepResults = append(actualStepResults, step.Values())
		}
		coordtest.EqualsWithNans(t, test.expectedStepResults, actualStepResults)

		assert.Equal(t, test.blockMeta.Tags, stepIter.Meta().Tags)

		seriesMeta := stepIter.SeriesMeta()
		for i, series := range test.seriesMeta {
			assert.Equal(t, series.Tags, seriesMeta[i].Tags)
		}

		mockBlockSeriesIters := newMockValueStepIter(test.dps)
		seriesIter := createMultiBlockSeriesIter(mockBlockSeriesIters, msBlock)

		var actualSeriesResults [][]float64
		for seriesIter.Next() {
			series, err := seriesIter.Current()
			require.NoError(t, err)
			actualSeriesResults = append(actualSeriesResults, series.Values())
		}
		coordtest.EqualsWithNans(t, test.expectedSeriesResults, actualSeriesResults)
		assert.Equal(t, test.blockMeta.Tags, seriesIter.Meta().Tags)

		for i, meta := range seriesIter.SeriesMeta() {
			assert.Equal(t, test.seriesMeta[i].Tags, meta.Tags)
		}
	}
}
