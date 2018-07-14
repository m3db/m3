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
	coordtest "github.com/m3db/m3db/src/coordinator/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSeriesBlockIter struct {
	dps []float64
	idx int
}

func (m *mockSeriesBlockIter) Next() bool {
	m.idx++
	if m.idx < len(m.dps) {
		return true
	}
	return false
}

func (m *mockSeriesBlockIter) Current() float64 {
	return m.dps[m.idx]
}

func (m *mockSeriesBlockIter) Close() {}

func newMockSeriesBlockIter(dps [][]float64) []block.ValueIterator {
	var valueIters []block.ValueIterator
	for _, dp := range dps {
		valueIters = append(valueIters, &mockSeriesBlockIter{
			dps: dp,
			idx: -1,
		})
	}

	return valueIters
}

func createMultiSeriesBlockStepIter(seriesBlockIters []block.ValueIterator) multiSeriesBlockStepIter {
	return multiSeriesBlockStepIter{
		seriesIters: seriesBlockIters,
	}
}

type multiSeriesBlockStepIterTestCase struct {
	dps             [][]float64
	expectedResults [][]float64
	description     string
}

func TestMultiSeriesBlockStepIter(t *testing.T) {
	testCases := []multiSeriesBlockStepIterTestCase{
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
		mockNSBlockIters := newMockSeriesBlockIter(test.dps)
		consolidatedSeriesBlock := createMultiSeriesBlockStepIter(mockNSBlockIters)

		var actualResults [][]float64
		for consolidatedSeriesBlock.Next() {
			step, err := consolidatedSeriesBlock.Current()
			require.NoError(t, err)
			actualResults = append(actualResults, step.Values())
		}
		coordtest.EqualsWithNans(t, test.expectedResults, actualResults)
	}
}

type mockBlockStepIter struct {
	seriesIters []block.ValueIterator
	index       int
	meta        block.Metadata
	blocks      ConsolidatedSeriesBlocks
}

type mockBlockStepIterTestCase struct {
	dps             [][]float64
	expectedResults [][]float64
	seriesMeta      []block.Metadata
	blockMeta       block.Metadata
	description     string
}

func newMockMultiSeriesBlockStepIter(valIter []block.ValueIterator, blocks ConsolidatedSeriesBlocks, meta block.Metadata) *multiSeriesBlockStepIter {
	return &multiSeriesBlockStepIter{
		seriesIters: valIter,
		index:       -1,
		blocks:      blocks,
		meta:        meta,
	}
}

func newConsolidateSeriesBlock(metas []block.Metadata) ConsolidatedSeriesBlocks {
	csBlocks := make([]ConsolidatedSeriesBlock, len(metas))
	for i, meta := range metas {
		csBlocks[i] = ConsolidatedSeriesBlock{
			Metadata: meta,
		}
	}
	return csBlocks
}

func TestMultiSeriesBlock(t *testing.T) {
	testCases := []mockBlockStepIterTestCase{
		{
			dps:             [][]float64{{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}},
			expectedResults: [][]float64{{1, 6}, {2, 7}, {3, 8}, {4, 9}, {5, 10}},
			description:     "return values from multiple blocks and test metadata",
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
		csBlocks := newConsolidateSeriesBlock(test.seriesMeta)
		mockSeriesBlockIters := newMockSeriesBlockIter(test.dps)
		stepIter := newMockMultiSeriesBlockStepIter(mockSeriesBlockIters, csBlocks, test.blockMeta)

		var actualResults [][]float64
		for stepIter.Next() {
			step, err := stepIter.Current()
			require.NoError(t, err)
			actualResults = append(actualResults, step.Values())
		}
		coordtest.EqualsWithNans(t, test.expectedResults, actualResults)

		assert.Equal(t, test.blockMeta.Tags, stepIter.Meta().Tags)

		seriesMeta := stepIter.SeriesMeta()
		for i, series := range test.seriesMeta {
			assert.Equal(t, series.Tags, seriesMeta[i].Tags)
		}
	}
}
