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
)

type mockNSBlockIter struct {
	dps []float64
	idx int
}

func (m *mockNSBlockIter) Next() bool {
	m.idx++
	if m.idx < len(m.dps) {
		return true
	}
	return false
}

func (m *mockNSBlockIter) Current() float64 {
	return m.dps[m.idx]
}

func (m *mockNSBlockIter) Close() {}

func newMockNSBlockIter(dps [][]float64) []block.ValueIterator {
	var valueIters []block.ValueIterator
	for _, dp := range dps {
		valueIters = append(valueIters, &mockNSBlockIter{
			dps: dp,
			idx: -1,
		})
	}

	return valueIters
}

func createConsolidatedSeriesBlockIter(nsBlockIters []block.ValueIterator) consolidatedSeriesBlockIter {
	return consolidatedSeriesBlockIter{
		consolidatedNSBlockIters: nsBlockIters,
	}
}

type consolidatedSeriesTestCase struct {
	dps             [][]float64
	expectedResults []float64
	description     string
}

func TestConsolidatedSeriesBlockIter(t *testing.T) {
	testCases := []consolidatedSeriesTestCase{
		{
			dps:             [][]float64{{1, 2, 3, 4, 5}},
			expectedResults: []float64{1, 2, 3, 4, 5},
			description:     "only return one set of datapoints",
		},
		{
			dps:             [][]float64{{6, 7, 8, 9, 10}, {1, 2, 3, 4, 5}},
			expectedResults: []float64{6, 7, 8, 9, 10},
			description:     "return only the first set of datapoints (consolidation)",
		},
	}

	for _, test := range testCases {
		mockNSBlockIters := newMockNSBlockIter(test.dps)
		consolidatedSeriesBlock := createConsolidatedSeriesBlockIter(mockNSBlockIters)

		var actualResults []float64
		for consolidatedSeriesBlock.Next() {
			actualResults = append(actualResults, consolidatedSeriesBlock.Current())
		}
		coordtest.EqualsWithNans(t, test.expectedResults, actualResults)
	}
}
