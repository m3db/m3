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
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeResults() [][]float64 {
	var results [][]float64
	sliceOfNaNs := []float64{math.NaN(), math.NaN()}
	results = append(results, sliceOfNaNs)
	results = append(results, []float64{1, 1})
	results = append(results, sliceOfNaNs, sliceOfNaNs, sliceOfNaNs)
	results = append(results, []float64{3, 3})
	for i := 0; i < 11; i++ {
		results = append(results, sliceOfNaNs)
	}
	results = append(results, []float64{5, 5})
	results = append(results, sliceOfNaNs)
	results = append(results, []float64{6, 6})

	return results
}

func TestStepIteration(t *testing.T) {
	now := time.Now()
	ctrl := gomock.NewController(t)
	seriesOne, seriesTwo := newMultiNamespaceSeries(ctrl, now, t)

	multiNamespaceSeriesList := []MultiNamespaceSeries{seriesOne, seriesTwo}
	m3CoordBlocks, err := SeriesBlockToMultiSeriesBlocks(multiNamespaceSeriesList, nil, time.Minute)
	require.NoError(t, err)

	expectedResults := makeResults()
	var actualResults [][]float64

	seriesMeta := m3CoordBlocks[0].SeriesMeta()
	tags := []map[string]string{{"foo": "bar"}, {"biz": "baz"}}
	for i, tag := range tags {
		for k, v := range tag {
			assert.Equal(t, v, seriesMeta[i].Tags[k])
		}
	}

	for _, seriesBlock := range m3CoordBlocks {
		stepIter := seriesBlock.StepIter()
		for stepIter.Next() {
			actualResults = append(actualResults, stepIter.Current().Values())
		}
	}

	assert.Equal(t, len(expectedResults), len(actualResults))

	for i, expectedSlice := range expectedResults {
		for j, exp := range expectedSlice {
			if math.IsNaN(exp) {
				assert.True(t, math.IsNaN(actualResults[i][j]))
			} else {
				assert.Equal(t, exp, actualResults[i][j])
			}
		}
	}
}
