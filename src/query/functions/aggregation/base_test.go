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

package aggregation

import (
	"math"
	"sort"
	"testing"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var collectTest = []struct {
	name            string
	matching        []string
	tagLists        []models.Tags
	expectedIndices [][]int
	expectedTags    []models.Tags
	withoutIndices  [][]int
	withoutTags     []models.Tags
}{
	{
		"noMatching",
		[]string{},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2", "c": "4"},
			{"b": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"c": "d"},
		},
		[][]int{{0, 1, 2, 3, 4, 5}},
		[]models.Tags{{}},
		[][]int{{0}, {1}, {2}, {3}, {4}, {5}},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2", "c": "4"},
			{"b": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"c": "d"},
		},
	},
	{
		"no equal Matching",
		[]string{"f", "g", "h"},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2", "c": "4"},
			{"b": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"c": "d"},
		},
		[][]int{{0, 1, 2, 3, 4, 5}},
		[]models.Tags{{}},
		[][]int{{0}, {1}, {2}, {3}, {4}, {5}},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2", "c": "4"},
			{"b": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"c": "d"},
		},
	},
	{
		"oneMatching",
		[]string{"a"},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2", "c": "4"},
			{"b": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"c": "d"},
		},
		[][]int{{0, 1, 3, 4}, {2, 5}},
		[]models.Tags{
			{"a": "1"},
			{},
		},
		[][]int{{0}, {1}, {2}, {3}, {4}, {5}},
		[]models.Tags{
			{},
			{"b": "2", "c": "4"},
			{"b": "2"},
			{"b": "2", "c": "3"},
			{"b": "2", "d": "3"},
			{"c": "d"},
		},
	},
	{
		"diffMatching",
		[]string{"a"},
		[]models.Tags{
			{"a": "1"},
			{"a": "2", "b": "2", "c": "4"},
			{"a": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"a": "d"},
		},
		[][]int{{0, 3, 4}, {1, 2}, {5}},
		[]models.Tags{
			{"a": "1"},
			{"a": "2"},
			{"a": "d"},
		},
		[][]int{{0, 2, 5}, {1}, {3}, {4}},
		[]models.Tags{
			{},
			{"b": "2", "c": "4"},
			{"b": "2", "c": "3"},
			{"b": "2", "d": "3"},
		},
	},
	{
		"someMatching",
		[]string{"a", "b"},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2", "c": "4"},
			{"b": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"c": "3"},
		},
		[][]int{{0}, {1, 3, 4}, {2}, {5}},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2"},
			{"b": "2"},
			{},
		},
		[][]int{{0, 2}, {1}, {3, 5}, {4}},
		[]models.Tags{
			{},
			{"c": "4"},
			{"c": "3"},
			{"d": "3"},
		},
	},
	{
		"functionMatching",
		[]string{"a"},
		[]models.Tags{
			{"a": "1"},
			{"a": "1"},
			{"a": "1", "b": "2"},
			{"a": "2", "b": "2"},
			{"b": "2"},
			{"c": "3"},
		},
		[][]int{{0, 1, 2}, {3}, {4, 5}},
		[]models.Tags{
			{"a": "1"},
			{"a": "2"},
			{},
		},
		[][]int{{0, 1}, {2, 3, 4}, {5}},
		[]models.Tags{
			{},
			{"b": "2"},
			{"c": "3"},
		},
	},
}

func testCollect(t *testing.T, without bool) {
	for _, tt := range collectTest {
		name := tt.name + " with tags"
		if without {
			name = tt.name + " without tags"
		}

		t.Run(name, func(t *testing.T) {
			metas := make([]block.SeriesMeta, len(tt.tagLists))
			for i, tagList := range tt.tagLists {
				metas[i] = block.SeriesMeta{Tags: tagList}
			}

			params := NodeParams{
				Matching: tt.matching,
				Without:  without,
			}

			indices, collected := collectSeries(params, name, metas)

			expectedTags := tt.expectedTags
			expectedIndicies := tt.expectedIndices
			if without {
				expectedTags = tt.withoutTags
				expectedIndicies = tt.withoutIndices
			}

			expectedMetas := make([]block.SeriesMeta, len(expectedTags))
			for i, tags := range expectedTags {
				expectedMetas[i] = block.SeriesMeta{
					Tags: tags,
					Name: name,
				}
			}

			compareLists(t, collected, expectedMetas, indices, expectedIndicies)
		})
	}
}

func TestCollectWithTags(t *testing.T) {
	testCollect(t, false)
}

func TestCollectWithoutTags(t *testing.T) {
	testCollect(t, true)
}

type match struct {
	indices []int
	metas   block.SeriesMeta
	values  []float64
}

type matches []match

func (m matches) Len() int           { return len(m) }
func (m matches) Less(i, j int) bool { return m[i].metas.Tags.ID() > m[j].metas.Tags.ID() }
func (m matches) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }

func compareLists(t *testing.T, meta, exMeta []block.SeriesMeta, index, exIndex [][]int) {
	require.Equal(t, len(exIndex), len(exMeta))
	require.Equal(t, len(exMeta), len(meta))
	require.Equal(t, len(exIndex), len(index))

	ex := make(matches, len(meta))
	actual := make(matches, len(meta))
	// build matchers
	for i := range meta {
		ex[i] = match{exIndex[i], exMeta[i], []float64{}}
		actual[i] = match{index[i], meta[i], []float64{}}
	}
	sort.Sort(ex)
	sort.Sort(actual)
	assert.Equal(t, ex, actual)
}

func compareValues(t *testing.T, meta, exMeta []block.SeriesMeta, vals, exVals [][]float64) {
	require.Equal(t, len(exVals), len(exMeta))
	require.Equal(t, len(exMeta), len(meta))
	require.Equal(t, len(exVals), len(vals))

	ex := make(matches, len(meta))
	actual := make(matches, len(meta))
	// build matchers
	for i := range meta {
		ex[i] = match{[]int{}, exMeta[i], exVals[i]}
		actual[i] = match{[]int{}, meta[i], vals[i]}
	}

	sort.Sort(ex)
	sort.Sort(actual)
	for i := range ex {
		assert.Equal(t, ex[i].metas, actual[i].metas)
		test.EqualsWithNansWithDelta(t, ex[i].values, actual[i].values, 0.00001)
	}
}

func TestFunctionWithFiltering(t *testing.T) {
	_, bounds := test.GenerateValuesAndBounds(nil, nil)
	seriesMetas := []block.SeriesMeta{
		{Tags: models.Tags{"a": "1"}},
		{Tags: models.Tags{"a": "1"}},
		{Tags: models.Tags{"a": "1", "b": "2"}},
		{Tags: models.Tags{"a": "2", "b": "2"}},
		{Tags: models.Tags{"b": "2"}},
		{Tags: models.Tags{"c": "3"}},
	}
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
		{10, 20, 30, 40, 50},
		{50, 60, 70, 80, 90},
		{100, 200, 300, 400, 500},
		{600, 700, 800, 900, 1000},
	}

	bl := test.NewBlockFromValuesWithSeriesMeta(bounds, seriesMetas, v)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	op, err := NewAggregationOp(StandardDeviationType, NodeParams{Matching: []string{"a"}, Without: false})
	require.NoError(t, err)
	node := op.(baseOp).Node(c)
	err = node.Process(parser.NodeID(0), bl)
	require.NoError(t, err)
	expected := [][]float64{
		// stddev of first three series
		{7.07107, 9.89949, 14.93318, 20.07486, 25.23886},
		// stddev of fourth series
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		// stddev of fifth and sixth series
		{353.55339, 353.55339, 353.55339, 353.55339, 353.55339},
	}

	expectedMetas := []block.SeriesMeta{
		{Name: StandardDeviationType, Tags: models.Tags{"a": "1"}},
		{Name: StandardDeviationType, Tags: models.Tags{"a": "2"}},
		{Name: StandardDeviationType, Tags: models.Tags{}},
	}

	compareValues(t, sink.Metas, expectedMetas, sink.Values, expected)
	assert.Equal(t, bounds, sink.Meta.Bounds)

	c, sink = executor.NewControllerWithSink(parser.NodeID(1))
	op, err = NewAggregationOp(StandardDeviationType, NodeParams{Matching: []string{"a"}, Without: true})
	require.NoError(t, err)
	node = op.(baseOp).Node(c)
	err = node.Process(parser.NodeID(0), bl)
	require.NoError(t, err)
	expected = [][]float64{
		// stddev of first two series
		{math.NaN(), math.NaN(), 3.53553, 3.53553, 3.53553},
		// stddev of third, fourth, and fifth series
		{45.0925, 94.51631, 145.71662, 197.31531, 249.06492},
		// stddev of sixth series
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
	}

	expectedMetas = []block.SeriesMeta{
		{Name: StandardDeviationType, Tags: models.Tags{}},
		{Name: StandardDeviationType, Tags: models.Tags{"b": "2"}},
		{Name: StandardDeviationType, Tags: models.Tags{"c": "3"}},
	}

	compareValues(t, sink.Metas, expectedMetas, sink.Values, expected)
	assert.Equal(t, bounds, sink.Meta.Bounds)
}
