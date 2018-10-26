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

package binary

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrWithExactValues(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	block1 := test.NewBlockFromValues(bounds, values)
	block2 := test.NewBlockFromValues(bounds, values)

	op, err := NewOp(
		OrType,
		NodeParams{
			LNode:          parser.NodeID(0),
			RNode:          parser.NodeID(1),
			VectorMatching: &VectorMatching{},
		},
	)
	require.NoError(t, err)

	c, sink := executor.NewControllerWithSink(parser.NodeID(2))
	node := op.(baseOp).Node(c, transform.Options{})

	err = node.Process(models.NoopQueryContext(), parser.NodeID(1), block2)
	require.NoError(t, err)
	err = node.Process(models.NoopQueryContext(), parser.NodeID(0), block1)
	require.NoError(t, err)
	assert.Equal(t, values, sink.Values)
}

func TestOrWithSomeValues(t *testing.T) {
	values1, bounds := test.GenerateValuesAndBounds(nil, nil)
	block1 := test.NewBlockFromValues(bounds, values1)

	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	block2 := test.NewBlockFromValues(bounds, v)

	op, err := NewOp(
		OrType,
		NodeParams{
			LNode:          parser.NodeID(0),
			RNode:          parser.NodeID(1),
			VectorMatching: &VectorMatching{},
		},
	)
	require.NoError(t, err)

	c, sink := executor.NewControllerWithSink(parser.NodeID(2))
	node := op.(baseOp).Node(c, transform.Options{})

	err = node.Process(models.NoopQueryContext(), parser.NodeID(1), block2)
	require.NoError(t, err)
	err = node.Process(models.NoopQueryContext(), parser.NodeID(0), block1)
	require.NoError(t, err)
	// NAN values should be filled
	expected := values1

	test.EqualsWithNans(t, expected, sink.Values)
}

func generateMetaDataWithTagsInRange(fromRange, toRange int) []block.SeriesMeta {
	length := toRange - fromRange
	meta := make([]block.SeriesMeta, length)
	for i := 0; i < length; i++ {
		strIdx := fmt.Sprint(fromRange + i)
		tags := test.TagSliceToTags([]models.Tag{{Name: []byte(strIdx), Value: []byte(strIdx)}})
		meta[i] = block.SeriesMeta{
			Tags: tags,
			Name: strIdx,
		}
	}
	return meta
}

var missingTests = []struct {
	name     string
	lhs      []block.SeriesMeta
	rhs      []block.SeriesMeta
	expected []int
}{
	{"equal tags", generateMetaDataWithTagsInRange(0, 5), generateMetaDataWithTagsInRange(0, 5), []int{}},
	{"empty rhs", generateMetaDataWithTagsInRange(0, 5), []block.SeriesMeta{}, []int{}},
	{"empty lhs", []block.SeriesMeta{}, generateMetaDataWithTagsInRange(0, 5), []int{0, 1, 2, 3, 4}},
	{"longer rhs", generateMetaDataWithTagsInRange(0, 5), generateMetaDataWithTagsInRange(-1, 6), []int{0, 6}},
	{"no overlap", generateMetaDataWithTagsInRange(0, 5), generateMetaDataWithTagsInRange(6, 9), []int{0, 1, 2}},
}

func TestMissing(t *testing.T) {
	matching := &VectorMatching{}

	for _, tt := range missingTests {
		t.Run(tt.name, func(t *testing.T) {
			missing, _ := missing(matching, tt.lhs, tt.rhs)
			assert.Equal(t, tt.expected, missing)
		})
	}
}

var orTests = []struct {
	name          string
	lhsMeta       []block.SeriesMeta
	lhs           [][]float64
	rhsMeta       []block.SeriesMeta
	rhs           [][]float64
	expectedMetas []block.SeriesMeta
	expected      [][]float64
	err           error
}{
	{
		"valid, equal tags",
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		test.NewSeriesMeta("a", 2),
		[][]float64{{3, 4}, {30, 40}},
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		nil,
	},
	{
		"valid, some overlap",
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		test.NewSeriesMeta("a", 3),
		[][]float64{{3, 4}, {30, 40}, {50, 60}},
		test.NewSeriesMeta("a", 3),
		[][]float64{{1, 2}, {10, 20}, {50, 60}},
		nil,
	},
	{
		"valid, equal size",
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		test.NewSeriesMeta("b", 2),
		[][]float64{{3, 4}, {30, 40}},
		append(test.NewSeriesMeta("a", 2), test.NewSeriesMeta("b", 2)...),
		[][]float64{{1, 2}, {10, 20}, {3, 4}, {30, 40}},
		nil,
	},
	{
		"valid, longer rhs",
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		test.NewSeriesMeta("b", 3),
		[][]float64{{3, 4}, {30, 40}, {300, 400}},
		append(test.NewSeriesMeta("a", 2), test.NewSeriesMeta("b", 3)...),
		[][]float64{{1, 2}, {10, 20}, {3, 4}, {30, 40}, {300, 400}},
		nil,
	},
	{
		"valid, longer lhs",
		test.NewSeriesMeta("a", 3),
		[][]float64{{1, 2}, {10, 20}, {100, 200}},
		test.NewSeriesMeta("b", 2),
		[][]float64{{3, 4}, {30, 40}},
		append(test.NewSeriesMeta("a", 3), test.NewSeriesMeta("b", 2)...),
		[][]float64{{1, 2}, {10, 20}, {100, 200}, {3, 4}, {30, 40}},
		nil,
	},
	{
		"mismatched step counts",
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2, 3}, {10, 20, 30}},
		test.NewSeriesMeta("b", 2),
		[][]float64{{3, 4}, {30, 40}},
		append(test.NewSeriesMeta("a", 2), test.NewSeriesMeta("b", 2)...),
		[][]float64{{1, 2}, {10, 20}, {3, 4}, {30, 40}},
		errMismatchedStepCounts,
	},
}

func TestOrs(t *testing.T) {
	now := time.Now()
	for _, tt := range orTests {
		t.Run(tt.name, func(t *testing.T) {

			op, err := NewOp(
				OrType,
				NodeParams{
					LNode:          parser.NodeID(0),
					RNode:          parser.NodeID(1),
					VectorMatching: &VectorMatching{},
				},
			)
			require.NoError(t, err)

			c, sink := executor.NewControllerWithSink(parser.NodeID(2))
			node := op.(baseOp).Node(c, transform.Options{})
			bounds := models.Bounds{
				Start:    now,
				Duration: time.Minute * time.Duration(len(tt.lhs[0])),
				StepSize: time.Minute,
			}

			lhs := test.NewBlockFromValuesWithSeriesMeta(bounds, tt.lhsMeta, tt.lhs)
			err = node.Process(models.NoopQueryContext(), parser.NodeID(0), lhs)
			require.NoError(t, err)

			bounds = models.Bounds{
				Start:    now,
				Duration: time.Minute * time.Duration(len(tt.rhs[0])),
				StepSize: time.Minute,
			}

			rhs := test.NewBlockFromValuesWithSeriesMeta(bounds, tt.rhsMeta, tt.rhs)
			err = node.Process(models.NoopQueryContext(), parser.NodeID(1), rhs)
			if tt.err != nil {
				require.EqualError(t, err, tt.err.Error())
				return
			}

			require.NoError(t, err)
			test.EqualsWithNans(t, tt.expected, sink.Values)
			assert.Equal(t, tt.expectedMetas, sink.Metas)
		})
	}
}

func TestOrsBoundsError(t *testing.T) {
	tt := orTests[0]
	bounds := models.Bounds{
		Start:    time.Now(),
		Duration: time.Minute * time.Duration(len(tt.lhs[0])),
		StepSize: time.Minute,
	}

	op, err := NewOp(
		OrType,
		NodeParams{
			LNode:          parser.NodeID(0),
			RNode:          parser.NodeID(1),
			VectorMatching: &VectorMatching{},
		},
	)
	require.NoError(t, err)

	c, _ := executor.NewControllerWithSink(parser.NodeID(2))
	node := op.(baseOp).Node(c, transform.Options{})

	lhs := test.NewBlockFromValuesWithSeriesMeta(bounds, tt.lhsMeta, tt.lhs)
	err = node.Process(models.NoopQueryContext(), parser.NodeID(0), lhs)
	require.NoError(t, err)

	differentBounds := models.Bounds{
		Start:    bounds.Start.Add(1),
		Duration: bounds.Duration,
		StepSize: bounds.StepSize,
	}
	rhs := test.NewBlockFromValuesWithSeriesMeta(differentBounds, tt.rhsMeta, tt.rhs)
	err = node.Process(models.NoopQueryContext(), parser.NodeID(1), rhs)
	require.EqualError(t, err, errMismatchedBounds.Error())
}

func createSeriesMeta() []block.SeriesMeta {
	return []block.SeriesMeta{
		{Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("foo"), Value: []byte("bar")}})},
		{Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("baz"), Value: []byte("qux")}})},
	}
}

func TestOrCombinedMetadata(t *testing.T) {
	op, err := NewOp(
		OrType,
		NodeParams{
			LNode:          parser.NodeID(0),
			RNode:          parser.NodeID(1),
			VectorMatching: &VectorMatching{},
		},
	)
	require.NoError(t, err)

	c, sink := executor.NewControllerWithSink(parser.NodeID(2))
	node := op.(baseOp).Node(c, transform.Options{})

	bounds := models.Bounds{
		Start:    time.Now(),
		Duration: time.Minute * 2,
		StepSize: time.Minute,
	}

	strTags := test.StringTags{{"a", "b"}, {"c", "d"}, {"e", "f"}}
	lhsMeta := block.Metadata{
		Bounds: bounds,
		Tags:   test.StringTagsToTags(strTags),
	}

	lSeriesMeta := createSeriesMeta()
	lhs := test.NewBlockFromValuesWithMetaAndSeriesMeta(
		lhsMeta,
		lSeriesMeta,
		[][]float64{{1, 2}, {10, 20}})

	err = node.Process(models.NoopQueryContext(), parser.NodeID(0), lhs)
	require.NoError(t, err)

	strTags = test.StringTags{{"a", "b"}, {"c", "*d"}, {"g", "h"}}
	rhsMeta := block.Metadata{
		Bounds: bounds,
		Tags:   test.StringTagsToTags(strTags),
	}

	// NB (arnikola): since common tags for the series differ,
	// all four series should be included in the combined
	// block despite the individual seriesMeta tags being the same.
	rSeriesMeta := createSeriesMeta()
	rhs := test.NewBlockFromValuesWithMetaAndSeriesMeta(
		rhsMeta,
		rSeriesMeta,
		[][]float64{{3, 4}, {30, 40}})

	err = node.Process(models.NoopQueryContext(), parser.NodeID(1), rhs)
	require.NoError(t, err)

	test.EqualsWithNans(t, [][]float64{{1, 2}, {10, 20}, {3, 4}, {30, 40}}, sink.Values)

	assert.Equal(t, sink.Meta.Bounds, bounds)
	exTags := test.TagSliceToTags([]models.Tag{{Name: []byte("a"), Value: []byte("b")}})
	assert.Equal(t, exTags.Tags, sink.Meta.Tags.Tags)

	stringTags := []test.StringTags{
		{{"c", "d"}, {"e", "f"}, {"foo", "bar"}},
		{{"baz", "qux"}, {"c", "d"}, {"e", "f"}},
		{{"c", "*d"}, {"foo", "bar"}, {"g", "h"}},
		{{"baz", "qux"}, {"c", "*d"}, {"g", "h"}},
	}

	tags := test.StringTagsSliceToTagSlice(stringTags)
	expectedMetas := make([]block.SeriesMeta, len(tags))
	for i, t := range tags {
		expectedMetas[i] = block.SeriesMeta{Tags: t}
	}

	assert.Equal(t, expectedMetas, sink.Metas)
}
