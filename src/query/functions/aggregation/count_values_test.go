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
	"testing"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPadValuesWithNans(t *testing.T) {
	// When padding necessary adds enough NaNs
	vals := bucketColumn{1}
	actual := padValuesWithNaNs(vals, 4)
	test.EqualsWithNans(t,
		[]float64{1, math.NaN(), math.NaN(), math.NaN()},
		[]float64(actual),
	)

	// When no padding necessary should do nothing
	vals = bucketColumn{1, 2, 3, 4}
	actual = padValuesWithNaNs(vals, 4)
	test.EqualsWithNans(t, []float64{1, 2, 3, 4}, []float64(actual))

	// When vals is longer than padding length, should do nothing
	vals = bucketColumn{1, 2, 3, 4, 5}
	actual = padValuesWithNaNs(vals, 4)
	test.EqualsWithNans(t, []float64{1, 2, 3, 4, 5}, []float64(actual))
}

func TestCountValuesFn(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5, 6, 7, 1}
	buckets := []int{0, 1, 7}
	actual := countValuesFn(values, buckets)
	assert.Equal(t, 2.0, actual[1])
	assert.Equal(t, 1.0, actual[2])
}

func tagsToSeriesMeta(tags []models.Tags) []block.SeriesMeta {
	expectedMetas := make([]block.SeriesMeta, len(tags))
	for i, m := range tags {
		expectedMetas[i] = block.SeriesMeta{
			Name: []byte(CountValuesType),
			Tags: m,
		}
	}
	return expectedMetas
}

func processCountValuesOp(
	t *testing.T,
	op parser.Params,
	metas []block.SeriesMeta,
	vals [][]float64,
) *executor.SinkNode {
	bl := test.NewBlockFromValuesWithSeriesMeta(bounds, metas, vals)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	node := op.(countValuesOp).Node(c, transform.Options{})
	err := node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), bl)
	require.NoError(t, err)
	return sink
}

var (
	simpleMetas = []block.SeriesMeta{
		{Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("a"), Value: []byte("1")}, {Name: []byte("b"), Value: []byte("2")}})},
		{Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("a"), Value: []byte("1")}, {Name: []byte("b"), Value: []byte("2")}})},
		{Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("a"), Value: []byte("1")}, {Name: []byte("b"), Value: []byte("3")}})},
		{Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("a"), Value: []byte("1")}, {Name: []byte("b"), Value: []byte("3")}})},
	}

	simpleVals = [][]float64{
		{0, math.NaN(), 0, 0, 0},
		{0, math.NaN(), 0, 0, 0},
		{math.NaN(), 0, 0, 0, 0},
		{math.NaN(), 0, 0, 0, 0},
	}
)

func TestSimpleProcessCountValuesFunctionUnfiltered(t *testing.T) {
	tagName := "tag_name"
	op, err := NewCountValuesOp(CountValuesType, NodeParams{
		StringParameter: tagName,
	})
	require.NoError(t, err)
	sink := processCountValuesOp(t, op, simpleMetas, simpleVals)
	expected := [][]float64{{2, 2, 4, 4, 4}}
	expectedTags := []models.Tags{models.EmptyTags()}

	// Double check expected tags is the same length as expected values
	require.Equal(t, len(expectedTags), len(expected))
	assert.Equal(t, bounds, sink.Meta.Bounds)
	ex := test.TagSliceToTags([]models.Tag{{Name: []byte(tagName), Value: []byte("0")}})
	assert.Equal(t, ex.Tags, sink.Meta.Tags.Tags)
	test.CompareValuesInOrder(t, sink.Metas, tagsToSeriesMeta(expectedTags), sink.Values, expected)
}

func TestSimpleProcessCountValuesFunctionFilteringWithoutA(t *testing.T) {
	tagName := "tag_name"
	op, err := NewCountValuesOp(CountValuesType, NodeParams{
		MatchingTags: [][]byte{[]byte("a")}, Without: true, StringParameter: tagName,
	})
	require.NoError(t, err)
	sink := processCountValuesOp(t, op, simpleMetas, simpleVals)
	expected := [][]float64{
		{2, math.NaN(), 2, 2, 2},
		{math.NaN(), 2, 2, 2, 2},
	}
	expectedTags := test.TagSliceSliceToTagSlice([][]models.Tag{
		{{Name: []byte("b"), Value: []byte("2")}},
		{{Name: []byte("b"), Value: []byte("3")}},
	})

	// Double check expected tags is the same length as expected values
	require.Equal(t, len(expectedTags), len(expected))
	assert.Equal(t, bounds, sink.Meta.Bounds)
	exTags := test.TagSliceToTags([]models.Tag{{Name: []byte(tagName), Value: []byte("0")}})
	assert.Equal(t, exTags.Tags, sink.Meta.Tags.Tags)
	test.CompareValuesInOrder(t, sink.Metas, tagsToSeriesMeta(expectedTags), sink.Values, expected)
}

func TestCustomProcessCountValuesFunctionFilteringWithoutA(t *testing.T) {
	tagName := "tag_name"
	op, err := NewCountValuesOp(CountValuesType, NodeParams{
		MatchingTags: [][]byte{[]byte("a")}, Without: true, StringParameter: tagName,
	})
	require.NoError(t, err)
	ms := []block.SeriesMeta{
		{Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("a"), Value: []byte("1")}, {Name: []byte("b"), Value: []byte("2")}})},
		{Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("a"), Value: []byte("1")}, {Name: []byte("b"), Value: []byte("2")}})},
		{Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("a"), Value: []byte("1")}, {Name: []byte("b"), Value: []byte("3")}})},
		{Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("a"), Value: []byte("1")}, {Name: []byte("b"), Value: []byte("3")}})},
	}

	vs := [][]float64{
		{0, math.NaN(), 0, 2, 0},
		{0, math.NaN(), 1, 0, 3},
		{math.NaN(), 0, 1, 0, 2},
		{math.NaN(), 0, 0, 2, 3},
	}

	sink := processCountValuesOp(t, op, ms, vs)
	expected := [][]float64{
		{2, math.NaN(), 1, 1, 1},
		{math.NaN(), math.NaN(), 1, math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), 1, math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 1},

		{math.NaN(), 2, 1, 1, math.NaN()},
		{math.NaN(), math.NaN(), 1, math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), 1, 1},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 1},
	}

	expectedTags := test.TagSliceSliceToTagSlice([][]models.Tag{
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("0")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("1")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("2")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("3")}},

		{{Name: []byte("b"), Value: []byte("3")}, {Name: []byte(tagName), Value: []byte("0")}},
		{{Name: []byte("b"), Value: []byte("3")}, {Name: []byte(tagName), Value: []byte("1")}},
		{{Name: []byte("b"), Value: []byte("3")}, {Name: []byte(tagName), Value: []byte("2")}},
		{{Name: []byte("b"), Value: []byte("3")}, {Name: []byte(tagName), Value: []byte("3")}},
	})

	// Double check expected tags is the same length as expected values
	require.Equal(t, len(expectedTags), len(expected))
	assert.Equal(t, bounds, sink.Meta.Bounds)
	assert.Equal(t, models.EmptyTags(), sink.Meta.Tags)
	test.CompareValuesInOrder(t, sink.Metas, tagsToSeriesMeta(expectedTags), sink.Values, expected)
}

func TestSimpleProcessCountValuesFunctionFilteringWithA(t *testing.T) {
	tagName := "tag_name_0"
	op, err := NewCountValuesOp(CountValuesType, NodeParams{
		MatchingTags: [][]byte{[]byte("a")}, Without: false, StringParameter: tagName,
	})
	require.NoError(t, err)
	sink := processCountValuesOp(t, op, simpleMetas, simpleVals)
	expected := [][]float64{{2, 2, 4, 4, 4}}
	expectedTags := []models.Tags{models.EmptyTags()}

	// Double check expected tags is the same length as expected values
	require.Equal(t, len(expectedTags), len(expected))
	assert.Equal(t, bounds, sink.Meta.Bounds)
	assert.Equal(t, test.TagSliceToTags([]models.Tag{{Name: []byte(tagName), Value: []byte("0")},
		{Name: []byte("a"), Value: []byte("1")}}).Tags, sink.Meta.Tags.Tags)
	test.CompareValuesInOrder(t, sink.Metas, tagsToSeriesMeta(expectedTags), sink.Values, expected)
}

func TestProcessCountValuesFunctionFilteringWithoutA(t *testing.T) {
	tagName := "tag_name"
	op, err := NewCountValuesOp(CountValuesType, NodeParams{
		MatchingTags: [][]byte{[]byte("a")}, Without: true, StringParameter: tagName,
	})
	require.NoError(t, err)
	sink := processCountValuesOp(t, op, seriesMetas, v)

	expected := [][]float64{
		// No shared values between series 1 and 2
		{1, math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), 1, math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), 1, math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), 1, math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), 1, math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), 1, math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 1},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 1},

		// One shared value between series 3, 4 and 5
		{1, math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		{1, math.NaN(), math.NaN(), math.NaN(), 1},
		{1, math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), 1, math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), 1, math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), 1, math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), 1, math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), 1, math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), 1, math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), 1, math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), 1, math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), 1, math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 1},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 1},

		// No shared values in series 6
		{1, math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), 1, math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), 1, math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), 1, math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 1},
	}

	expectedTags := test.TagSliceSliceToTagSlice([][]models.Tag{
		// No shared values between series 1 and 2, but two NaNs
		{{Name: []byte(tagName), Value: []byte("0")}},
		{{Name: []byte(tagName), Value: []byte("6")}},
		{{Name: []byte(tagName), Value: []byte("2")}},
		{{Name: []byte(tagName), Value: []byte("7")}},
		{{Name: []byte(tagName), Value: []byte("3")}},
		{{Name: []byte(tagName), Value: []byte("8")}},
		{{Name: []byte(tagName), Value: []byte("4")}},
		{{Name: []byte(tagName), Value: []byte("9")}},

		// One shared value between series 3, 4 and 5,
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("10")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("50")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("100")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("20")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("60")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("200")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("30")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("70")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("300")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("40")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("80")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("400")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("90")}},
		{{Name: []byte("b"), Value: []byte("2")}, {Name: []byte(tagName), Value: []byte("500")}},

		// No shared values in series 6
		{{Name: []byte("c"), Value: []byte("3")}, {Name: []byte(tagName), Value: []byte("600")}},
		{{Name: []byte("c"), Value: []byte("3")}, {Name: []byte(tagName), Value: []byte("700")}},
		{{Name: []byte("c"), Value: []byte("3")}, {Name: []byte(tagName), Value: []byte("800")}},
		{{Name: []byte("c"), Value: []byte("3")}, {Name: []byte(tagName), Value: []byte("900")}},
		{{Name: []byte("c"), Value: []byte("3")}, {Name: []byte(tagName), Value: []byte("1000")}},
	})

	// Double check expected tags is the same length as expected values
	require.Equal(t, len(expectedTags), len(expected))
	assert.Equal(t, bounds, sink.Meta.Bounds)
	ex := test.TagSliceToTags([]models.Tag{{Name: []byte("d"), Value: []byte("4")}})
	assert.Equal(t, ex.Tags, sink.Meta.Tags.Tags)
	test.CompareValues(t, sink.Metas, tagsToSeriesMeta(expectedTags), sink.Values, expected)
}

func TestShouldFailWhenInvalidLabelName(t *testing.T) {
	tagName := "tag-name"
	op, _ := NewCountValuesOp(CountValuesType, NodeParams{
		StringParameter: tagName,
	})
	bl := test.NewBlockFromValuesWithSeriesMeta(bounds, simpleMetas, simpleVals)
	c, _ := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	node := op.(countValuesOp).Node(c, transform.Options{})
	err := node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), bl)
	require.Error(t, err)
}
