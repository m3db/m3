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
			Name: CountValuesType,
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
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	node := op.(countValuesOp).Node(c, transform.Options{})
	err := node.Process(parser.NodeID(0), bl)
	require.NoError(t, err)
	return sink
}

var (
	simpleMetas = []block.SeriesMeta{
		{Tags: models.Tags{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}}},
		{Tags: models.Tags{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}}},
		{Tags: models.Tags{{Name: "a", Value: "1"}, {Name: "b", Value: "3"}}},
		{Tags: models.Tags{{Name: "a", Value: "1"}, {Name: "b", Value: "3"}}},
	}

	simpleVals = [][]float64{
		{0, math.NaN(), 0, 0, 0},
		{0, math.NaN(), 0, 0, 0},
		{math.NaN(), 0, 0, 0, 0},
		{math.NaN(), 0, 0, 0, 0},
	}
)

func TestSimpleProcessCountValuesFunctionUnfiltered(t *testing.T) {
	tagName := "tag-name"
	op, err := NewCountValuesOp(CountValuesType, NodeParams{
		StringParameter: tagName,
	})
	require.NoError(t, err)
	sink := processCountValuesOp(t, op, simpleMetas, simpleVals)
	expected := [][]float64{{2, 2, 4, 4, 4}}
	expectedTags := []models.Tags{{}}

	// Double check expected tags is the same length as expected values
	require.Equal(t, len(expectedTags), len(expected))
	assert.Equal(t, bounds, sink.Meta.Bounds)
	assert.Equal(t, models.Tags{{Name: tagName, Value: "0"}}, sink.Meta.Tags)
	test.CompareValues(t, sink.Metas, tagsToSeriesMeta(expectedTags), sink.Values, expected)
}

func TestSimpleProcessCountValuesFunctionFilteringWithoutA(t *testing.T) {
	tagName := "tag-name"
	op, err := NewCountValuesOp(CountValuesType, NodeParams{
		MatchingTags: []string{"a"}, Without: true, StringParameter: tagName,
	})
	require.NoError(t, err)
	sink := processCountValuesOp(t, op, simpleMetas, simpleVals)
	expected := [][]float64{
		{2, math.NaN(), 2, 2, 2},
		{math.NaN(), 2, 2, 2, 2},
	}
	expectedTags := []models.Tags{
		{{Name: "b", Value: "2"}},
		{{Name: "b", Value: "3"}},
	}

	// Double check expected tags is the same length as expected values
	require.Equal(t, len(expectedTags), len(expected))
	assert.Equal(t, bounds, sink.Meta.Bounds)
	assert.Equal(t, models.Tags{{Name: tagName, Value: "0"}}, sink.Meta.Tags)
	test.CompareValues(t, sink.Metas, tagsToSeriesMeta(expectedTags), sink.Values, expected)
}

func TestCustomProcessCountValuesFunctionFilteringWithoutA(t *testing.T) {
	tagName := "tag-name"
	op, err := NewCountValuesOp(CountValuesType, NodeParams{
		MatchingTags: []string{"a"}, Without: true, StringParameter: tagName,
	})
	require.NoError(t, err)
	ms := []block.SeriesMeta{
		{Tags: models.Tags{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}}},
		{Tags: models.Tags{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}}},
		{Tags: models.Tags{{Name: "a", Value: "1"}, {Name: "b", Value: "3"}}},
		{Tags: models.Tags{{Name: "a", Value: "1"}, {Name: "b", Value: "3"}}},
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

	expectedTags := []models.Tags{
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "0"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "1"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "2"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "3"}},

		{{Name: "b", Value: "3"}, {Name: tagName, Value: "0"}},
		{{Name: "b", Value: "3"}, {Name: tagName, Value: "1"}},
		{{Name: "b", Value: "3"}, {Name: tagName, Value: "2"}},
		{{Name: "b", Value: "3"}, {Name: tagName, Value: "3"}},
	}

	// Double check expected tags is the same length as expected values
	require.Equal(t, len(expectedTags), len(expected))
	assert.Equal(t, bounds, sink.Meta.Bounds)
	assert.Equal(t, models.Tags{}, sink.Meta.Tags)
	test.CompareValues(t, sink.Metas, tagsToSeriesMeta(expectedTags), sink.Values, expected)
}

func TestSimpleProcessCountValuesFunctionFilteringWithA(t *testing.T) {
	tagName := "0_tag-name"
	op, err := NewCountValuesOp(CountValuesType, NodeParams{
		MatchingTags: []string{"a"}, Without: false, StringParameter: tagName,
	})
	require.NoError(t, err)
	sink := processCountValuesOp(t, op, simpleMetas, simpleVals)
	expected := [][]float64{{2, 2, 4, 4, 4}}
	expectedTags := []models.Tags{{}}

	// Double check expected tags is the same length as expected values
	require.Equal(t, len(expectedTags), len(expected))
	assert.Equal(t, bounds, sink.Meta.Bounds)
	assert.Equal(t, models.Tags{{Name: tagName, Value: "0"}, {Name: "a", Value: "1"}}, sink.Meta.Tags)
	test.CompareValues(t, sink.Metas, tagsToSeriesMeta(expectedTags), sink.Values, expected)
}

func TestProcessCountValuesFunctionFilteringWithoutA(t *testing.T) {
	tagName := "tag-name"
	op, err := NewCountValuesOp(CountValuesType, NodeParams{
		MatchingTags: []string{"a"}, Without: true, StringParameter: tagName,
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

	expectedTags := []models.Tags{
		// No shared values between series 1 and 2, but two NaNs
		{{Name: tagName, Value: "0"}},
		{{Name: tagName, Value: "6"}},
		{{Name: tagName, Value: "2"}},
		{{Name: tagName, Value: "7"}},
		{{Name: tagName, Value: "3"}},
		{{Name: tagName, Value: "8"}},
		{{Name: tagName, Value: "4"}},
		{{Name: tagName, Value: "9"}},

		// One shared value between series 3, 4 and 5,
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "10"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "50"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "100"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "20"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "60"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "200"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "30"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "70"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "300"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "40"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "80"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "400"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "90"}},
		{{Name: "b", Value: "2"}, {Name: tagName, Value: "500"}},

		// No shared values in series 6
		{{Name: "c", Value: "3"}, {Name: tagName, Value: "600"}},
		{{Name: "c", Value: "3"}, {Name: tagName, Value: "700"}},
		{{Name: "c", Value: "3"}, {Name: tagName, Value: "800"}},
		{{Name: "c", Value: "3"}, {Name: tagName, Value: "900"}},
		{{Name: "c", Value: "3"}, {Name: tagName, Value: "1000"}},
	}

	// Double check expected tags is the same length as expected values
	require.Equal(t, len(expectedTags), len(expected))
	assert.Equal(t, bounds, sink.Meta.Bounds)
	assert.Equal(t, models.Tags{{Name: "d", Value: "4"}}, sink.Meta.Tags)
	test.CompareValues(t, sink.Metas, tagsToSeriesMeta(expectedTags), sink.Values, expected)
}
