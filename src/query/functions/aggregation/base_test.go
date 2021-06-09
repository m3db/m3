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
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	seriesMetas = []block.SeriesMeta{
		{Tags: test.StringTagsToTags(test.StringTags{{N: "a", V: "1"}, {N: "d", V: "4"}})},
		{Tags: test.StringTagsToTags(test.StringTags{{N: "a", V: "1"}, {N: "d", V: "4"}})},
		{Tags: test.StringTagsToTags(test.StringTags{{N: "a", V: "1"}, {N: "b", V: "2"}, {N: "d", V: "4"}})},
		{Tags: test.StringTagsToTags(test.StringTags{{N: "a", V: "2"}, {N: "b", V: "2"}, {N: "d", V: "4"}})},
		{Tags: test.StringTagsToTags(test.StringTags{{N: "b", V: "2"}, {N: "d", V: "4"}})},
		{Tags: test.StringTagsToTags(test.StringTags{{N: "c", V: "3"}, {N: "d", V: "4"}})},
	}
	v = [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
		{10, 20, 30, 40, 50},
		{50, 60, 70, 80, 90},
		{100, 200, 300, 400, 500},
		{600, 700, 800, 900, 1000},
	}

	bounds = models.Bounds{
		Start:    xtime.Now(),
		Duration: time.Minute * 5,
		StepSize: time.Minute,
	}

	typeBytes = []byte(StandardDeviationType)
)

func processAggregationOp(t *testing.T, op parser.Params) *executor.SinkNode {
	bl := test.NewBlockFromValuesWithSeriesMeta(bounds, seriesMetas, v)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	node := op.(baseOp).Node(c, transform.Options{})
	err := node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), bl)
	require.NoError(t, err)
	return sink
}

func TestFunctionFilteringWithA(t *testing.T) {
	op, err := NewAggregationOp(StandardDeviationType, NodeParams{
		MatchingTags: [][]byte{[]byte("a")}, Without: false,
	})
	require.NoError(t, err)
	sink := processAggregationOp(t, op)
	expected := [][]float64{
		// stddev of fifth and sixth series
		{250, 250, 250, 250, 250},
		// stddev of first three series
		{5, 7, 12.19289, 16.39105, 20.60744},
		// stddev of fourth series
		{0, 0, 0, 0, 0},
	}

	expectedMetas := []block.SeriesMeta{
		{Name: typeBytes, Tags: models.EmptyTags()},
		{Name: typeBytes, Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("a"), Value: []byte("1")}})},
		{Name: typeBytes, Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("a"), Value: []byte("2")}})},
	}
	expectedMetaTags := models.EmptyTags()

	test.CompareValuesInOrder(t, sink.Metas, expectedMetas, sink.Values, expected)
	assert.Equal(t, bounds, sink.Meta.Bounds)
	assert.Equal(t, expectedMetaTags.Tags, sink.Meta.Tags.Tags)
}

func TestFunctionFilteringWithoutA(t *testing.T) {
	op, err := NewAggregationOp(StandardDeviationType, NodeParams{
		MatchingTags: [][]byte{[]byte("a")}, Without: true,
	})
	require.NoError(t, err)
	sink := processAggregationOp(t, op)
	expected := [][]float64{
		// stddev of third, fourth, and fifth series
		{36.81787, 77.17225, 118.97712, 161.10728, 203.36065},
		// stddev of sixth series
		{0, 0, 0, 0, 0},
		// stddev of first two series
		{0, 0, 2.5, 2.5, 2.5},
	}

	expectedMetas := []block.SeriesMeta{
		{Name: typeBytes, Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("b"), Value: []byte("2")}})},
		{Name: typeBytes, Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("c"), Value: []byte("3")}})},
		{Name: typeBytes, Tags: models.EmptyTags()},
	}

	expectedMetaTags := test.TagSliceToTags([]models.Tag{{Name: []byte("d"), Value: []byte("4")}})
	test.CompareValuesInOrder(t, sink.Metas, expectedMetas, sink.Values, expected)
	assert.Equal(t, bounds, sink.Meta.Bounds)
	assert.Equal(t, expectedMetaTags.Tags, sink.Meta.Tags.Tags)
}

func TestFunctionFilteringWithD(t *testing.T) {
	op, err := NewAggregationOp(StandardDeviationType, NodeParams{
		MatchingTags: [][]byte{[]byte("d")}, Without: false,
	})
	require.NoError(t, err)
	sink := processAggregationOp(t, op)
	expected := [][]float64{
		// stddev of all series
		{226.75096, 260.61343, 286.42611, 325.77587, 366.35491},
	}

	expectedMetas := []block.SeriesMeta{
		{Name: typeBytes, Tags: models.EmptyTags()},
	}

	expectedMetaTags := test.TagSliceToTags([]models.Tag{{Name: []byte("d"), Value: []byte("4")}})
	test.CompareValuesInOrder(t, sink.Metas, expectedMetas, sink.Values, expected)
	assert.Equal(t, bounds, sink.Meta.Bounds)
	assert.Equal(t, expectedMetaTags.Tags, sink.Meta.Tags.Tags)
}

func TestFunctionFilteringWithoutD(t *testing.T) {
	op, err := NewAggregationOp(StandardDeviationType, NodeParams{
		MatchingTags: [][]byte{[]byte("d")}, Without: true,
	})
	require.NoError(t, err)
	sink := processAggregationOp(t, op)

	expected := [][]float64{
		// stddev of first two series
		{0, 0, 2.5, 2.5, 2.5},
		// stddev of third series
		{0, 0, 0, 0, 0},
		// stddev of fourth series
		{0, 0, 0, 0, 0},
		// stddev of fifth series
		{0, 0, 0, 0, 0},
		// stddev of sixth series
		{0, 0, 0, 0, 0},
	}

	expectedMetas := []block.SeriesMeta{
		{Name: typeBytes, Tags: test.StringTagsToTags(test.StringTags{{N: "a", V: "1"}})},
		{Name: typeBytes, Tags: test.StringTagsToTags(test.StringTags{{N: "a", V: "1"}, {N: "b", V: "2"}})},
		{Name: typeBytes, Tags: test.StringTagsToTags(test.StringTags{{N: "a", V: "2"}, {N: "b", V: "2"}})},
		{Name: typeBytes, Tags: test.StringTagsToTags(test.StringTags{{N: "b", V: "2"}})},
		{Name: typeBytes, Tags: test.StringTagsToTags(test.StringTags{{N: "c", V: "3"}})},
	}
	expectedMetaTags := models.EmptyTags()

	test.CompareValuesInOrder(t, sink.Metas, expectedMetas, sink.Values, expected)
	assert.Equal(t, bounds, sink.Meta.Bounds)
	assert.Equal(t, expectedMetaTags.Tags, sink.Meta.Tags.Tags)
}
