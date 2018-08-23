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
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	op, err := NewAggregationOp(StandardDeviationType, NodeParams{
		MatchingTags: []string{"a"}, Without: true,
	})
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

	test.CompareValues(t, sink.Metas, expectedMetas, sink.Values, expected)
	assert.Equal(t, bounds, sink.Meta.Bounds)

	c, sink = executor.NewControllerWithSink(parser.NodeID(1))
	op, err = NewAggregationOp(StandardDeviationType, NodeParams{
		MatchingTags: []string{"a"}, Without: true,
	})
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

	test.CompareValues(t, sink.Metas, expectedMetas, sink.Values, expected)
	assert.Equal(t, bounds, sink.Meta.Bounds)
}
