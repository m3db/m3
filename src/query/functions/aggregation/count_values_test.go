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
	"testing"

	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
	seriesMetas = []block.SeriesMeta{
		{Tags: models.FromMap(map[string]string{"d": "4"})},
		{Tags: models.FromMap(map[string]string{"d": "4"})},

		{Tags: models.FromMap(map[string]string{"b": "2", "d": "4"})},
		{Tags: models.FromMap(map[string]string{"b": "2", "d": "4"})},
		{Tags: models.FromMap(map[string]string{"b": "2", "d": "4"})},

		{Tags: models.FromMap(map[string]string{"c": "3", "d": "4"})},
	}

	v = [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},

		{10, 20, 30, 40, 50},
		{50, 60, 70, 80, 90},
		{100, 200, 300, 400, 500},

		{600, 700, 800, 900, 1000},
	}
*/

func processCountValuesOp(t *testing.T, op parser.Params) *executor.SinkNode {
	bl := test.NewBlockFromValuesWithSeriesMeta(bounds, seriesMetas, v)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	node := op.(countValuesOp).Node(c, transform.Options{})
	err := node.Process(parser.NodeID(0), bl)
	require.NoError(t, err)
	return sink
}

func TestProcessCountValuesFunctionFilteringWithoutA(t *testing.T) {
	op, err := NewCountValuesOp(CountValuesType, NodeParams{
		MatchingTags: []string{"a"}, Without: true, StringParameter: "prefix",
	})
	require.NoError(t, err)
	processCountValuesOp(t, op)
	// expected := [][]float64{
	// 	// Taking bottomk(1) of first two series, keeping both series
	// 	{0, math.NaN(), 2, 3, 4},
	// 	{math.NaN(), 6, math.NaN(), math.NaN(), math.NaN()},
	// 	// Taking bottomk(1) of third, fourth, and fifth two series, keeping all series
	// 	{10, 20, 30, 40, 50},
	// 	{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
	// 	{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
	// 	// Taking bottomk(1) of last series, keeping it
	// 	{600, 700, 800, 900, 1000},
	// }

	// // Should have the same metas as when started
	// assert.Equal(t, seriesMetas, sink.Metas)
	// test.EqualsWithNansWithDelta(t, expected, sink.Values, math.Pow10(-5))
	// assert.Equal(t, bounds, sink.Meta.Bounds)
}

func TestCountValuesFn(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5, 6, 7, 1}
	buckets := []int{0, 1, 7}
	actual := countValuesFn(values, buckets)
	assert.Equal(t, 2.0, actual[1])
	assert.Equal(t, 1.0, actual[2])
}
