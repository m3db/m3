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
	"fmt"
	"math"
	"testing"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/compare"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTakeInstantFn(t *testing.T) {
	valuesMin := []float64{1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1, 9.1}
	buckets := [][]int{{0, 1, 2, 3}, {4}, {5, 6, 7, 8}}

	var (
		seriesMetasTakeOrdered = []block.SeriesMeta{
			{Tags: test.StringTagsToTags(test.StringTags{{N: "job", V: "api-server"}, {N: "instance", V: "0"}, {N: "group", V: "production"}})},
			{Tags: test.StringTagsToTags(test.StringTags{{N: "job", V: "api-server"}, {N: "instance", V: "1"}, {N: "group", V: "production"}})},
			{Tags: test.StringTagsToTags(test.StringTags{{N: "job", V: "api-server"}, {N: "instance", V: "2"}, {N: "group", V: "production"}})},
			{Tags: test.StringTagsToTags(test.StringTags{{N: "job", V: "api-server"}, {N: "instance", V: "0"}, {N: "group", V: "canary"}})},
			{Tags: test.StringTagsToTags(test.StringTags{{N: "job", V: "api-server"}, {N: "instance", V: "1"}, {N: "group", V: "canary"}})},
			{Tags: test.StringTagsToTags(test.StringTags{{N: "job", V: "app-server"}, {N: "instance", V: "0"}, {N: "group", V: "production"}})},
			{Tags: test.StringTagsToTags(test.StringTags{{N: "job", V: "app-server"}, {N: "instance", V: "1"}, {N: "group", V: "production"}})},
			{Tags: test.StringTagsToTags(test.StringTags{{N: "job", V: "app-server"}, {N: "instance", V: "0"}, {N: "group", V: "canary"}})},
			{Tags: test.StringTagsToTags(test.StringTags{{N: "job", V: "app-server"}, {N: "instance", V: "1"}, {N: "group", V: "canary"}})},
		}
	)

	expectedMin := []valueAndMeta{
		{val: 1.1, seriesMeta: seriesMetasTakeOrdered[0]},
		{val: 2.1, seriesMeta: seriesMetasTakeOrdered[1]},
		{val: 3.1, seriesMeta: seriesMetasTakeOrdered[2]},

		{val: 5.1, seriesMeta: seriesMetasTakeOrdered[4]},

		{val: 6.1, seriesMeta: seriesMetasTakeOrdered[5]},
		{val: 7.1, seriesMeta: seriesMetasTakeOrdered[6]},
		{val: 8.1, seriesMeta: seriesMetasTakeOrdered[7]},
	}

	size := 3
	minHeap := utils.NewFloatHeap(false, size)
	actual := takeInstantFn(minHeap, valuesMin, buckets, seriesMetasTakeOrdered) //9

	actualString := fmt.Sprint(actual)
	expectedString := fmt.Sprint(expectedMin)

	assert.EqualValues(t, expectedString, actualString)

	valuesMax := []float64{1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1, 9.1}
	expectedMax := []valueAndMeta{
		{val: 4.1, seriesMeta: seriesMetasTakeOrdered[3]},
		{val: 3.1, seriesMeta: seriesMetasTakeOrdered[2]},
		{val: 2.1, seriesMeta: seriesMetasTakeOrdered[1]},

		{val: 5.1, seriesMeta: seriesMetasTakeOrdered[4]},

		{val: 9.1, seriesMeta: seriesMetasTakeOrdered[8]},
		{val: 8.1, seriesMeta: seriesMetasTakeOrdered[7]},
		{val: 7.1, seriesMeta: seriesMetasTakeOrdered[6]},
	}

	maxHeap := utils.NewFloatHeap(true, size)
	actual = takeInstantFn(maxHeap, valuesMax, buckets, seriesMetasTakeOrdered)
	actualString = fmt.Sprint(actual)
	expectedString = fmt.Sprint(expectedMax)

	assert.EqualValues(t, expectedString, actualString)
}

func TestTakeFn(t *testing.T) {
	valuesMin := []float64{1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1}
	buckets := [][]int{{0, 1, 2, 3}, {4}, {5, 6, 7}}
	expectedMin := []float64{1.1, 2.1, 3.1, math.NaN(), 5.1, 6.1, 7.1, 8.1}
	size := 3
	minHeap := utils.NewFloatHeap(false, size)

	actual := takeFn(minHeap, valuesMin, buckets)
	compare.EqualsWithNans(t, expectedMin, actual)
	compare.EqualsWithNans(t, expectedMin, valuesMin)

	valuesMax := []float64{1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1}
	expectedMax := []float64{math.NaN(), 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1}

	maxHeap := utils.NewFloatHeap(true, size)
	actual = takeFn(maxHeap, valuesMax, buckets)
	compare.EqualsWithNans(t, expectedMax, actual)
	compare.EqualsWithNans(t, expectedMax, valuesMax)

	valuesQuantile := []float64{1.1, 2.1, 3.1, 4.1, 5.1}
	actualQ := bucketedQuantileFn(0, valuesQuantile, []int{0, 1, 2, 3})
	compare.EqualsWithNans(t, 1.1, actualQ)
}

func processTakeOp(t *testing.T, op parser.Params) *executor.SinkNode {
	bl := test.NewBlockFromValuesWithSeriesMeta(bounds, seriesMetas, v)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	node := op.(takeOp).Node(c, transform.Options{})
	err := node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), bl)
	require.NoError(t, err)
	return sink
}

func TestTakeBottomFunctionFilteringWithoutA(t *testing.T) {
	op, err := NewTakeOp(BottomKType, NodeParams{
		MatchingTags: [][]byte{[]byte("a")}, Without: true, Parameter: 1,
	})
	require.NoError(t, err)
	sink := processTakeOp(t, op)
	expected := [][]float64{
		// Taking bottomk(1) of first two series, keeping both series
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, math.NaN(), math.NaN(), math.NaN()},
		// Taking bottomk(1) of third, fourth, and fifth two series, keeping all series
		{10, 20, 30, 40, 50},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		// Taking bottomk(1) of last series, keeping it
		{600, 700, 800, 900, 1000},
	}

	// Should have the same metas as when started
	assert.Equal(t, seriesMetas, sink.Metas)
	compare.EqualsWithNansWithDelta(t, expected, sink.Values, math.Pow10(-5))
	assert.Equal(t, bounds, sink.Meta.Bounds)
}

func TestTakeTopFunctionFilteringWithoutA(t *testing.T) {
	op, err := NewTakeOp(TopKType, NodeParams{
		MatchingTags: [][]byte{[]byte("a")}, Without: true, Parameter: 1,
	})
	require.NoError(t, err)
	sink := processTakeOp(t, op)

	expected := [][]float64{
		// Taking bottomk(1) of first two series, keeping both series
		{0, math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), 6, 7, 8, 9},
		// Taking bottomk(1) of third, fourth, and fifth two series, keeping all series
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		{100, 200, 300, 400, 500},
		// Taking bottomk(1) of last series, keeping it
		{600, 700, 800, 900, 1000},
	}

	// Should have the same metas as when started
	assert.Equal(t, seriesMetas, sink.Metas)
	compare.EqualsWithNansWithDelta(t, expected, sink.Values, math.Pow10(-5))
	assert.Equal(t, bounds, sink.Meta.Bounds)
}

func TestTakeTopFunctionFilteringWithoutALessThanOne(t *testing.T) {
	op, err := NewTakeOp(TopKType, NodeParams{
		MatchingTags: [][]byte{[]byte("a")}, Without: true, Parameter: -1,
	})
	require.NoError(t, err)
	sink := processTakeOp(t, op)
	expected := [][]float64{
		// Taking bottomk(1) of first two series, keeping both series
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		// Taking bottomk(1) of third, fourth, and fifth two series, keeping all series
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		// Taking bottomk(1) of last series, keeping it
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
	}

	// Should have the same metas as when started
	assert.Equal(t, seriesMetas, sink.Metas)
	compare.EqualsWithNansWithDelta(t, expected, sink.Values, math.Pow10(-5))
	assert.Equal(t, bounds, sink.Meta.Bounds)
}

func TestTakeOpParamIsNaN(t *testing.T) {
	op, err := NewTakeOp(TopKType, NodeParams{
		Parameter: math.NaN(),
	})
	require.NoError(t, err)
	assert.True(t, op.(takeOp).k < 0)
}
