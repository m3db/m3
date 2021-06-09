// Copyright (c) 2020 Uber Technologies, Inc.
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

package linear

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

func TestShouldFailWhenOpTypeIsInvalid(t *testing.T) {
	_, err := NewSortOp("sortAsc")
	require.Error(t, err)
}

func TestSortAscInstant(t *testing.T) {
	op, err := NewSortOp(SortType)
	require.NoError(t, err)

	sink := processSortOp(t, op, true)

	expected := [][]float64{
		{100},
		{200},
		{300},
		{400},
		{500},
		{600},
		{700},
		{800},
		{math.NaN()},
	}

	assert.Equal(t, seriesMetas, sink.Metas)
	test.EqualsWithNansWithDelta(t, expected, sink.Values, math.Pow10(-5))
}

func TestSortDescInstant(t *testing.T) {
	op, err := NewSortOp(SortDescType)
	require.NoError(t, err)

	sink := processSortOp(t, op, true)

	expected := [][]float64{
		{800},
		{700},
		{600},
		{500},
		{400},
		{300},
		{200},
		{100},
		{math.NaN()},
	}

	assert.Equal(t, seriesMetas, sink.Metas)
	test.EqualsWithNansWithDelta(t, expected, sink.Values, math.Pow10(-5))
}

func TestSortNop(t *testing.T) {
	op, err := NewSortOp(SortDescType)
	require.NoError(t, err)

	sink := processSortOp(t, op, false)

	expected := v

	assert.Equal(t, seriesMetas, sink.Metas)
	test.EqualsWithNansWithDelta(t, expected, sink.Values, math.Pow10(-5))
}

var (
	seriesMetas = []block.SeriesMeta{
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

	v = [][]float64{
		{60, 70, 80, 90, 100},
		{150, 160, 170, 180, 200},
		{180, 210, 240, 270, 300},
		{240, 280, 320, 360, 400},
		{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		{300, 350, 400, 450, 500},
		{360, 420, 480, 540, 600},
		{320, 390, 460, 530, 700},
		{480, 560, 640, 720, 800},
	}

	bounds = models.Bounds{
		Start:    xtime.Now(),
		Duration: time.Minute * 5,
		StepSize: time.Minute,
	}
)

func processSortOp(t *testing.T, op parser.Params, instant bool) *executor.SinkNode {
	bl := test.NewBlockFromValuesWithSeriesMeta(bounds, seriesMetas, v)
	c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
	node := op.(sortOp).Node(c, transform.Options{})
	queryContext := models.NoopQueryContext()
	queryContext.Options.Instantaneous = instant
	err := node.Process(queryContext, parser.NodeID(rune(0)), bl)
	require.NoError(t, err)
	return sink
}
