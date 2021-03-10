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

func m(i int) indexMatcher {
	return mm(i, i)
}

func mm(l, r int) indexMatcher {
	return indexMatcher{lhsIndex: l, rhsIndex: r}
}

var distinctLeftTests = []struct {
	name     string
	lhs, rhs []block.SeriesMeta
	expected []indexMatcher
}{
	{
		"equal tags",
		generateMetaDataWithTagsInRange(0, 5),
		generateMetaDataWithTagsInRange(0, 5),
		[]indexMatcher{m(0), m(1), m(2), m(3), m(4)},
	},
	{
		"empty rhs",
		generateMetaDataWithTagsInRange(0, 5),
		[]block.SeriesMeta{},
		[]indexMatcher{},
	},
	{
		"empty lhs",
		[]block.SeriesMeta{},
		generateMetaDataWithTagsInRange(0, 5),
		[]indexMatcher{},
	},
	{
		"longer lhs",
		generateMetaDataWithTagsInRange(-1, 6),
		generateMetaDataWithTagsInRange(0, 5),
		[]indexMatcher{mm(1, 0), mm(2, 1), mm(3, 2), mm(4, 3), mm(5, 4)},
	},
	{
		"longer rhs",
		generateMetaDataWithTagsInRange(0, 5),
		generateMetaDataWithTagsInRange(-1, 6),
		[]indexMatcher{mm(0, 1), mm(1, 2), mm(2, 3), mm(3, 4), mm(4, 5)},
	},
	{
		"shorter lhs",
		generateMetaDataWithTagsInRange(1, 4),
		generateMetaDataWithTagsInRange(0, 5),
		[]indexMatcher{mm(0, 1), mm(1, 2), mm(2, 3)},
	},
	{
		"shorter rhs",
		generateMetaDataWithTagsInRange(0, 5),
		generateMetaDataWithTagsInRange(1, 4),
		[]indexMatcher{mm(1, 0), mm(2, 1), mm(3, 2)},
	},
	{
		"partial overlap",
		generateMetaDataWithTagsInRange(0, 5),
		generateMetaDataWithTagsInRange(1, 6),
		[]indexMatcher{mm(1, 0), mm(2, 1), mm(3, 2), mm(4, 3)},
	},
	{
		"no overlap",
		generateMetaDataWithTagsInRange(0, 5),
		generateMetaDataWithTagsInRange(6, 9),
		[]indexMatcher{},
	},
}

func TestMatchingIndices(t *testing.T) {
	matching := VectorMatching{}
	for _, tt := range distinctLeftTests {
		t.Run(tt.name, func(t *testing.T) {
			excluded := matchingIndices(matching, tt.lhs, tt.rhs)
			assert.Equal(t, tt.expected, excluded)
		})
	}
}

var unlessTests = []struct {
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
		test.NewSeriesMetaWithoutName("a", 2),
		[][]float64{{nan, nan}, {nan, nan}},
		nil,
	},
	{
		"valid, some overlap right",
		test.NewSeriesMeta("a", 2),
		[][]float64{{nan, 2}, {10, 20}},
		test.NewSeriesMeta("a", 3),
		[][]float64{{3, nan}, {30, 40}, {50, 60}},
		test.NewSeriesMetaWithoutName("a", 2),
		[][]float64{{nan, 2}, {nan, nan}},
		nil,
	},
	{
		"valid, some overlap left",
		test.NewSeriesMeta("a", 3),
		[][]float64{{1, 2}, {10, 20}, {100, 200}},
		test.NewSeriesMeta("a", 3)[1:],
		[][]float64{{3, 4}, {nan, 40}},
		test.NewSeriesMetaWithoutName("a", 3),
		[][]float64{{1, 2}, {nan, nan}, {100, nan}},
		nil,
	},
	{
		"valid, some overlap both",
		test.NewSeriesMeta("a", 3),
		[][]float64{{1, 2}, {10, 20}, {100, 200}},
		test.NewSeriesMeta("a", 4)[1:],
		[][]float64{{3, nan}, {nan, 40}, {300, 400}},
		test.NewSeriesMetaWithoutName("a", 3),
		[][]float64{{1, 2}, {nan, 20}, {100, nan}},
		nil,
	},
	{
		"valid, different tags",
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		test.NewSeriesMeta("b", 2),
		[][]float64{{nan, 4}, {30, 40}},
		test.NewSeriesMetaWithoutName("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		nil,
	},
	{
		"valid, different tags, longer rhs",
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		test.NewSeriesMeta("b", 3),
		[][]float64{{3, 4}, {30, 40}, {300, 400}},
		test.NewSeriesMetaWithoutName("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		nil,
	},
	{
		"valid, different tags, longer lhs",
		test.NewSeriesMeta("a", 3),
		[][]float64{{1, 2}, {10, 20}, {100, 200}},
		test.NewSeriesMeta("b", 2),
		[][]float64{{3, 4}, {30, 40}},
		test.NewSeriesMetaWithoutName("a", 3),
		[][]float64{{1, 2}, {10, 20}, {100, 200}},
		nil,
	},
	{
		"mismatched step counts",
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2, 3}, {10, 20, 30}},
		test.NewSeriesMeta("b", 2),
		[][]float64{{3, 4}, {30, 40}},
		test.NewSeriesMetaWithoutName("a", 0),
		[][]float64{},
		errMismatchedStepCounts,
	},
}

func TestUnless(t *testing.T) {
	now := time.Now()
	for _, tt := range unlessTests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewOp(
				UnlessType,
				NodeParams{
					LNode:                parser.NodeID(rune(0)),
					RNode:                parser.NodeID(rune(1)),
					VectorMatcherBuilder: emptyVectorMatcherBuilder,
				},
			)
			require.NoError(t, err)

			c, sink := executor.NewControllerWithSink(parser.NodeID(rune(2)))
			node := op.(baseOp).Node(c, transform.Options{})
			bounds := models.Bounds{
				Start:    now,
				Duration: time.Minute * time.Duration(len(tt.lhs[0])),
				StepSize: time.Minute,
			}

			lhs := test.NewBlockFromValuesWithSeriesMeta(bounds, tt.lhsMeta, tt.lhs)
			err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), lhs)
			require.NoError(t, err)
			bounds = models.Bounds{
				Start:    now,
				Duration: time.Minute * time.Duration(len(tt.rhs[0])),
				StepSize: time.Minute,
			}

			rhs := test.NewBlockFromValuesWithSeriesMeta(bounds, tt.rhsMeta, tt.rhs)
			err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(1)), rhs)
			if tt.err != nil {
				require.EqualError(t, err, tt.err.Error())
				return
			}

			require.NoError(t, err)
			test.EqualsWithNans(t, tt.expected, sink.Values)
			meta := sink.Meta
			assert.Equal(t, 0, meta.Tags.Len())
			assert.True(t, meta.Bounds.Equals(bounds))
			assert.Equal(t, tt.expectedMetas, sink.Metas)
		})
	}
}
