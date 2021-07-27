// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/query/test/compare"
	"github.com/m3db/m3/src/query/test/executor"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func toArgs(f float64) []interface{} { return []interface{}{f} }

var (
	start     = xtime.Now()
	testBound = models.Bounds{
		Start:    start,
		Duration: time.Hour,
		StepSize: time.Minute * 15,
	}
)

var absentTests = []struct {
	name         string
	meta         block.Metadata
	seriesMetas  []block.SeriesMeta
	vals         [][]float64
	expectedMeta block.Metadata
	expectedVals []float64
}{
	{
		"no series",
		test.MustMakeMeta(testBound),
		[]block.SeriesMeta{},
		[][]float64{},
		test.MustMakeMeta(testBound),
		[]float64{1, 1, 1, 1},
	},
	{
		"no series with tags",
		test.MustMakeMeta(testBound, "A", "B", "C", "D"),
		[]block.SeriesMeta{},
		[][]float64{},
		test.MustMakeMeta(testBound, "A", "B", "C", "D"),
		[]float64{1, 1, 1, 1},
	},
	{
		"series with tags and values",
		test.MustMakeMeta(testBound, "A", "B", "C", "D"),
		[]block.SeriesMeta{test.MustMakeSeriesMeta("B", "B")},
		[][]float64{{1, 1, 1, 1}},
		test.MustMakeMeta(testBound, "A", "B", "B", "B", "C", "D"),
		nil,
	},
	{
		"series with tags and some missing",
		test.MustMakeMeta(testBound, "A", "B", "C", "D"),
		[]block.SeriesMeta{test.MustMakeSeriesMeta("bar", "baz")},
		[][]float64{{1, 1, 1, math.NaN()}},
		test.MustMakeMeta(testBound, "A", "B", "bar", "baz", "C", "D"),
		[]float64{nan, nan, nan, 1},
	},
	{
		"series with mismatched tags",
		test.MustMakeMeta(testBound, "A", "B", "C", "D"),
		[]block.SeriesMeta{
			test.MustMakeSeriesMeta("B", "B"),
			test.MustMakeSeriesMeta("F", "F"),
		},
		[][]float64{
			{1, 1, 1, math.NaN()},
			{math.NaN(), 1, 1, math.NaN()},
		},
		test.MustMakeMeta(testBound, "A", "B", "C", "D"),
		[]float64{nan, nan, nan, 1},
	},
	{
		"series with no missing values",
		test.MustMakeMeta(testBound, "A", "B", "C", "D"),
		[]block.SeriesMeta{
			test.MustMakeSeriesMeta("F", "F"),
			test.MustMakeSeriesMeta("F", "F"),
		},
		[][]float64{
			{1, math.NaN(), math.NaN(), 2},
			{math.NaN(), 1, 1, math.NaN()},
		},
		test.MustMakeMeta(testBound, "A", "B", "C", "D", "F", "F"),
		nil,
	},
}

func TestAbsent(t *testing.T) {
	for _, tt := range absentTests {
		t.Run(tt.name, func(t *testing.T) {
			block := test.NewBlockFromValuesWithMetaAndSeriesMeta(
				tt.meta,
				tt.seriesMetas,
				tt.vals,
			)

			c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
			absentOp := NewAbsentOp()
			op, ok := absentOp.(transform.Params)
			require.True(t, ok)

			node := op.Node(c, transform.Options{})
			err := node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
			require.NoError(t, err)

			if tt.expectedVals == nil {
				require.Equal(t, 0, len(sink.Values))
			} else {
				require.Equal(t, 1, len(sink.Values))
				compare.EqualsWithNans(t, tt.expectedVals, sink.Values[0])
				assert.True(t, tt.expectedMeta.Equals(sink.Meta))
			}
		})
	}
}
