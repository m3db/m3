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
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/require"
)

func toArgs(f float64) []interface{} { return []interface{}{f} }

var (
	start     = time.Now()
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
	expectedVals []float64
}{
	{
		name:         "no series",
		meta:         test.MustMakeMeta(testBound),
		seriesMetas:  []block.SeriesMeta{},
		vals:         [][]float64{},
		expectedVals: []float64{1, 1, 1, 1},
	},
	{
		name:         "no series with tags",
		meta:         test.MustMakeMeta(testBound, "A", "B", "C", "D"),
		seriesMetas:  []block.SeriesMeta{},
		vals:         [][]float64{},
		expectedVals: []float64{1, 1, 1, 1},
	},
	{
		name:         "series with tags and values",
		meta:         test.MustMakeMeta(testBound, "A", "B", "C", "D"),
		seriesMetas:  []block.SeriesMeta{test.MustMakeSeriesMeta("B", "B")},
		vals:         [][]float64{{1, 1, 1, 1}},
		expectedVals: nil,
	},
	{
		name:         "series with tags and some missing",
		meta:         test.MustMakeMeta(testBound, "A", "B", "C", "D"),
		seriesMetas:  []block.SeriesMeta{test.MustMakeSeriesMeta("bar", "baz")},
		vals:         [][]float64{{1, 1, 1, math.NaN()}},
		expectedVals: []float64{nan, nan, nan, 1},
	},
	{
		name: "series with mismatched tags",
		meta: test.MustMakeMeta(testBound, "A", "B", "C", "D"),
		seriesMetas: []block.SeriesMeta{
			test.MustMakeSeriesMeta("B", "B"),
			test.MustMakeSeriesMeta("F", "F"),
		},
		vals: [][]float64{
			{1, 1, 1, math.NaN()},
			{math.NaN(), 1, 1, math.NaN()},
		},
		expectedVals: []float64{nan, nan, nan, 1},
	},
	{
		name: "series with no missing values",
		meta: test.MustMakeMeta(testBound, "A", "B", "C", "D"),
		seriesMetas: []block.SeriesMeta{
			test.MustMakeSeriesMeta("F", "F"),
			test.MustMakeSeriesMeta("F", "F"),
		},
		vals: [][]float64{
			{1, math.NaN(), math.NaN(), 2},
			{math.NaN(), 1, 1, math.NaN()},
		},
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

			c, sink := executor.NewControllerWithSink(parser.NodeID(1))
			absentOp := NewAbsentOp()
			op, ok := absentOp.(transform.Params)
			require.True(t, ok)

			node := op.Node(c, transform.Options{})
			err := node.Process(models.NoopQueryContext(), parser.NodeID(0), block)
			require.NoError(t, err)

			if tt.expectedVals == nil {
				require.Equal(t, 0, len(sink.Values))
			} else {
				require.Equal(t, 1, len(sink.Values))
				test.EqualsWithNans(t, tt.expectedVals, sink.Values[0])
			}
		})
	}
}
