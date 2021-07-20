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

package linear

import (
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/compare"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/require"
)

var (
	emptyArgs = []interface{}{}
	nan       = math.NaN()
	step      = time.Second
	tests     = []struct {
		name     string
		v        []float64
		args     []interface{}
		expected []float64
	}{
		{"default", []float64{1.2, 4.5, 6, nan},
			emptyArgs, []float64{1, 5, 6, nan}},

		{"1.2", []float64{1.2, 4.5, 6, nan},
			toArgs(1.2), []float64{1.2, 4.8, 6, nan}},

		{"-3", []float64{1.2, 4.5, 6, nan},
			toArgs(-3), []float64{0, 3, 6, nan}},

		{"0", []float64{1.2, 4.5, 6, nan},
			toArgs(0), []float64{nan, nan, nan, nan}},
	}
)

func toArgs(f float64) []interface{} { return []interface{}{f} }

func TestRoundWithArgs(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bounds := models.Bounds{
				StepSize: step,
				Duration: step * time.Duration(len(tt.v)),
			}

			v := [][]float64{tt.v}
			block := test.NewBlockFromValues(bounds, v)
			c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
			roundOp, err := NewRoundOp(tt.args)
			require.NoError(t, err)

			op, ok := roundOp.(transform.Params)
			require.True(t, ok)

			node := op.Node(c, transform.Options{})
			err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), block)
			require.NoError(t, err)
			require.Len(t, sink.Values, 1)
			compare.EqualsWithNans(t, tt.expected, sink.Values[0])
		})
	}
}
