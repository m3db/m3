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

package datetime

import (
	"math"
	"testing"

	"github.com/m3db/m3db/src/coordinator/parser"
	"github.com/m3db/m3db/src/coordinator/test"
	"github.com/m3db/m3db/src/coordinator/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimestampType(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	times := getTimes(values, bounds)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	op, err := NewTimestampOp(TimestampType)
	require.NoError(t, err)
	node := op.Node(c)
	err = node.Process(parser.NodeID(0), block)
	require.NoError(t, err)
	expected := expectedDateVals(times, op.operatorType)
	for i, vals := range values {
		for j := range vals {
			if math.IsNaN(vals[j]) {
				expected[i][j] = math.NaN()
			}
		}
	}
	assert.Len(t, sink.Values, 2)
	test.EqualsWithNans(t, expected, sink.Values)
}
