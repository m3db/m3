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

package logical

import (
	"fmt"
	"math"
	"testing"

	"github.com/m3db/m3db/src/coordinator/parser"
	"github.com/m3db/m3db/src/coordinator/test"
	"github.com/m3db/m3db/src/coordinator/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrWithExactValues(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	block1 := test.NewBlockFromValues(bounds, values)
	block2 := test.NewBlockFromValues(bounds, values)

	op := NewOrOp(parser.NodeID(0), parser.NodeID(1), &VectorMatching{})
	c, sink := executor.NewControllerWithSink(parser.NodeID(2))
	node := op.Node(c)

	err := node.Process(parser.NodeID(1), block2)
	require.NoError(t, err)
	err = node.Process(parser.NodeID(0), block1)
	require.NoError(t, err)
	assert.Equal(t, values, sink.Values)
}

func TestOrWithSomeValues(t *testing.T) {
	values1, bounds1 := test.GenerateValuesAndBounds(nil, nil)
	block1 := test.NewBlockFromValues(bounds1, values1)

	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 4, 9},
	}

	values2, bounds2 := test.GenerateValuesAndBounds(v, nil)
	block2 := test.NewBlockFromValues(bounds2, values2)

	op := NewOrOp(parser.NodeID(0), parser.NodeID(1), &VectorMatching{})
	c, sink := executor.NewControllerWithSink(parser.NodeID(2))
	node := op.Node(c)

	err := node.Process(parser.NodeID(1), block2)
	require.NoError(t, err)
	err = node.Process(parser.NodeID(0), block1)
	require.NoError(t, err)
	// NAN values should be filled
	expected := values1

	fmt.Println(sink.Values)
	test.EqualsWithNans(t, expected, sink.Values)
}
